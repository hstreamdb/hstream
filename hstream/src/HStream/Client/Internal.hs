{-# LANGUAGE CPP             #-}
{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Client.Internal
  ( streamingFetch
  , cliFetch
  , cliFetch'
  ) where

import           Control.Monad                    (void)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientRequest (..))
import qualified Proto3.Suite                     as PB
import           Text.StringRandom                (stringRandomIO)

import           HStream.Client.Action
import           HStream.Client.Execute
import           HStream.Client.Types             (HStreamCliContext,
                                                   Resource (..))
import           HStream.Client.Utils
import qualified HStream.Server.HStreamApi        as API
#ifdef HStreamUseV2Engine
import           HStream.SQL.Codegen              (DropObject (..),
                                                   TerminationSelection (..))
#else
import           HStream.SQL.Codegen.V1           (DropObject (..),
                                                   TerminationSelection (..))
#endif
import qualified HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (ResourceType (..),
                                                   decompressBatchedRecord,
                                                   formatResult, getServerResp,
                                                   newRandomText)


streamingFetch :: T.Text -> API.HStreamApi ClientRequest response -> IO ()
streamingFetch = streamingFetch' (putStr . formatResult @PB.Struct)

streamingFetch' :: (PB.Struct -> IO ()) -> T.Text -> API.HStreamApi ClientRequest response -> IO ()
streamingFetch' handleResult subId API.HStreamApi{..} = do
  clientId <- genClientId
  void $ hstreamApiStreamingFetch (ClientBiDiRequest 10000 mempty (action clientId))
  where
    action clientId _clientCall _meta streamRecv streamSend writesDone = do
      _ <- streamSend initReq
      receiving
      where
        initReq = API.StreamingFetchRequest
          { API.streamingFetchRequestSubscriptionId = subId
          , API.streamingFetchRequestConsumerName   = clientId
          , API.streamingFetchRequestAckIds         = V.empty
          }
        receiving :: IO ()
        receiving = withInterrupt (void writesDone) $ streamRecv >>= \case
          Left err -> print err
          Right (Just API.StreamingFetchResponse{streamingFetchResponseReceivedRecords = rs}) -> do
            let hRecords = maybe V.empty decompressBatchedRecord (API.receivedRecordRecord =<< rs)
            let ackReq = initReq { API.streamingFetchRequestAckIds
                                 = maybe V.empty API.receivedRecordRecordIds rs }
            let results = PB.fromByteString . API.hstreamRecordPayload <$> hRecords
            mapM_ (\case Right x -> handleResult x; Left x -> print x) results
            _ <- streamSend ackReq
            receiving
          Right Nothing -> putStrLn terminateMsg

cliFetch :: HStreamCliContext -> String -> IO ()
cliFetch = cliFetch' Nothing

-- TODO: should exit if any of the following action failed
cliFetch' :: Maybe (PB.Struct -> IO ()) -> HStreamCliContext -> String -> IO ()
cliFetch' handleResult ctx sql = do
  (sName, newSql) <- genRandomSinkStreamSQL (T.pack . removeEmitChanges . words $ sql)
  subId <- genRandomSubscriptionId
  qName <-  ("cli_generated_" <>) <$> newRandomText 10
  API.CreateQueryResponse (Just (API.CreateQueryResponseInfoQuery API.QueryInfo{..}))
    <- getServerResp =<< executeWithLookupResource ctx (Resource ResQuery qName)
    (createStreamBySelectWithCustomQueryName (T.unpack newSql) qName)
  void . execute ctx $ createSubscription subId sName
  executeWithLookupResource_ ctx (Resource ResSubscription subId)
    (case handleResult of Nothing -> streamingFetch subId
                          Just h  -> streamingFetch' h subId)
  executeWithLookupResource_ ctx (Resource ResSubscription subId) (void . deleteSubscription subId True)
  executeWithLookupResource_ ctx (Resource ResQuery qName) (terminateQueries (OneQuery queryInfoName))
  executeWithLookupResource_ ctx (Resource ResStream sName) (void . dropAction False (DStream sName))

genRandomSubscriptionId :: IO T.Text
genRandomSubscriptionId =  do
  randomName <- stringRandomIO "[a-zA-Z]{20}"
  return $ "cli_internal_subscription_" <> randomName

genRandomSinkStreamSQL :: T.Text -> IO (T.Text, T.Text)
genRandomSinkStreamSQL sql = do
  randomName <- stringRandomIO "[a-zA-Z]{20}"
  let streamName = "cli_generated_stream_" <> randomName
  return (streamName, "CREATE STREAM " <> streamName <> " AS " <> sql)
