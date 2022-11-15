{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Client.Internal
  ( streamingFetch
  , cliFetch
  ) where

import           Control.Monad                    (void)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientRequest (..))
import qualified Proto3.Suite                     as PB


import           HStream.Client.Action
import           HStream.Client.Execute
import           HStream.Client.Types             (HStreamSqlContext (..),
                                                   Resource (..))
import           HStream.Client.Utils
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL.Codegen              (DropObject (..))
import qualified HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (ResourceType (..),
                                                   decompressBatchedRecord,
                                                   formatResult)
import           Text.StringRandom                (stringRandomIO)

streamingFetch :: T.Text -> API.HStreamApi ClientRequest response -> IO ()
streamingFetch subId API.HStreamApi{..} = do
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
            let results = (formatResult @PB.Struct <$>) . PB.fromByteString . API.hstreamRecordPayload <$> hRecords
            mapM_ (\case Right x -> putStr x; Left x -> print x) results
            _ <- streamSend ackReq
            receiving
          Right Nothing -> putStrLn terminateMsg

cliFetch :: HStream.Client.Types.HStreamSqlContext -> String -> IO ()
cliFetch ctx sql = do
  (sName, newSql) <- genRandomSinkStreamSQL (T.pack . removeEmitChanges . words $ sql)
  subId <- genRandomSubscriptionId
  void . execute ctx $ createStreamBySelect (T.unpack newSql)
  void . execute ctx $ createSubscription subId sName
  executeWithLookupResource_ ctx (Resource ResSubscription subId) (streamingFetch subId)
  executeWithLookupResource_ ctx (Resource ResSubscription subId) (void . deleteSubscription subId True)
  -- FIXME: Replace resource type with Res Stream once lookup stream is supported
  executeWithLookupResource_ ctx (Resource ResSubscription subId) (void . dropAction False (DStream sName))

genRandomSubscriptionId :: IO T.Text
genRandomSubscriptionId =  do
  randomName <- stringRandomIO "[a-zA-Z]{20}"
  return $ "cli_internal_subscription_" <> randomName

genRandomSinkStreamSQL :: T.Text -> IO (T.Text, T.Text)
genRandomSinkStreamSQL sql = do
  randomName <- stringRandomIO "[a-zA-Z]{20}"
  let streamName = "cli_generated_stream_" <> randomName
  return (streamName, "CREATE STREAM " <> streamName <> " AS " <> sql)
