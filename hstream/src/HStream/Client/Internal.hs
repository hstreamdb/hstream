{-# LANGUAGE CPP             #-}
{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Client.Internal
  ( streamingFetch
  , cliFetch
  , cliFetch'
  , interactiveAppend
  ) where

import           Control.Concurrent               (threadDelay)
import           Control.Monad                    (void, when)
import           Control.Monad.IO.Class
import           Data.Aeson                       as Aeson
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Char8            as BC
import qualified Data.ByteString.Lazy             as BSL
import           Data.IORef                       (IORef, newIORef, readIORef,
                                                   writeIORef)
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as TE
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (ClientRequest (..))
import qualified Proto3.Suite                     as PB
import qualified System.Console.Haskeline         as RL
import           Text.StringRandom                (stringRandomIO)

import           HStream.Client.Action
import           HStream.Client.Execute
import           HStream.Client.Types             (AppendContext (..),
                                                   HStreamCliContext (..),
                                                   Resource (..),
                                                   getShardIdByKey)
import           HStream.Client.Utils
import           HStream.Common.Types             (hashShardKey)
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL                      (DropObject (..))
import qualified HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (ResourceType (..),
                                                   clientDefaultKey,
                                                   decompressBatchedRecord,
                                                   formatResult, getServerResp,
                                                   jsonObjectToStruct,
                                                   newRandomText, splitOn)

streamingFetch :: HStreamCliContext -> T.Text -> API.HStreamApi ClientRequest response -> IO ()
streamingFetch = streamingFetch' (putStr . formatResult @PB.Struct) False

streamingFetch' :: (PB.Struct -> IO ()) -> Bool -> HStreamCliContext -> T.Text -> API.HStreamApi ClientRequest response -> IO ()
streamingFetch' handleResult recovered ctx subId API.HStreamApi{..} = do
  when recovered $ putStrLn "Query is recovered"
  clientId <- genClientId
  void $ hstreamApiStreamingFetch (ClientBiDiRequest 10000 mempty (action clientId))
  where
    action clientId _clientCall _meta streamRecv streamSend writesDone = do
      _ <- streamSend initReq
      interruptSignal <- newIORef False
      receiving interruptSignal
      where
        initReq = API.StreamingFetchRequest
          { API.streamingFetchRequestSubscriptionId = subId
          , API.streamingFetchRequestConsumerName   = clientId
          , API.streamingFetchRequestAckIds         = V.empty
          }
        receiving :: IORef Bool ->  IO ()
        receiving sig = withInterrupt (writeIORef sig True >> void writesDone) $ streamRecv >>= \case
          Left err -> print err
          Right (Just API.StreamingFetchResponse{streamingFetchResponseReceivedRecords = rs}) -> do
            let hRecords = maybe V.empty decompressBatchedRecord (API.receivedRecordRecord =<< rs)
            let ackReq = initReq { API.streamingFetchRequestAckIds
                                 = maybe V.empty API.receivedRecordRecordIds rs }
            let results = PB.fromByteString . API.hstreamRecordPayload <$> hRecords
            mapM_ (\case Right x -> handleResult x; Left x -> print x) results
            _ <- streamSend ackReq
            receiving sig
          Right Nothing -> do
            interrupted <- readIORef sig
            if interrupted
              then putStrLn terminateMsg
              else do
                putStrLn "The original server is dead, recovering query..."
                threadDelay 5000000
                executeWithLookupResource_ ctx (Resource ResSubscription subId) $
                  streamingFetch' handleResult True ctx subId

cliFetch :: HStreamCliContext -> String -> IO ()
cliFetch = cliFetch' Nothing

-- TODO: should exit if any of the following action failed
cliFetch' :: Maybe (PB.Struct -> IO ()) -> HStreamCliContext -> String -> IO ()
cliFetch' handleResult ctx sql = do
  (sName, newSql) <- genRandomSinkStreamSQL (T.pack . removeEmitChanges . words $ sql)
  subId <- genRandomSubscriptionId
  qName <-  ("cli_generated_" <>) <$> newRandomText 10
  API.Query {..} <- getServerResp =<< executeWithLookupResource ctx (Resource ResQuery qName)
    (createStreamBySelectWithCustomQueryName (T.unpack newSql) qName)
  void . execute ctx $ createSubscription subId sName
  executeWithLookupResource_ ctx (Resource ResSubscription subId)
    (case handleResult of Nothing -> streamingFetch ctx subId
                          Just h  -> streamingFetch' h False ctx subId)
  executeWithLookupResource_ ctx (Resource ResSubscription subId) (void . deleteSubscription subId True)
  executeWithLookupResource_ ctx (Resource ResQuery qName) (terminateQuery queryId)
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

interactiveAppend :: AppendContext -> IO ()
interactiveAppend AppendContext{..} = do
  RL.runInputT settings loop
 where
  settings = RL.Settings RL.noCompletion Nothing False

  loop = RL.withInterrupt . RL.handleInterrupt loop $ do
    RL.getInputLine "> " >>= \case
      Nothing -> pure ()
      Just str -> do
        let items = splitOn appKeySeparator (BC.pack str)
        when (null items || length items > 2) $
          errorWithoutStackTrace "invalid input: specific multiple keys"

        let partitionKey = if length items == 1 then clientDefaultKey else TE.decodeUtf8 . head $ items
        let record = if length items == 1 then head items else last items
        let shardKey = hashShardKey partitionKey
        case getShardIdByKey shardKey appShardMap of
          Just sid -> do
            let (isHRecord, payload) = toHRecord record
            liftIO $ executeWithLookupResource_ cliCtx (Resource ResShard (T.pack $ show sid))
              (retry appRetryLimit appRetryInterval $ insertIntoStream' appStream sid isHRecord (V.fromList [payload]) API.CompressionTypeNone partitionKey)
            loop
          Nothing  -> errorWithoutStackTrace $ "Failed to calculate shardId with stream: "
                                             <> show appStream <> ", parition key: " <> show (head items)

  toHRecord payload = case Aeson.eitherDecode . BS.fromStrict $ payload of
    Left _  -> (False, payload)
    Right p -> (True, BSL.toStrict . PB.toLazyByteString . jsonObjectToStruct $ p)

