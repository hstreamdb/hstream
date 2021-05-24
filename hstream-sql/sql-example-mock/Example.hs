{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Data.Aeson
import qualified Data.HashMap.Strict                as HM
import           Data.Scientific
import           Data.Text.IO                       (getLine)
import           HStream.Processing.Connector
import           HStream.Processing.MockStreamStore
import           HStream.Processing.Processor
import           HStream.Processing.Type
import           HStream.Processing.Util
import           HStream.SQL.Codegen                (ExecutionPlan (..),
                                                     streamCodegen)
import qualified Prelude                            as P
import           RIO
import qualified RIO.ByteString.Lazy                as BL
import           System.Random                      (Random (randomR),
                                                     getStdRandom)

---------------------------------- Example -------------------------------------
-- CREATE STREAM demoSink AS SELECT * FROM source1 EMIT CHANGES WITH (FORMAT = "JSON");

-- CREATE STREAM demoSink AS SELECT SUM(source2.humidity) AS result FROM source2 INNER JOIN source1 WITHIN (INTERVAL 5 SECOND) ON (source2.temperature = source1.temperature) WHERE source2.humidity > 20 GROUP BY source2.humidity, TUMBLING (INTERVAL 10 SECOND) EMIT CHANGES WITH (FORMAT = "JSON");
--------------------------------------------------------------------------------

main :: IO ()
main = getLine >>= run

run :: Text -> IO ()
run input = do
  plan <- streamCodegen input
  (sStreamName,task) <- case plan of
    SelectPlan source sink task           -> return (sink,task)
    CreateBySelectPlan source sink task _ -> return (sink,task)
    _                                     -> error "Not supported"

  let tStreamName = "source1"
  let hStreamName = "source2"

  mockStore <- mkMockStreamStore
  sourceConnector1 <- mkMockStoreSourceConnector mockStore
  sourceConnector2 <- mkMockStoreSourceConnector mockStore
  sinkConnector <- mkMockStoreSinkConnector mockStore

  _ <- async $
    forever $
      do
        threadDelay 1000000
        MockMessage {..} <- mkMockData
        writeRecord
          sinkConnector
          SinkRecord
            { snkStream = hStreamName,
              snkKey = mmKey,
              snkValue = mmValue,
              snkTimestamp = mmTimestamp
            }
        writeRecord
          sinkConnector
          SinkRecord
            { snkStream = tStreamName,
              snkKey = mmKey,
              snkValue = mmValue,
              snkTimestamp = mmTimestamp
            }

  _ <- async $
    forever $
      do
        subscribeToStream sourceConnector1 sStreamName Earlist
        records <- readRecords sourceConnector1
        forM_ records $ \SourceRecord {..} ->
          P.putStr ">>> result: " >> BL.putStrLn srcValue

  runTask sourceConnector2 sinkConnector task

--------------------------------------------------------------------------------
mkMockData :: IO MockMessage
mkMockData = do
  k <- getStdRandom (randomR (1, 3)) :: IO Int
  t <- getStdRandom (randomR (0, 100)) :: IO Int
  h <- getStdRandom (randomR (0, 100)) :: IO Int
  let r = HM.fromList [ ("temperature" :: Text, Number (scientific (toInteger t) 0))
                      , ("humidity"    :: Text, Number (scientific (toInteger h) 0)) ]
  P.putStrLn $ "gen data: " ++ show r
  ts <- getCurrentTimestamp
  return
    MockMessage
      { mmTimestamp = ts,
        -- WARNING: A Nothing key in a task with JOIN can raise an exception
        mmKey = Just $ encode $ HM.fromList [ ("key" :: Text, Number (scientific (toInteger k) 0))],     -- TLE.encodeUtf8 $ TL.pack $ show k,
        mmValue = encode r
      }
