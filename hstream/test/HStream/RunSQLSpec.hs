{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms   #-}

module HStream.RunSQLSpec (spec) where

import           Control.Monad                  (replicateM)
import           HStream.Processing.Connector
import           HStream.Processing.Processor
import           HStream.Processing.Type
import           HStream.Processing.Util        (getCurrentTimestamp)
import           HStream.SQL.Codegen
import           HStream.Server.HStoreConnector
import           HStream.Server.Handler
import           HStream.Server.Utils
import           HStream.Store
import           RIO
import qualified RIO.ByteString.Lazy            as BL
import qualified RIO.Map                        as Map
import qualified RIO.Text                       as Text
import           System.IO.Unsafe               (unsafePerformIO)
import           System.Random
import           Test.Hspec
import           Text.Printf                    (printf)

ldclient :: LDClient
ldclient = unsafePerformIO $ newLDClient  "/data/store/logdevice.conf"
{-# NOINLINE ldclient #-}

spec :: Spec
spec = describe "HStream.RunSQLSpec" $ do
  source1 <- runIO $ newRandomText 10
  source2 <- runIO $ newRandomText 10
  sink1 <- runIO $ newRandomText 10
  sink2 <- runIO $ newRandomText 10

  it "create streams" $
    (do
        setLogDeviceDbgLevel C_DBG_ERROR
        handleCreateStreamSQL $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"
        handleCreateStreamSQL $ "CREATE STREAM " <> source2 <> ";"
        handleCreateStreamSQL $ "CREATE STREAM " <> sink1   <> " WITH (REPLICATE = 3);"
        handleCreateStreamSQL $ "CREATE STREAM " <> sink2   <> " ;"
    ) `shouldReturn` ()

  it "insert data to source streams" $
    (do
      handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (22, 80);"
      handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (15, 10);"
    ) `shouldReturn` ()

  it "drop streams" $
    (do
        setLogDeviceDbgLevel C_DBG_ERROR
        handleDropStreamSQL $ "DROP STREAM " <> source1 <> " ;"
        handleDropStreamSQL $ "DROP STREAM " <> source2 <> " ;"
        handleDropStreamSQL $ "DROP STREAM " <> sink1   <> " ;"
        handleDropStreamSQL $ "DROP STREAM " <> sink2   <> " ;"
        handleDropStreamSQL $ "DROP STREAM IF EXIST " <> source1 <> " ;"
        handleDropStreamSQL $ "DROP STREAM IF EXIST " <> sink1   <> " ;"
    ) `shouldReturn` ()

  it "a simple SQL query" $
    (do
      handleCreateStreamSQL $ "CREATE STREAM " <> source1 <> ";"
      handleCreateStreamSQL $ "CREATE STREAM " <> source2 <> ";"
      handleCreateStreamSQL $ "CREATE STREAM " <> sink1   <> ";"

      result <- async . handleCreateBySelectSQL $
        "CREATE STREAM " <> sink1 <> " AS SELECT * FROM " <> source1 <> " EMIT CHANGES WITH (REPLICATE = 3);"
      threadDelay 5000000
      handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (31, 26);"
      handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (22, 80);"

      handleDropStreamSQL   $ "DROP STREAM "   <> source2 <> " ;"
      handleCreateStreamSQL $ "CREATE STREAM " <> source2 <> ";"

      handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (15, 10);"
      handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (31, 26);"
      wait result
    ) `shouldReturn` ["{\"temperature\":31,\"humidity\":26}","{\"temperature\":15,\"humidity\":10}"]

  -- it "a more complex query" $
  --   (do
  --      result <- async . handleCreateBySelectSQL $ Text.pack $ printf
  --        "CREATE STREAM %s AS SELECT SUM(%s.humidity) AS result FROM %s INNER JOIN %s WITHIN (INTERVAL 5 SECOND) ON (%s.temperature = %s.temperature) WHERE %s.humidity > 20 GROUP BY %s.humidity, TUMBLING (INTERVAL 10 SECOND) EMIT CHANGES WITH (FORMAT = \"JSON\", REPLICATE = 3);" sink2 source2 source2 source1 source2 source1 source2 source2
  --      threadDelay 500000
  --      handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (31, 26);"
  --      handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (22, 80);"
  --      handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (15, 10);"
  --      handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (31, 26);"
  --      wait result
  --   ) `shouldReturn` ["{\"result\":26}"]

handleCreateStreamSQL :: Text -> IO ()
handleCreateStreamSQL sql = do
  plan <- streamCodegen sql
  case plan of
    CreatePlan streamName _ ->
      createStream ldclient (textToCBytes streamName) (LogAttrs $ HsLogAttrs{logReplicationFactor = 3, logExtraAttrs=Map.empty})
    _ -> error "Execution plan type mismatched"

handleDropStreamSQL :: Text -> IO ()
handleDropStreamSQL sql = do
  plan <- streamCodegen sql
  case plan of
    DropPlan checkIfExist stream -> do
      streamExists <- doesStreamExists ldclient (textToCBytes stream)
      if streamExists then removeStream ldclient (textToCBytes stream)
      else if checkIfExist then return () else error "stream does not exist"
    _ -> error "Execution plan type mismatched"

handleInsertSQL :: Text -> IO ()
handleInsertSQL sql = do
  putStrLn "Start handleInsertSQL..."
  plan <- streamCodegen sql
  case plan of
    InsertPlan streamName payload -> do
      timestamp <- getCurrentTimestamp
      writeRecord
        (hstoreSinkConnector ldclient)
        SinkRecord
          { snkStream = streamName,
            snkKey = Nothing,
            snkValue = payload,
            snkTimestamp = timestamp
          }
    _ -> error "Execution plan type mismatched"

handleCreateBySelectSQL :: Text -> IO [BL.ByteString]
handleCreateBySelectSQL sql = do
  putStrLn "enter handleCreateBySelectSQL"
  plan <- streamCodegen sql
  case plan of
    CreateBySelectPlan _ sink taskBuilder _ -> do
      ldreader <- newLDFileCkpReader ldclient (textToCBytes (Text.append (getTaskName taskBuilder) "-result")) "/tmp/checkpoint" 1 Nothing 3
      let sc = hstoreSourceConnector ldclient ldreader
      subscribeToStream sc sink Latest
      async $ runTaskWrapper taskBuilder ldclient
      threadDelay 5000000
      records <- replicateM 10 $ do
        threadDelay 500000
        readRecords sc
      return $ srcValue <$> concat records
    _ -> error "Execution plan type mismatch"

newRandomText :: Int -> IO Text
newRandomText n = Text.pack . take n . randomRs ('a', 'z') <$> newStdGen
