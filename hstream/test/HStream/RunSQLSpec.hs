{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunSQLSpec (spec) where

import           Control.Monad                    (replicateM)
import qualified Data.ByteString.Char8            as C
import qualified Data.ByteString.Lazy.Char8       as DBCL
import           Database.ClickHouseDriver.Client
import           Database.ClickHouseDriver.Types
import qualified Database.MySQL.Base              as MySQL
import           RIO
import qualified RIO.ByteString.Lazy              as BL
import qualified RIO.Map                          as Map
import qualified RIO.Text                         as Text
import           System.IO.Streams                as SIS hiding (take)
import           System.IO.Unsafe                 (unsafePerformIO)
import           System.Random
import           Test.Hspec

import           HStream.Connector.ClickHouse
import           HStream.Connector.HStore
import           HStream.Connector.MySQL
import           HStream.Processing.Connector
import           HStream.Processing.Processor
import           HStream.Processing.Type
import           HStream.Processing.Util          (getCurrentTimestamp)
import           HStream.SQL.AST
import           HStream.SQL.Codegen
import           HStream.Server.Handler
import           HStream.Store
import           HStream.Utils

ldclient :: LDClient
ldclient = unsafePerformIO $ newLDClient  "/data/store/logdevice.conf"
{-# NOINLINE ldclient #-}

spec :: Spec
spec = describe "HStream.RunSQLSpec" $ do
  source1 <- runIO $ newRandomText 10
  source2 <- runIO $ newRandomText 10
  sink1 <- runIO $ newRandomText 10
  sink2 <- runIO $ newRandomText 10
  let source3 = "source3"

  it "create streams" $
    (do
        setLogDeviceDbgLevel C_DBG_ERROR
        handleDropStreamSQL $ "DROP STREAM IF EXISTS " <> source1 <> " ;"
        handleDropStreamSQL $ "DROP STREAM IF EXISTS " <> source2 <> " ;"
        handleDropStreamSQL $ "DROP STREAM IF EXISTS " <> source3 <> " ;"
        handleCreateStreamSQL $ "CREATE STREAM " <> source1 <> " WITH (REPLICATE = 3);"
        handleCreateStreamSQL $ "CREATE STREAM " <> source2 <> ";"
        handleCreateStreamSQL $ "CREATE STREAM " <> source3 <> " WITH (REPLICATE = 3);"
        handleCreateStreamSQL $ "CREATE STREAM " <> sink1   <> " WITH (REPLICATE = 3);"
        handleCreateStreamSQL $ "CREATE STREAM " <> sink2   <> " ;"
    ) `shouldReturn` ()

  it "insert data to source streams" $
    (do
      handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (22, 80);"
      handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (15, 10);"
    ) `shouldReturn` ()


  it "create connectors" $
    (do
      handleCreateMysqlTable source3
      handleCreateConnectorSQL $ "CREATE SOURCE | SINK CONNECTOR mysql WITH (type = \"mysql\", host = \"127.0.0.1\", streamname = \""<> source3 <>"\");"
      handleInsertSQL $ "INSERT INTO " <> source3 <> " (temperature, humidity) VALUES (12, 84);"
      handleInsertSQL $ "INSERT INTO " <> source3 <> " (temperature, humidity) VALUES (22, 83);"
      handleInsertSQL $ "INSERT INTO " <> source3 <> " (temperature, humidity) VALUES (32, 82);"
      handleInsertSQL $ "INSERT INTO " <> source3 <> " (temperature, humidity) VALUES (42, 81);"
      threadDelay 5000000
      handleCleanMysql source3
    ) `shouldReturn` ()

  it "drop streams" $
    (do
        setLogDeviceDbgLevel C_DBG_ERROR
        handleDropStreamSQL $ "DROP STREAM " <> source1 <> " ;"
        handleDropStreamSQL $ "DROP STREAM " <> source2 <> " ;"
        handleDropStreamSQL $ "DROP STREAM " <> sink1   <> " ;"
        handleDropStreamSQL $ "DROP STREAM " <> sink2   <> " ;"
        handleDropStreamSQL $ "DROP STREAM IF EXISTS " <> source1 <> " ;"
        handleDropStreamSQL $ "DROP STREAM IF EXISTS " <> sink1   <> " ;"
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
      createStream ldclient (transToStreamName streamName) (LogAttrs $ HsLogAttrs{logReplicationFactor = 3, logExtraAttrs=Map.empty})
    _ -> error "Execution plan type mismatched"

handleCreateConnectorSQL :: Text -> IO ()
handleCreateConnectorSQL sql = do
    plan <- streamCodegen sql
    case plan of
      CreateConnectorPlan cName (RConnectorOptions cOptions) -> do
          ldreader <- newLDFileCkpReader ldclient (textToCBytes (Text.append cName "_reader")) checkpointRootPath 1000 Nothing 3
          let sc = hstoreSourceConnector ldclient ldreader
          let streamM = lookup "streamname" cOptions
          let typeM = lookup "type" cOptions
          let fromCOptionString m = case m of
                Just (ConstantString s) -> Just $ C.pack s
                _                       -> Nothing
          let fromCOptionStringToString m = case m of
                Just (ConstantString s) -> Just s
                _                       -> Nothing
          let fromCOptionIntToPortNumber m = case m of
                Just (ConstantInt s) -> Just $ fromIntegral s
                _                    -> Nothing
          let sk' = case typeM of
                Just (ConstantString cType) ->
                  do
                    case cType of
                        "clickhouse" -> do
                          cli <- createClient $ ConnParams
                              (fromMaybe "default" $ fromCOptionString (lookup "username" cOptions))
                              (fromMaybe "127.0.0.1" $ fromCOptionString (lookup "host" cOptions))
                              (fromMaybe "9000" $ fromCOptionString (lookup "port" cOptions))
                              (fromMaybe "" $ fromCOptionString (lookup "password" cOptions))
                              False
                              (fromMaybe "default" $ fromCOptionString (lookup "database" cOptions))
                          let connector = clickHouseSinkConnector cli
                          return $ Right connector
                        "mysql" -> do
                          conn <- MySQL.connect $ MySQL.ConnectInfo
                              (fromMaybe "127.0.0.1" $ fromCOptionStringToString (lookup "host" cOptions))
                              (fromMaybe 3306 $ fromCOptionIntToPortNumber (lookup "port" cOptions))
                              (fromMaybe "mysql" $ fromCOptionString (lookup "database" cOptions))
                              (fromMaybe "root" $ fromCOptionString (lookup "username" cOptions))
                              (fromMaybe "password" $ fromCOptionString (lookup "password" cOptions))
                              33
                          let connector = mysqlSinkConnector conn
                          return $ Right connector
                        _ -> return $ Left "unsupported sink connector"
                _ -> return $ Left "Invalid type in connector options"
          sk <- sk'
          case sk of
            Left err -> error err
            Right conn -> do
              case streamM of
                Just (ConstantString stream) -> do
                  subscribeToStream sc (Text.pack stream) Latest
                  _ <- async $
                      forever $
                        do
                          records <- readRecords sc
                          forM_ records $ \SourceRecord {..} ->
                            writeRecord
                              conn
                              SinkRecord
                                { snkStream = (Text.pack stream),
                                  snkKey = srcKey,
                                  snkValue = srcValue,
                                  snkTimestamp = srcTimestamp
                                }
                  return ()
                _ -> error "streamname is nothing..."
      _ -> error "Execution plan type mismatched"

handleDropStreamSQL :: Text -> IO ()
handleDropStreamSQL sql = do
  plan <- streamCodegen sql
  case plan of
    DropPlan checkIfExist (DStream stream) -> do
      streamExists <- doesStreamExists ldclient (transToStreamName stream)
      if streamExists then removeStream ldclient (transToStreamName stream)
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

-- create mysql table for testing
handleCreateMysqlTable :: Text -> IO ()
handleCreateMysqlTable source = do
  conn <- MySQL.connect MySQL.ConnectInfo {
    ciUser = "root",
    ciPassword = "password",
    ciPort = 3306,
    ciHost = "127.0.0.1",
    ciDatabase = "mysql",
    ciCharset = 33
  }

  _ <- MySQL.execute_ conn $ MySQL.Query . DBCL.pack $ "CREATE TABLE IF NOT EXISTS "<> Text.unpack source <>"(temperature INT, humidity INT) CHARACTER SET utf8"
  MySQL.close conn

-- clean mysql data and check insert rows are correct
handleCleanMysql :: Text -> IO ()
handleCleanMysql source = do
  conn <- MySQL.connect MySQL.ConnectInfo {
    ciUser = "root",
    ciPassword = "password",
    ciPort = 3306,
    ciHost = "127.0.0.1",
    ciDatabase = "mysql",
    ciCharset = 33
  }

  (_, is) <- MySQL.query_ conn $ MySQL.Query . DBCL.pack $ "SELECT * FROM " <> Text.unpack source
  (rowCount :: Int) <- SIS.fold (\s _ -> s+1) 0 is

  _ <- MySQL.execute_ conn $ MySQL.Query . DBCL.pack $ "DROP TABLE IF EXISTS " <> Text.unpack source

  MySQL.close conn

  if rowCount == 4
    then return ()
    else error "Sink connector sync data failed"
