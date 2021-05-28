{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE NoImplicitPrelude  #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StrictData         #-}


module HStream.Server.ClickHouseConnector
  ( 
    clickHouseSinkConnector,
  )
where

import           HStream.Processing.Connector
import           HStream.Processing.Type          as HPT
import           HStream.Server.Utils
import           HStream.Store
import           HStream.Store.Internal.LogDevice
import           RIO
import qualified RIO.Map                          as M
import qualified Z.Data.CBytes                    as ZCB
import qualified Z.Data.JSON                      as JSON

import           HStream.SQL.Codegen
import           HStream.Processing.Util 
import           System.IO.Unsafe
import           System.IO

import Database.ClickHouseDriver
import Database.ClickHouseDriver.Types
import Haxl.Core (Env(states))
import qualified Data.HashMap.Strict                             as HM
import           Data.Aeson
import HStream.SQL.AST
import Data.Aeson.Types
import Data.Scientific
import qualified Data.ByteString.Char8 as DBC
import HStream.Server.Converter
import qualified Data.Text as T


main :: IO ()
main = do
  handleInsertSQL $ "INSERT INTO " <> "source1" <> " (temperature, humidity) VALUES (22, \"80\");"

defaultCKClient :: IO(Env () w)
defaultCKClient = createClient ConnParams{
       username'    = "default"
      ,host'        = "host.docker.internal"
      ,port'        = "9000"
      ,password'    = ""
      ,compression' = False
      ,database'    = "default"
    }

handleInsertSQL :: Text -> IO ()
handleInsertSQL sql = do
  plan <- streamCodegen sql
  case plan of
    InsertPlan topicName payload -> do
      timestamp <- getCurrentTimestamp
      writeRecord
        (clickHouseSinkConnector defaultCKClient)
        SinkRecord
          { snkStream = topicName,
            snkKey = Nothing,
            snkValue = payload,
            snkTimestamp = timestamp
          }
    _ -> error "Execution plan type mismatched"

clickHouseSinkConnector :: IO(Env () w) -> SinkConnector
clickHouseSinkConnector ckClient = SinkConnector {
  writeRecord = writeRecordToClickHouse ckClient
}

valueToCKType :: Value -> ClickhouseType  
valueToCKType (String s) = CKString (DBC.pack $ show s)
valueToCKType (Bool b) = if b then CKInt8 1 else CKInt8 0
valueToCKType (Number sci) = do
  case floatingOrInteger sci of
    Left r -> CKDecimal64 r 
    Right i -> CKInt64 i

writeRecordToClickHouse :: IO(Env () w) -> SinkRecord -> IO ()
writeRecordToClickHouse ckClient SinkRecord{..} = do
    conn <- ckClient
    ping conn
    let snkValue = "{\"temperature\":22,\"humidity\":\"80\",\"a\":{\"b\":1,\"c\":2,\"d\":{\"e\":5}}}"
    let insertMap = (decode snkValue) :: Maybe (HM.HashMap T.Text Value)
    print insertMap
    case insertMap of 
      Just l -> do
        let flatterned = flattern l
        print flatterned
        print $ HM.keys flatterned
        let keys = HM.keys flatterned
        let elems = HM.elems flatterned
        insertOneRow conn ("INSERT INTO " ++ show snkStream ++ " " ++ keysAsSql (map T.unpack keys) ++" VALUES ") (valueToCKType <$> elems)
      _ -> do return "Invalid SinK Value"

    -- logId <- getCLogIDByStreamName ckClient (textToCBytes snkStream)
    -- let payload =
    --       Payload {
    --         pTimestamp = snkTimestamp,
    --         pKey = fmap lazyByteStringToCbytes snkKey,
    --         pValue = lazyByteStringToCbytes snkValue
    --       }
    -- lsn <- appendSync ckClient logId (JSON.encode payload) Nothing
    return ()
  where
    keysAsSql :: [String] -> String
    keysAsSql ss = '(' : helper' ss
      where
        helper' (a:b:ss) = a ++ (',':helper' (b:ss))
        helper' (a:ss) = a ++ (helper' ss)
        helper' [] = ")"

data Payload = Payload {
  pTimestamp :: Timestamp,
  pKey       :: Maybe ZCB.CBytes,
  pValue     :: ZCB.CBytes
} deriving (Show, Generic, JSON.JSON)

