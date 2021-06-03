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

import           HStream.Processing.Util
import           HStream.SQL.Codegen
import           System.IO
import           System.IO.Unsafe

import           Data.Aeson
import           Data.Aeson.Types
import qualified Data.ByteString.Char8            as DBC
import qualified Data.HashMap.Strict              as HM
import           Data.Scientific
import qualified Data.Text                        as T
import           Database.ClickHouseDriver
import           Database.ClickHouseDriver.Types
import           HStream.SQL.AST
import           HStream.Server.Converter
import           Haxl.Core                        (Env (states))

clickHouseSinkConnector :: IO(Env () w) -> SinkConnector
clickHouseSinkConnector ckClient = SinkConnector {
  writeRecord = writeRecordToClickHouse ckClient
}

valueToCKType :: Value -> ClickhouseType
valueToCKType (String s) = CKString (DBC.pack $ show s)
valueToCKType (Bool b) = if b then CKInt8 1 else CKInt8 0
valueToCKType (Number sci) = do
  case floatingOrInteger sci of
    Left r  -> CKDecimal64 r
    Right i -> CKInt64 i

writeRecordToClickHouse :: IO(Env () w) -> SinkRecord -> IO ()
writeRecordToClickHouse ckClient SinkRecord{..} = do
    conn <- ckClient
    ping conn
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
    return ()
  where
    keysAsSql :: [String] -> String
    keysAsSql ss = '(' : helper' ss
      where
        helper' (a:b:ss) = a ++ (',':helper' (b:ss))
        helper' (a:ss)   = a ++ (helper' ss)
        helper' []       = ")"

data Payload = Payload {
  pTimestamp :: Timestamp,
  pKey       :: Maybe ZCB.CBytes,
  pValue     :: ZCB.CBytes
} deriving (Show, Generic, JSON.JSON)

