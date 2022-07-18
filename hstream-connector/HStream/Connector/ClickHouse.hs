{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Connector.ClickHouse
  ( clickHouseSinkConnector
  ) where

import           Control.Monad                   (void)
import qualified Data.Aeson                      as Aeson
import qualified Data.ByteString.Char8           as DBC
import qualified Data.HashMap.Strict             as HM
import           Data.List                       (intercalate)
import           Data.Scientific                 (floatingOrInteger)
import qualified Data.Text                       as Text
import qualified Database.ClickHouseDriver       as CK
import qualified Database.ClickHouseDriver.Types as CK
import           Haxl.Core                       (Env)
import           HStream.Utils                   (flattenJSON)

import           HStream.Connector.Common        (SinkConnector (..))
import           HStream.Connector.Type          (SinkRecord (..))
import qualified HStream.Logger                  as Log

clickHouseSinkConnector :: Env () w -> SinkConnector
clickHouseSinkConnector ckClient =
  SinkConnector { writeRecord = writeRecordToClickHouse ckClient }

valueToCKType :: Aeson.Value -> CK.ClickhouseType
valueToCKType (Aeson.String s) = CK.CKString (DBC.pack $ show s)
valueToCKType (Aeson.Bool b) = if b then CK.CKInt8 1 else CK.CKInt8 0
valueToCKType (Aeson.Number sci) = either CK.CKDecimal64 CK.CKInt64 (floatingOrInteger sci)
valueToCKType _ = error "Not implemented"

writeRecordToClickHouse :: Env () w -> SinkRecord -> IO ()
writeRecordToClickHouse ckClient SinkRecord{..} = do
  let insertMap = Aeson.decode snkValue :: Maybe (HM.HashMap Text.Text Aeson.Value)
  case insertMap of
    Just l -> do
      let !flattened = flattenJSON l
      let keys = "(" <> (intercalate "," . map Text.unpack $ HM.keys flattened) <> ")"
          elems = map valueToCKType $ HM.elems flattened
      let prefix = "INSERT INTO " ++ show snkStream ++ " " ++ keys ++" VALUES "
      Log.debug . Log.buildString $ prefix ++ show elems
      void $ CK.insertOneRow ckClient prefix elems
    _ -> do
      Log.warning "Invalid Sink Value"
