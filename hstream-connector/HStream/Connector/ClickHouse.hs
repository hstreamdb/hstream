{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Connector.ClickHouse
  ( clickHouseSinkConnector
  ) where

import           Control.Monad                   (join, void)
import qualified Data.Aeson                      as Aeson
import           Data.Bifunctor                  (first)
import qualified Data.ByteString.Char8           as DBC
import qualified Data.HashMap.Strict             as HM
import           Data.List                       (intercalate)
import           Data.Scientific                 (floatingOrInteger)
import           Data.Text                       (Text)
import qualified Data.Text                       as Text
import qualified Database.ClickHouseDriver       as CK
import qualified Database.ClickHouseDriver.Types as CK
import           Haxl.Core                       (Env)
import qualified Z.IO.Logger                     as Log

import           HStream.Processing.Connector    (SinkConnector (..))
import           HStream.Processing.Type         (SinkRecord (..))

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
      let !flattened = flatten l
      let keys = "(" <> (intercalate "," . map Text.unpack $ HM.keys flattened) <> ")"
          elems = map valueToCKType $ HM.elems flattened
      void $ CK.insertOneRow ckClient ("INSERT INTO " ++ show snkStream ++ " " ++ keys ++" VALUES ") elems
    _ -> do
      Log.warning "Invalid Sink Value"

-- | Flatten all JSON structures.
--
-- >>> flatten (HM.fromList [("a", Aeson.Object $ HM.fromList [("b", Aeson.Number 1)])])
-- fromList [("a.b",Number 1.0)]
flatten :: HM.HashMap Text Aeson.Value -> HM.HashMap Text Aeson.Value
flatten jsonMap =
  let flattened = join $ map (flatten' "." Text.empty) (HM.toList jsonMap)
   in HM.fromList $ map (first Text.tail) flattened

flatten' :: Text -> Text -> (Text, Aeson.Value) -> [(Text, Aeson.Value)]
flatten' splitor prefix (k, v) = do
  -- TODO: we will not support array?
  case v of
    Aeson.Object o -> join $ map (flatten' splitor (prefix <> splitor <> k)) (HM.toList o)
    _              -> [(prefix <> splitor <> k, v)]
