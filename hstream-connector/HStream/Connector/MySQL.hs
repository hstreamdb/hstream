{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Connector.MySQL
  ( mysqlSinkConnector
  ) where

import           Control.Monad                (void)
import qualified Data.Aeson                   as Aeson
import qualified Data.ByteString.Lazy.Char8   as DBCL
import qualified Data.HashMap.Strict          as HM
import           Data.List                    (intercalate)
import           Data.Scientific              (floatingOrInteger)
import qualified Data.Text                    as Text
import           Database.MySQL.Base          as MySQL
import           HStream.Utils                (flattenJSON)

import qualified HStream.Logger               as Log
import           HStream.Processing.Connector (SinkConnector (..))
import           HStream.Processing.Type      (SinkRecord (..))

mysqlSinkConnector :: Text.Text -> MySQL.MySQLConn -> SinkConnector
mysqlSinkConnector table myClient =
  SinkConnector { writeRecord = writeRecordToMySQL table myClient }

toMySQLValue :: Aeson.Value -> String
toMySQLValue (Aeson.String s) = "'" ++  show s ++ "'"
toMySQLValue (Aeson.Bool b) = if b then "True" else "False"
toMySQLValue (Aeson.Number sci) = do
  case floatingOrInteger sci of
    Left r  -> show (r :: Double)
    Right i -> show (i :: Int)
toMySQLValue _ = error "Not implemented"

writeRecordToMySQL :: Text.Text -> MySQL.MySQLConn -> SinkRecord -> IO ()
writeRecordToMySQL table myClient SinkRecord{..} = do
  let insertMap = Aeson.decode snkValue :: Maybe (HM.HashMap Text.Text Aeson.Value)
  case insertMap of
    Just l -> do
      let !flattened = flattenJSON l
      let keys = "(" <> (intercalate "," . map Text.unpack $ HM.keys flattened) <> ")"
          elems = "(" <> (intercalate "," . map toMySQLValue $ HM.elems flattened) <> ")"
      let sentence = "INSERT INTO " ++ Text.unpack table ++ " " ++ keys ++ " VALUES " ++ elems
      Log.debug $ Log.buildString sentence
      void $ execute_ myClient $ Query $ DBCL.pack sentence
    _ -> do
      Log.warning "Invalid Sink Value"
