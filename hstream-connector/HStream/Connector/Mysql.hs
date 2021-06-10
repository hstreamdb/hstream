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

import qualified Z.IO.Logger                  as Log

import           HStream.Processing.Connector (SinkConnector (..))
import           HStream.Processing.Type      (SinkRecord (..))

mysqlSinkConnector :: MySQL.MySQLConn -> SinkConnector
mysqlSinkConnector myClient =
  SinkConnector { writeRecord = writeRecordToMySQL myClient }

toMySQLValue :: Aeson.Value -> String
toMySQLValue (Aeson.String s) = "'" ++  show s ++ "'"
toMySQLValue (Aeson.Bool b) = if b then "True" else "False"
toMySQLValue (Aeson.Number sci) = do
  case floatingOrInteger sci of
    Left r  -> show (r :: Double)
    Right i -> show (i :: Int)
toMySQLValue _ = error "Not implemented"

writeRecordToMySQL :: MySQL.MySQLConn -> SinkRecord -> IO ()
writeRecordToMySQL myClient SinkRecord{..} = do
  let insertMap = Aeson.decode snkValue :: Maybe (HM.HashMap Text.Text Aeson.Value)
  case insertMap of
    Just l -> do
      let !flattened = flattenJSON l
      let keys = "(" <> (intercalate "," . map Text.unpack $ HM.keys flattened) <> ")"
          elems = "(" <> (intercalate "," . map toMySQLValue $ HM.elems flattened) <> ")"
      void $ execute_ myClient $ Query $ DBCL.pack ("INSERT INTO " ++ Text.unpack snkStream ++ " " ++ keys ++ " VALUES " ++ elems)
    _ -> do
      Log.warning "Invalid Sink Value"
