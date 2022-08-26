{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}
{-# LANGUAGE ViewPatterns   #-}

module HStream.Common.RqliteUtils where

import           Data.Aeson              (FromJSON, ToJSON)
import qualified Data.Aeson              as A
import qualified Data.Aeson.Types        as A
import           Data.ByteString         (ByteString)
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Char8   as BSC
import qualified Data.ByteString.Lazy    as BSL
import           Data.Functor            ((<&>))
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import qualified Data.Text.Lazy          as TL
import qualified Data.Text.Lazy.Encoding as TL
import           GHC.Generics            (Generic)
import qualified HStream.Logger          as Log
import qualified Network.HTTP.Client     as H
import qualified Network.HTTP.Types      as H

createTable :: H.Manager -> T.Text -> T.Text -> IO Bool
createTable manager uri tableName = do
  let table = T.encodeUtf8 tableName
  let stmt = "\"CREATE TABLE " <> table
          <> " (id TEXT NOT NULL PRIMARY KEY, value TEXT, version INTEGER)\""
  debug stmt
  req <- mkExecuteHttpRequest uri $ wrapStmt stmt
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  case getResults bsl of
    Nothing -> return False
    Just _  -> return True

deleteTable :: H.Manager -> T.Text -> T.Text -> IO Bool
deleteTable manager uri tableName = do
  let table = T.encodeUtf8 tableName
  let stmt = "\"DROP TABLE " <> table <> "\""
  debug stmt
  req <- mkExecuteHttpRequest uri $ wrapStmt stmt
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  case getResults bsl of
    Nothing -> return False
    Just _  -> return True

insertInto :: ToJSON a => H.Manager -> T.Text -> T.Text -> T.Text -> a -> IO Bool
insertInto manager uri tableName i value = do
  let table = T.encodeUtf8 tableName
      stmt  = wrapStmt $ wrapStmts $ mkInsertStmt table i (A.encode value)
  debug stmt
  req <- mkExecuteHttpRequest uri stmt
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  case getRowAffected bsl of
    Nothing -> return False
    Just n  -> return $ n == 1

updateSet :: ToJSON a => H.Manager -> T.Text -> T.Text -> T.Text -> Maybe Int -> a -> IO Bool
updateSet manager uri tableName i version value  = do
  let t = T.encodeUtf8 tableName
      v = T.encodeUtf8 . T.pack . show <$> version
  let stmt = wrapStmt $ wrapStmts $ mkUpdateStmt t i v (A.encode value)
  debug stmt
  req <- mkExecuteHttpRequest uri stmt
  res@(H.responseBody -> bsl) <- H.httpLbs req manager
  debug res
  debug bsl
  case getRowAffected bsl of
    Nothing -> return False
    Just n  -> return $ n == 1

deleteFrom :: H.Manager -> T.Text -> T.Text -> T.Text -> Maybe Int -> IO Bool
deleteFrom manager uri tableName i version = do
  let t = T.encodeUtf8 tableName
      v = T.encodeUtf8 . T.pack . show <$> version
  let stmt = wrapStmt $ wrapStmts $  mkDeleteStmt t i v
  debug stmt
  req <- mkExecuteHttpRequest uri stmt
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  case getRowAffected bsl of
    Nothing -> return False
    Just n  -> return $ n == 1

selectFrom :: FromJSON a => H.Manager -> T.Text -> T.Text -> Maybe T.Text -> IO (Maybe [a])
selectFrom manager uri tableName i = do
  let table = T.encodeUtf8 tableName
  let stmt = wrapStmt $ wrapStmts $ mkSelectStmtWithKey table i
  debug stmt
  initReq <- H.parseRequest $ T.unpack $ "http://" <> uri
  let req = initReq {
        H.method = "POST"
      , H.path = "/db/query"
      , H.requestHeaders = [(H.hContentType, "application/json")]
      , H.requestBody = H.RequestBodyBS stmt
      }
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  debug $ getResultValue bsl
  return $ getResultValue bsl >>= mapM (A.decode . TL.encodeUtf8)

data Op
  = Check Int
  | Insert T.Text ByteString
  | Update T.Text (Maybe Int) ByteString
  | Delete T.Text (Maybe Int)

type TableNameBS = ByteString
type IdBS        = ByteString
type ValueBS     = ByteString
type VersionBS   = ByteString

mkInsertStmt :: (Show id, Show value) => TableNameBS -> id -> value -> [ByteString]
mkInsertStmt t i p = [ "\"INSERT INTO " <> t <> "(id, value, version) VALUES (?, ?, 1)\""
                      , quotes i , quotes p]

mkUpdateStmt :: (Show id, Show value) => TableNameBS -> id -> Maybe VersionBS -> value -> [ByteString]
mkUpdateStmt t i (Just v) p = [ "\"UPDATE " <> t <> " SET value = ? , version = version + 1 "
                                                 <> "WHERE id = ? AND version = ? \""
                             , quotes p , quotes i, v]
mkUpdateStmt t i Nothing p =  [ "\"UPDATE " <> t <> " SET value = ? , version = version + 1 "
                                                 <> "WHERE id = ? \""
                              , quotes p , quotes i]

mkDeleteStmt :: Show id => TableNameBS -> id -> Maybe VersionBS -> [ByteString]
mkDeleteStmt t i (Just v) = [ "\"DELETE FROM " <> t <> " WHERE id = ? AND version = ? \"", quotes i, v]
mkDeleteStmt t i Nothing  = [ "\"DELETE FROM " <> t <> " WHERE id = ? \"", quotes i]

mkSelectStmtWithKey :: Show id => TableNameBS -> Maybe id -> [ByteString]
mkSelectStmtWithKey t (Just i) = ["\"SELECT value FROM " <> t <> " WHERE id = ? \"", quotes i]
mkSelectStmtWithKey t Nothing  = ["\"SELECT value FROM " <> t <> "\""]

mkExecuteHttpRequest :: T.Text -> ByteString -> IO H.Request
mkExecuteHttpRequest uri requestBody = do
  initReq <- H.parseRequest $ T.unpack $ "http://" <> uri
  return initReq {
        H.method = "POST"
      , H.path = "/db/execute"
      , H.requestHeaders = [(H.hContentType, "application/json")]
      , H.requestBody = H.RequestBodyBS requestBody
      }

wrapStmts :: [ByteString] -> ByteString
wrapStmts bss = "[" <> BS.intercalate (BSC.singleton ',') bss <> "]"

wrapStmt :: ByteString -> ByteString
wrapStmt bs = "[" <> bs <> "]"

quotes :: Show a => a -> ByteString
quotes = T.encodeUtf8 . T.pack . show

decodePrint :: H.Response BSL.ByteString -> IO (Maybe A.Value)
decodePrint resp = do
  let decoded = A.decode (H.responseBody resp) :: Maybe A.Value
  debug decoded
  return decoded

getRowAffected :: BSL.ByteString -> Maybe Int
getRowAffected = A.parseMaybe parseResp
  where
    parseResp (A.decode -> Just obj) = obj A..: "results" >>= (A..: "rows_affected") . head
    parseResp _ = mempty

getResults :: BSL.ByteString -> Maybe [A.Value]
getResults = A.parseMaybe parseResp
  where
    parseResp (A.decode -> Just obj) = obj A..: "results"
    parseResp _                      = mempty

getResultValue :: BSL.ByteString -> Maybe [TL.Text]
getResultValue = A.parseMaybe parseResp
  where
    parseResp (A.decode -> Just obj) = obj A..: "results" >>= (A..: "values") . head <&> (concat :: [[a]] -> [a])
    parseResp _                      = mempty

debug :: Show a => a -> IO ()
debug = Log.debug . Log.buildString'
