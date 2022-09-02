{-# LANGUAGE ViewPatterns #-}

module HStream.MetaStore.RqliteUtils where

import           Data.Aeson              (FromJSON, ToJSON)
import qualified Data.Aeson              as A
import qualified Data.Aeson.Types        as A
import           Data.ByteString         (ByteString)
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Char8   as BSC
import qualified Data.ByteString.Lazy    as BL
import           Data.Functor            ((<&>))
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import qualified Data.Text.Lazy          as TL
import qualified Data.Text.Lazy.Encoding as TL
import qualified Network.HTTP.Client     as H
import qualified Network.HTTP.Types      as H

import qualified HStream.Logger          as Log

type Id           = T.Text
type Url          = T.Text
type Version      = Int
type TableName    = T.Text
type Statement    = ByteString
type EncodedValue = ByteString

data ROp
  = CheckROp  TableName Id Version
  | InsertROp TableName Id EncodedValue
  | UpdateROp TableName Id EncodedValue (Maybe Version)
  | DeleteROp TableName Id (Maybe Version)

createTable :: H.Manager -> Url -> TableName -> IO Bool
createTable manager uri tableName = do
  let table = T.encodeUtf8 tableName
  let stmt = "\"CREATE TABLE " <> table
          <> " (id TEXT NOT NULL PRIMARY KEY, value TEXT, version INTEGER)\""
  debug stmt
  req <- mkExecuteHttpRequest uri $ wrapStmt stmt
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  case getError bsl of
    Nothing -> return True
    Just _  -> return False

deleteTable :: H.Manager -> Url -> TableName -> IO Bool
deleteTable manager uri tableName = do
  let table = T.encodeUtf8 tableName
  let stmt = "\"DROP TABLE " <> table <> "\""
  debug stmt
  req <- mkExecuteHttpRequest uri $ wrapStmt stmt
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  case getError bsl of
    Nothing -> return True
    Just _  -> return False

insertInto :: ToJSON a => H.Manager -> Url -> TableName -> Id -> a -> IO Bool
insertInto manager uri t i value = do
  let stmt = wrapStmt $ wrapStmts $ mkInsertStmt t i (BL.toStrict $ A.encode value)
  debug stmt
  req <- mkExecuteHttpRequest uri stmt
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  case getRowAffected bsl of
    Nothing -> return False
    Just n  -> return $ n == 1

updateSet :: ToJSON a => H.Manager -> Url -> TableName -> Id -> a -> Maybe Version -> IO Bool
updateSet manager uri t i value v = do
  let stmt = wrapStmt $ wrapStmts $ mkUpdateStmt t i (BL.toStrict $ A.encode value) v
  debug stmt
  req <- mkExecuteHttpRequest uri stmt
  res@(H.responseBody -> bsl) <- H.httpLbs req manager
  debug res
  debug bsl
  case getRowAffected bsl of
    Nothing -> return False
    Just n  -> return $ n == 1

deleteFrom :: H.Manager -> Url -> TableName -> Id -> Maybe Version -> IO Bool
deleteFrom manager uri t i v = do
  let stmt = wrapStmt $ wrapStmts $  mkDeleteStmt t i v
  debug stmt
  req <- mkExecuteHttpRequest uri stmt
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  case getRowAffected bsl of
    Nothing -> return False
    Just n  -> return $ n == 1

selectFrom :: FromJSON a => H.Manager -> Url -> TableName -> Maybe Id -> IO (Maybe [a])
selectFrom manager uri table i = do
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

transaction :: H.Manager -> Url -> [ROp] -> IO Bool
transaction manager uri ops = do
  let stmt = wrapStmts $ rOp2Stmt <$> ops
  debug stmt
  initReq <- H.parseRequest $ T.unpack $ "http://" <> uri
  let req = initReq {
        H.method = "POST"
      , H.path = "/db/execute?transaction"
      , H.requestHeaders = [(H.hContentType, "application/json")]
      , H.requestBody = H.RequestBodyBS stmt
      }
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  debug $ getResults bsl
  case getError bsl of
    Nothing -> return True
    Just _  -> return False

--------------------------------------------------------------------------------
-- Utils

rOp2Stmt :: ROp -> Statement
rOp2Stmt (CheckROp  t i v)    = wrapStmts $ mkCheckStmt t i v
rOp2Stmt (InsertROp t i v)    = wrapStmts $ mkInsertStmt t i v
rOp2Stmt (DeleteROp t i mv)   = wrapStmts $ mkDeleteStmt t i mv
rOp2Stmt (UpdateROp t i mv v) = wrapStmts $ mkUpdateStmt t i mv v

mkInsertStmt :: TableName -> Id -> EncodedValue -> [Statement]
mkInsertStmt t i p = [ "\"INSERT INTO " <> T.encodeUtf8 t <> "(id, value, version) VALUES (?, ?, 1)\""
                      , quotes i , quotes p]

mkUpdateStmt :: TableName -> Id -> EncodedValue -> Maybe Version -> [Statement]
mkUpdateStmt t i p (Just v) = [ "\"UPDATE " <> T.encodeUtf8 t <> " SET value = ? , version = version + 1 "
                                                 <> "WHERE id = ? AND version = ? \""
                             , quotes p , quotes i, quotes v]
mkUpdateStmt t i p Nothing  =  [ "\"UPDATE " <> T.encodeUtf8 t <> " SET value = ? , version = version + 1 "
                                                 <> "WHERE id = ? \""
                              , quotes p , quotes i]

mkDeleteStmt :: TableName -> Id -> Maybe Version -> [Statement]
mkDeleteStmt t i (Just v) = [ "\"DELETE FROM " <> T.encodeUtf8 t <> " WHERE id = ? AND version = ? \"", quotes i, quotes v]
mkDeleteStmt t i Nothing  = [ "\"DELETE FROM " <> T.encodeUtf8 t <> " WHERE id = ? \"", quotes i]

mkSelectStmtWithKey :: TableName -> Maybe Id -> [Statement]
mkSelectStmtWithKey t (Just i) = ["\"SELECT value FROM " <> T.encodeUtf8 t <> " WHERE id = ? \"", quotes i]
mkSelectStmtWithKey t Nothing  = ["\"SELECT value FROM " <> T.encodeUtf8 t <> "\""]

mkCheckStmt :: TableName -> Id -> Version -> [Statement]
mkCheckStmt t i v = [ "\"INSERT INTO " <> T.encodeUtf8 t <> "(id, version) "
                 <> "SELECT id, version FROM "<> T.encodeUtf8 t
                 <> " WHERE (NOT version = ?) AND id = ?;\"" , quotes v, quotes i]

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

decodePrint :: H.Response BL.ByteString -> IO (Maybe A.Value)
decodePrint resp = do
  let decoded = A.decode (H.responseBody resp) :: Maybe A.Value
  debug decoded
  return decoded

getRowAffected :: BL.ByteString -> Maybe Int
getRowAffected = A.parseMaybe parseResp
  where
    parseResp (A.decode -> Just obj) = obj A..: "results" >>= (A..: "rows_affected") . head
    parseResp _ = mempty

getResults :: BL.ByteString -> Maybe [A.Value]
getResults = A.parseMaybe parseResp
  where
    parseResp (A.decode -> Just obj) = obj A..: "results"
    parseResp _                      = mempty

getError :: BL.ByteString -> Maybe TL.Text
getError = A.parseMaybe parseResp
  where
    parseResp (A.decode -> Just obj) = obj A..: "results" >>= (A..: "error") . head
    parseResp _ = mempty

getResultValue :: BL.ByteString -> Maybe [TL.Text]
getResultValue = A.parseMaybe parseResp
  where
    parseResp (A.decode -> Just obj) = obj A..: "results" >>= (A..: "values") . head <&> (concat :: [[a]] -> [a])
    parseResp _                      = mempty

debug :: Show a => a -> IO ()
debug = Log.debug . Log.buildString'
