{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns  #-}

module HStream.MetaStore.RqliteUtils where

import           Control.Exception       (throw, throwIO, try)
import           Control.Monad           (forM, forM_, when)
import           Data.Aeson              (FromJSON, ToJSON)
import qualified Data.Aeson              as A
import qualified Data.Aeson.Types        as A
import qualified Data.Attoparsec.Text    as AT
import           Data.ByteString         (ByteString)
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Char8   as BSC
import qualified Data.ByteString.Lazy    as BL
import qualified Data.Map.Strict         as Map
import           Data.Maybe              (fromMaybe)
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import qualified Data.Text.Lazy          as TL
import qualified Data.Text.Lazy.Encoding as TL
import qualified Network.HTTP.Client     as H
import qualified Network.HTTP.Types      as H

import           HStream.Exception
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
  | UpdateROp TableName Id EncodedValue
  | DeleteROp TableName Id
  | ExistROp  TableName Id

createTable :: H.Manager -> Url -> TableName -> IO ()
createTable manager uri tableName = do
  let table = T.encodeUtf8 tableName
  let stmt = wrapStmt $ "\"CREATE TABLE " <> table
          <> " (id TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL, version INTEGER)\""
  debug stmt
  httpExecute manager uri stmt False mempty

deleteTable :: H.Manager -> Url -> TableName -> IO ()
deleteTable manager uri tableName = do
  let table = T.encodeUtf8 tableName
  let stmt = wrapStmt $ "\"DROP TABLE " <> table <> "\""
  debug stmt
  httpExecute manager uri stmt False mempty

insertInto :: ToJSON a => H.Manager -> Url -> TableName -> Id -> a -> IO ()
insertInto manager uri t i value = do
  let stmt = wrapStmt $ wrapStmts $ mkInsertStmt t i (BL.toStrict $ A.encode value)
  debug stmt
  httpExecute manager uri stmt True (i <> "@" <> t)

updateSetIfExists :: ToJSON a => H.Manager -> Url -> TableName -> Id -> a -> Maybe Version -> IO ()
updateSetIfExists manager uri t i value ver = do
  let stmt = wrapStmt $ wrapStmts $ mkUpdateStmt t i (BL.toStrict $ A.encode value) ver
  debug stmt
  httpExecute manager uri stmt True (i <> "@" <> t)

updateSet :: ToJSON a => H.Manager -> Url -> TableName -> Id -> a -> Maybe Version -> IO ()
updateSet manager uri t i v ver = transaction manager uri $
  ExistROp t i : case ver of
    Nothing   -> [op]
    Just ver' -> [CheckROp t i ver', op]
  where
    op = UpdateROp t i (BL.toStrict $ A.encode v)

deleteFromIfExists :: H.Manager -> Url -> TableName -> Id -> Maybe Version -> IO ()
deleteFromIfExists manager uri t i v = do
  let stmt = wrapStmt $ wrapStmts $  mkDeleteStmt t i v
  debug stmt
  httpExecute manager uri stmt True (i <> "@" <> t)

deleteFrom :: H.Manager -> Url -> TableName -> Id -> Maybe Version -> IO ()
deleteFrom manager uri t i ver = transaction manager uri $
  ExistROp t i : case ver of
    Nothing   -> [DeleteROp t i]
    Just ver' -> [CheckROp t i ver', DeleteROp t i]

selectFrom :: FromJSON a => H.Manager -> Url -> TableName -> Maybe Id -> IO (Map.Map Id a)
selectFrom manager uri table i = catchHttpException $ do
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
  ivs <- getRqSelectIdValue bsl
  debug ivs
  idValues <- case mapM (\(x, y) -> (TL.toStrict x,) <$> (A.decode . TL.encodeUtf8) y) ivs of
    Nothing -> throwIO . RQLiteDecodeErr . T.unpack . TL.toStrict . TL.concat . map snd $ ivs
    Just xs -> pure xs
  pure $ Map.fromList idValues

transaction :: H.Manager -> Url -> [ROp] -> IO ()
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
  let results = getResults bsl
  debug results
  forM_ results $ \case
    Err e  -> throwRqHttpErrMsg e ""
    Ok _ _ -> pure ()

--------------------------------------------------------------------------------
-- Utils

rOp2Stmt :: ROp -> Statement
rOp2Stmt (CheckROp  t i v) = wrapStmts $ mkCheckVerStmt t i v
rOp2Stmt (InsertROp t i v) = wrapStmts $ mkInsertStmt t i v
rOp2Stmt (UpdateROp t i v) = wrapStmts $ mkUpdateStmt t i v Nothing
rOp2Stmt (DeleteROp t i)   = wrapStmts $ mkDeleteStmt t i Nothing
rOp2Stmt (ExistROp t i)    = wrapStmts $ mkCheckExistStmt t i

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
mkSelectStmtWithKey t (Just i) = ["\"SELECT id, value FROM " <> T.encodeUtf8 t <> " WHERE id = ? \"", quotes i]
mkSelectStmtWithKey t Nothing  = ["\"SELECT id, value FROM " <> T.encodeUtf8 t <> "\""]

mkCheckVerStmt :: TableName -> Id -> Version -> [Statement]
mkCheckVerStmt t i v = [ "\"INSERT INTO " <> T.encodeUtf8 t
                     <> " SELECT id, NULL, version FROM " <> T.encodeUtf8 t
                     <> " WHERE (NOT version = ?) AND id = ?; \""
                     , quotes v, quotes i]

mkCheckExistStmt :: TableName -> Id -> [Statement]
mkCheckExistStmt t i = [ "\"INSERT INTO " <> T.encodeUtf8 t
                     <> " SELECT id, NULL, version FROM " <> T.encodeUtf8 t
                     <> " WHERE NOT EXISTS"
                     <> "(SELECT id FROM " <> T.encodeUtf8 t <> " WHERE id = ?) LIMIT 1;\""
                     ,  quotes i]

wrapStmts :: [ByteString] -> ByteString
wrapStmts bss = "[" <> BS.intercalate (BSC.singleton ',') bss <> "]"

wrapStmt :: ByteString -> ByteString
wrapStmt bs = "[" <> bs <> "]"

quotes :: Show a => a -> ByteString
quotes = T.encodeUtf8 . T.pack . show

--------------------------------------------------------------------------------

mkExecuteHttpRequest :: T.Text -> ByteString -> IO H.Request
mkExecuteHttpRequest uri requestBody = do
  initReq <- H.parseRequest $ T.unpack $ "http://" <> uri
  return initReq {
        H.method = "POST"
      , H.path = "/db/execute"
      , H.requestHeaders = [(H.hContentType, "application/json")]
      , H.requestBody = H.RequestBodyBS requestBody
      }

httpExecute :: H.Manager -> Url -> ByteString -> Bool -> T.Text -> IO ()
httpExecute manager uri stmt flag msg = catchHttpException $ do
  req <- mkExecuteHttpRequest uri stmt
  (H.responseBody -> bsl) <- H.httpLbs req manager
  debug bsl
  getRqResult bsl flag msg

--------------------------------------------------------------------------------

type Results = [Result]

data Result
  = Ok Int [[TL.Text]]
  | Err T.Text
  deriving Show

getResults :: BL.ByteString -> Results
getResults = fromMaybe [] . A.parseMaybe parseResp
  where
    parseResp (A.decode -> Just (obj :: A.Object)) = do
      results <- obj A..: "results"
      forM results $ \resObj -> do
        rows_affected <- resObj A..:? "rows_affected" A..!= 0
        errorMsg <- resObj A..:? "error"
        values <- resObj A..:? "values" A..!= []
        return $ case errorMsg of
          Just x  -> Err x
          Nothing -> Ok rows_affected values
    parseResp _ = throw $ RQLiteUnspecifiedErr "Invalid JSON format from rqlite server"

getRqResult :: BL.ByteString -> Bool -> T.Text -> IO ()
getRqResult bsl requireResults msg = case getResults bsl of
  x:_ -> case x of
    -- FIXME：case 0 could also be BADVERSION
    -- delete & update with version needs to be reimplemented with transaction
    -- in order to identify bad version exception
    Ok 0 _ -> when requireResults . throwIO . RQLiteRowNotFound $ T.unpack msg
    Ok 1 _ -> pure ()
    Ok _ _ -> throwIO $ RQLiteUnspecifiedErr "more rows are affected"
    Err e  -> throwRqHttpErrMsg e msg
  _ -> throwIO $ RQLiteUnspecifiedErr "No result in response from rqlite server"

getRqSelectIdValue :: BL.ByteString -> IO [(TL.Text, TL.Text)]
getRqSelectIdValue bsl = case getResults bsl of
  x:_ -> case x of
    Ok _ ivs -> pure $ map (\(i:v:_) -> (i, v)) ivs
    Err e    -> throwRqHttpErrMsg e ""
  _ -> throwIO $ RQLiteUnspecifiedErr "No result in response from rqlite server"

catchHttpException :: IO a -> IO a
catchHttpException io = returnRes =<< try io
 where
  returnRes = \case
    Left ee  -> returnErr ee
    Right rr -> return rr
  returnErr = \case
    H.HttpExceptionRequest _ content -> throwIO $ RQLiteNetworkErr (show content)
    H.InvalidUrlException url reason -> throwIO $ RQLiteNetworkErr $ "Invalid uri: " <> url <> " " <> reason

throwRqHttpErrMsg :: T.Text -> T.Text -> IO a
throwRqHttpErrMsg txt msg = do
  case AT.parseOnly parser1 txt of
    Right e -> throwIO e; Left _  -> case AT.parseOnly parser2 txt of
      Right e -> throwIO e; Left _  -> case AT.parseOnly parser3 txt of
        Right e -> throwIO e; Left _  -> case AT.parseOnly parser4 txt of
          Right e -> throwIO e; Left _  -> throwIO $ RQLiteUnspecifiedErr $ T.unpack txt
  where
    parser1 = RQLiteTableNotFound . T.unpack <$> AT.string tableNotFound <* AT.takeText
    parser2 = RQLiteRowAlreadyExists . T.unpack <$> AT.string rowExists <* AT.takeText
    parser3 = RQLiteTableAlreadyExists . T.unpack <$  AT.string "table "
                                                  <*> AT.takeTill (== ' ')
                                                  <*  AT.string " already exists"
    parser4 = RQLiteRowNotFound . T.unpack <$ AT.string rowNotFound <*> pure msg
--------------------------------------------------------------------------------

debug :: Show a => a -> IO ()
debug = Log.debug . Log.buildString'

tableNotFound :: T.Text
tableNotFound = "not such table:"

rowExists :: T.Text
rowExists = "UNIQUE constraint failed:"

rowNotFound :: T.Text
rowNotFound = "NOT NULL constraint failed:"
