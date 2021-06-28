{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module HStream.HTTP.Server.Query (
  QueriesAPI, queryServer
) where

import           Control.Concurrent           (forkIO, killThread)
import           Control.Exception            (SomeException, catch, try)
import           Control.Monad                (void)
import           Control.Monad.IO.Class       (liftIO)
import           Data.Aeson                   (FromJSON, ToJSON)
import           Data.Int                     (Int64)
import           Data.List                    (find)
import qualified Data.Map.Strict              as Map
import           Data.Swagger                 (ToSchema)
import qualified Data.Text                    as T
import           GHC.Generics                 (Generic)
import           Servant                      (Capture, Delete, Get, JSON,
                                               PlainText, Post, ReqBody,
                                               type (:>), (:<|>) (..))
import           Servant.Server               (Handler, Server)
import           Z.Data.Builder.Base          (string8)
import qualified Z.Data.CBytes                as ZDC
import qualified Z.Data.Text                  as ZT
import qualified Z.IO.Logger                  as Log
import           Z.IO.Time                    (SystemTime (..), getSystemTime')
import qualified ZooKeeper.Types              as ZK

import qualified HStream.Connector.HStore     as HCH
import           HStream.Processing.Connector (subscribeToStream)
import           HStream.Processing.Processor (getTaskName, taskBuilderWithName)
import           HStream.Processing.Type      (Offset (..))
import qualified HStream.SQL.Codegen          as HSC
import           HStream.SQL.Exception        (SomeSQLException)
import           HStream.Server.Handler       (runTaskWrapper)
import qualified HStream.Server.Persistence   as HSP
import qualified HStream.Store                as HS
import           HStream.Utils.Converter      (cbytesToText, textToCBytes)

-- BO is short for Business Object
data QueryBO = QueryBO
  { id          :: T.Text
  , status      :: Maybe Int
  , createdTime :: Maybe Int64
  , queryText   :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON QueryBO
instance FromJSON QueryBO
instance ToSchema QueryBO

type QueriesAPI =
  "queries" :> Get '[JSON] [QueryBO]
  :<|> "queries" :> "restart" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "queries" :> "cancel" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "queries" :> ReqBody '[JSON] QueryBO :> Post '[JSON] QueryBO
  :<|> "queries" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "queries" :> Capture "name" String :> Get '[JSON] (Maybe QueryBO)

hstreamQueryToQueryBO :: HSP.Query -> QueryBO
hstreamQueryToQueryBO (HSP.Query queryId (HSP.Info sqlStatement createdTime) (HSP.Status status _)) =
  QueryBO (cbytesToText queryId) (Just $ fromEnum status) (Just createdTime) (T.pack $ ZT.unpack sqlStatement)

hstreamQueryNameIs :: T.Text -> HSP.Query -> Bool
hstreamQueryNameIs name (HSP.Query queryId _ _) = (cbytesToText queryId) == name

removeQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
removeQueryHandler ldClient zkHandle name = liftIO $ catch
  ((HSP.withMaybeZHandle zkHandle $ HSP.removeQuery (ZDC.pack name)) >> return True)
  (\(e :: SomeException) -> return False)

-- TODO: we should remove the duplicate code in HStream/Admin/Server/Query.hs and HStream/Server/Handler.hs
createQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> (Int, ZDC.CBytes) -> QueryBO -> Handler QueryBO
createQueryHandler ldClient zkHandle (streamRepFactor, checkpointRootPath) query = do
  err <- liftIO $ do
    plan' <- try $ HSC.streamCodegen $ queryText query
    case plan' of
      Left  (_ :: SomeSQLException) -> return $ Just "exception on parsing or codegen"
      Right (HSC.SelectPlan sources sink taskBuilder) -> do
        let taskBuilder' = taskBuilderWithName taskBuilder $ HStream.HTTP.Server.Query.id query
        exists <- mapM (HS.doesStreamExists ldClient . HCH.transToStreamName) sources
        if (not . and) exists then return $ Just "some source stream do not exist"
        else do
          e' <- try $ HS.createTempStream ldClient (HCH.transToTempStreamName sink)
            (HS.LogAttrs $ HS.HsLogAttrs streamRepFactor Map.empty)
          case e' of
            Left (_ :: SomeException) -> return $ Just "error when creating sink stream."
            Right _                   -> do
              -- create persistent query
              MkSystemTime timestamp _ <- getSystemTime'
              let qid = ZDC.pack $ T.unpack $ getTaskName taskBuilder'
                  qinfo = HSP.Info (ZT.pack $ T.unpack $ queryText query) timestamp
              HSP.withMaybeZHandle zkHandle $ HSP.insertQuery qid qinfo
              -- run task
              _ <- forkIO $ HSP.withMaybeZHandle zkHandle (HSP.setQueryStatus qid HSP.Running)
                >> runTaskWrapper True taskBuilder' ldClient
              ldreader' <- HS.newLDFileCkpReader ldClient
                (textToCBytes (T.append (getTaskName taskBuilder') "-result"))
                checkpointRootPath 1 Nothing 3
              let sc = HCH.hstoreTempSourceConnector ldClient ldreader'
              subscribeToStream sc sink Latest
              return Nothing
      Right _ -> return $ Just "inconsistent method called"
      -- TODO: return error code
  case err of
    Just err -> liftIO $ Log.fatal . string8 $ err
    Nothing  -> return ()
  return query

fetchQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> Handler [QueryBO]
fetchQueryHandler ldClient zkHandle = do
  -- queryNames <- liftIO $ findQueries ldClient True
  queries <- liftIO $ HSP.withMaybeZHandle zkHandle HSP.getQueries
  return $ map hstreamQueryToQueryBO queries

getQueryHandler :: Maybe ZK.ZHandle -> String -> Handler (Maybe QueryBO)
getQueryHandler zkHandle name = do
  query <- liftIO $ do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    return $ find (hstreamQueryNameIs (T.pack name)) queries
  return $ hstreamQueryToQueryBO <$> query

-- Question: What else should I do to really restart the query? Unsubscribe and Stop CkpReader?
restartQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
restartQueryHandler ldClient zkHandle name = do
  res <- liftIO $ do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    case find (hstreamQueryNameIs (T.pack name)) queries of
      Just query -> do
        _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setQueryStatus (HSP.queryId query) HSP.Running)
        return True
      Nothing -> return False
  return res

cancelQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
cancelQueryHandler ldClient zkHandle name = do
  res <- liftIO $ do
    queries <- HSP.withMaybeZHandle zkHandle HSP.getQueries
    case find (hstreamQueryNameIs (T.pack name)) queries of
      Just query -> do
        _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setQueryStatus (HSP.queryId query) HSP.Terminated)
        return True
      Nothing -> return False
  return res

queryServer :: HS.LDClient -> Maybe ZK.ZHandle -> (Int, ZDC.CBytes) -> Server QueriesAPI
queryServer ldClient zkHandle (streamRepFactor, checkpointRootPath) =
  (fetchQueryHandler ldClient zkHandle)
  :<|> (restartQueryHandler ldClient zkHandle)
  :<|> (cancelQueryHandler ldClient zkHandle)
  :<|> (createQueryHandler ldClient zkHandle (streamRepFactor, checkpointRootPath))
  :<|> (removeQueryHandler ldClient zkHandle)
  :<|> (getQueryHandler zkHandle)
