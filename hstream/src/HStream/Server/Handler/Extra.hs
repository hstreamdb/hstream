{-# LANGUAGE DataKinds  #-}
{-# LANGUAGE GADTs      #-}
{-# LANGUAGE LambdaCase #-}

module HStream.Server.Handler.Extra where

import           Control.Exception                (throwIO)
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.Server.Core.Extra        (getVersion)
import           HStream.Server.Exception
import           HStream.Server.Handler.Query
import           HStream.Server.HStreamApi        as API
import           HStream.Server.Types             (ServerContext (..))
import           HStream.SQL
import           HStream.Utils
import           Proto3.Suite.Class               (HasDefault (def))

parseSqlHandler
  :: ServerContext
  -> ServerRequest 'Normal API.ParseSqlRequest API.ParseSqlResponse
  -> IO (ServerResponse 'Normal API.ParseSqlResponse)
parseSqlHandler _ (ServerNormalRequest _metadata req) = queryExceptionHandle $
  parseSql req >>= returnResp

handleParseSql :: ServerContext -> G.UnaryHandler API.ParseSqlRequest API.ParseSqlResponse
handleParseSql _ _ req = catchQueryEx $ parseSql req

parseSql :: API.ParseSqlRequest  -> IO API.ParseSqlResponse
parseSql API.ParseSqlRequest{..} = do
  streamCodegen parseSqlRequestSql >>= \case
    SelectPlan sources _ _ _ -> return $
      ParseSqlResponse (Just $ ParseSqlResponseSqlEvqSql $ ExecuteViewQuerySql (head sources))
    _ -> throwIO $ HE.SQLNotSupportedByParseSQL parseSqlRequestSql

getVersionHandler
  :: ServerContext
  -> ServerRequest 'Normal API.GetVersionRequest API.GetVersionResponse
  -> IO (ServerResponse 'Normal API.GetVersionResponse)
getVersionHandler _ _ = defaultExceptionHandle $ do
    version <- getVersion
    Log.info $ "get server version: " <> Log.build (show version)
    returnResp $ def { getVersionResponseVersion = Just version }

handleGetVersion :: ServerContext -> G.UnaryHandler API.GetVersionRequest API.GetVersionResponse
handleGetVersion _ _ _ = catchDefaultEx $ do
    version <- getVersion
    Log.info $ "get server version: " <> Log.build (show version)
    return $ def { getVersionResponseVersion = Just version }

