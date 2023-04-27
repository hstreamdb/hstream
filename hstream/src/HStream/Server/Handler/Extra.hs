{-# LANGUAGE DataKinds  #-}
{-# LANGUAGE GADTs      #-}
{-# LANGUAGE LambdaCase #-}

module HStream.Server.Handler.Extra where

import           Control.Exception                (throwIO)
import qualified HsGrpc.Server                    as G
import qualified HsGrpc.Server.Types              as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Exception                as HE
import           HStream.Server.Exception
import           HStream.Server.Handler.Query
import           HStream.Server.HStreamApi        as API
import           HStream.Server.Types             (ServerContext (..))
import           HStream.SQL
import           HStream.Utils

parseSqlHandler
  :: ServerContext
  -> ServerRequest 'Normal API.ParseSqlRequest API.ParseSqlResponse
  -> IO (ServerResponse 'Normal API.ParseSqlResponse)
parseSqlHandler sc (ServerNormalRequest _metadata req) = queryExceptionHandle $
  parseSql req >>= returnResp

handleParseSql :: ServerContext -> G.UnaryHandler API.ParseSqlRequest API.ParseSqlResponse
handleParseSql sc _ req = catchQueryEx $ parseSql req

parseSql :: API.ParseSqlRequest  -> IO API.ParseSqlResponse
parseSql API.ParseSqlRequest{..} = do
  streamCodegen parseSqlRequestSql >>= \case
    SelectPlan sources _ _ _ _ -> return $
      ParseSqlResponse (Just $ ParseSqlResponseSqlEvqSql $ ExecuteViewQuerySql (head sources))
    _ -> throwIO $ HE.SQLNotSupportedByParseSQL parseSqlRequestSql
