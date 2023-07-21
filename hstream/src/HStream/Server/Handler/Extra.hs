{-# LANGUAGE CPP        #-}
{-# LANGUAGE DataKinds  #-}
{-# LANGUAGE GADTs      #-}
{-# LANGUAGE LambdaCase #-}

module HStream.Server.Handler.Extra where

import           Control.Exception                (throwIO)
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Exception                as HE
import           HStream.Server.Handler.Query
import           HStream.Server.HStreamApi        as API
import qualified HStream.Server.MetaData.Types    as P
import           HStream.Server.Types             (ServerContext (..))
import           HStream.SQL
import           HStream.Utils

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
#ifdef HStreamEnableSchema
  streamCodegen parseSqlRequestSql P.getSchema >>= \case
#else
  streamCodegen parseSqlRequestSql             >>= \case
#endif
    SelectPlan sources _ _ _ -> return $
      ParseSqlResponse (Just $ ParseSqlResponseSqlEvqSql $ ExecuteViewQuerySql (head sources))
    _ -> throwIO $ HE.SQLNotSupportedByParseSQL parseSqlRequestSql
