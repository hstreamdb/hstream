{-# LANGUAGE CPP               #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.Handler.Schema where

#ifdef HStreamEnableSchema
import           Control.Exception
import qualified Data.Text                        as T
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (catchDefaultEx,
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import qualified HStream.Server.MetaData.Types    as P
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (returnResp)

registerSchemaHandler :: ServerContext
                      -> ServerRequest 'Normal Schema Empty
                      -> IO (ServerResponse 'Normal Empty)
registerSchemaHandler sc (ServerNormalRequest _metadata schema) = defaultExceptionHandle $ do
  Log.debug $ "Receive Register Schema Request: " <> Log.buildString' schema
  P.registerSchema (metaHandle sc) (P.schemaToHStreamSchema schema)
  returnResp Empty

handleRegisterSchema :: ServerContext
                     -> G.UnaryHandler Schema Empty
handleRegisterSchema sc _ schema = catchDefaultEx $ do
  Log.debug $ "Receive Register Schema Request: " <> Log.buildString' schema
  P.registerSchema (metaHandle sc) (P.schemaToHStreamSchema schema)
  pure Empty

getSchemaHandler :: ServerContext
                 -> ServerRequest 'Normal GetSchemaRequest Schema
                 -> IO (ServerResponse 'Normal Schema)
getSchemaHandler sc (ServerNormalRequest _metadata (GetSchemaRequest schemaOwner)) = defaultExceptionHandle $ do
  Log.debug $ "Receive Get Schema Request: " <> Log.buildString' schemaOwner
  hs_schema_m <- P.getSchema (metaHandle sc) schemaOwner
  case hs_schema_m of
    Nothing -> throwIO $ HE.LocalMetaStoreObjectNotFound (T.unpack schemaOwner)
    Just hs_schema -> returnResp (P.hstreamSchemaToSchema hs_schema)

handleGetSchema :: ServerContext
                -> G.UnaryHandler GetSchemaRequest Schema
handleGetSchema sc _ (GetSchemaRequest schemaOwner) = catchDefaultEx $ do
  Log.debug $ "Receive Get Schema Request: " <> Log.buildString' schemaOwner
  hs_schema_m <- P.getSchema (metaHandle sc) schemaOwner
  case hs_schema_m of
    Nothing -> throwIO $ HE.LocalMetaStoreObjectNotFound (T.unpack schemaOwner)
    Just hs_schema -> return (P.hstreamSchemaToSchema hs_schema)

unregisterSchemaHandler :: ServerContext
                        -> ServerRequest 'Normal UnregisterSchemaRequest Empty
                        -> IO (ServerResponse 'Normal Empty)
unregisterSchemaHandler sc (ServerNormalRequest _metadata (UnregisterSchemaRequest schemaOwner)) = defaultExceptionHandle $ do
  Log.debug $ "Receive Unregister Schema Request: " <> Log.buildString' schemaOwner
  P.unregisterSchema (metaHandle sc) schemaOwner
  returnResp Empty

handleUnregisterSchema :: ServerContext
                       -> G.UnaryHandler UnregisterSchemaRequest Empty
handleUnregisterSchema sc _ (UnregisterSchemaRequest schemaOwner) = catchDefaultEx $ do
  Log.debug $ "Receive Unregister Schema Request: " <> Log.buildString' schemaOwner
  P.unregisterSchema (metaHandle sc) schemaOwner
  pure Empty
#endif
