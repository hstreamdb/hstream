{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module HStream.Server.Exception
  ( -- * for grpc-haskell
    defaultHandlers
  , defaultExceptionHandle
  , defaultServerStreamExceptionHandle
  , defaultBiDiStreamExceptionHandle
    -- * for hs-grpc-server
  , defaultExHandlers
  , catchDefaultEx
  ) where

import           Control.Concurrent.Async      (AsyncCancelled (..))
import           Control.Exception             (Handler (Handler), IOException,
                                                SomeException, catches)
import qualified HsGrpc.Server.Types           as HsGrpc
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server (ServerResponse (ServerBiDiResponse, ServerWriterResponse))

import qualified HStream.Exception             as HE
import qualified HStream.Logger                as Log
import qualified HStream.Store                 as Store
import           HStream.Utils                 (mkServerErrResp)

--------------------------------------------------------------------------------

defaultExceptionHandle :: HE.ExceptionHandle (ServerResponse 'Normal a)
defaultExceptionHandle = HE.mkExceptionHandle $
  HE.setRespType mkServerErrResp defaultHandlers

defaultServerStreamExceptionHandle :: HE.ExceptionHandle (ServerResponse 'ServerStreaming a)
defaultServerStreamExceptionHandle = HE.mkExceptionHandle $
  HE.setRespType (ServerWriterResponse mempty) defaultHandlers

defaultBiDiStreamExceptionHandle :: HE.ExceptionHandle (ServerResponse 'BiDiStreaming a)
defaultBiDiStreamExceptionHandle = HE.mkExceptionHandle $
  HE.setRespType (ServerBiDiResponse mempty) defaultHandlers

defaultHandlers :: [Handler (StatusCode, StatusDetails)]
defaultHandlers = HE.defaultHServerExHandlers
               ++ storeExceptionHandlers
               ++ HE.zkExceptionHandlers
               ++ finalExceptionHandlers

finalExceptionHandlers :: [Handler (StatusCode, StatusDetails)]
finalExceptionHandlers = [
  Handler $ \(err :: IOException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInternal, HE.mkStatusDetails err)
  ,
  Handler $ \(_ :: AsyncCancelled) -> do
    return (StatusOk, "")
  ,
  Handler $ \(err :: SomeException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusUnknown, "UnKnown exception: " <> HE.mkStatusDetails err)
  ]

storeExceptionHandlers :: [Handler (StatusCode, StatusDetails)]
storeExceptionHandlers = [
  Handler $ \(err :: Store.EXISTS) -> do
    Log.warning $ Log.buildString' err
    return (StatusAlreadyExists, "Stream or view with same name already exists in store")
  ,
  Handler $ \(err :: Store.SomeHStoreException) -> do
    Log.warning $ Log.buildString' err
    return (StatusInternal, HE.mkStatusDetails err)
  ]

--------------------------------------------------------------------------------

catchDefaultEx :: IO a -> IO a
catchDefaultEx action = action `catches` defaultExHandlers
{-# INLINEABLE catchDefaultEx #-}

defaultExHandlers :: [Handler a]
defaultExHandlers =
  HE.hServerExHandlers ++ hStoreExHandlers ++ HE.zkExHandlers ++ finalExHandlers

finalExHandlers :: [Handler a]
finalExHandlers =
  [ Handler $ \(err :: IOException) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ HE.mkGrpcStatus err HsGrpc.StatusInternal

  , Handler $ \(_ :: AsyncCancelled) -> do
      HsGrpc.throwGrpcError $ HsGrpc.GrpcStatus HsGrpc.StatusOK Nothing Nothing

  , Handler $ \(err :: SomeException) -> do
      Log.fatal $ Log.buildString' err
      let x = ("UnKnown exception: " <>) <$> HE.mkStatusMsg err
      HsGrpc.throwGrpcError $ HsGrpc.GrpcStatus HsGrpc.StatusUnknown x Nothing
  ]

hStoreExHandlers :: [Handler a]
hStoreExHandlers =
  [ Handler $ \(err :: Store.EXISTS) -> do
      Log.warning $ Log.buildString' err
      let x = Just "Stream or view with same name already exists in store"
      HsGrpc.throwGrpcError $ HsGrpc.GrpcStatus HsGrpc.StatusAlreadyExists x Nothing

  , Handler $ \(err :: Store.SomeHStoreException) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ HE.mkGrpcStatus err HsGrpc.StatusInternal
  ]
