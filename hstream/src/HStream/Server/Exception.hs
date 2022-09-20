{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module HStream.Server.Exception
  ( defaultHandlers
  , defaultExceptionHandle
  , defaultServerStreamExceptionHandle
  , defaultBiDiStreamExceptionHandle
  ) where

import           Control.Concurrent.Async      (AsyncCancelled (..))
import           Control.Exception             (Handler (Handler), IOException,
                                                SomeException)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server (ServerResponse (ServerBiDiResponse, ServerWriterResponse))

import qualified HStream.Exception             as HE
import qualified HStream.Logger                as Log
import qualified HStream.Store                 as Store
import           HStream.Utils                 (mkServerErrResp)

--------------------------------------------------------------------------------

type Handlers a = [Handler a]

defaultExceptionHandle :: HE.ExceptionHandle (ServerResponse 'Normal a)
defaultExceptionHandle = HE.mkExceptionHandle $
  HE.setRespType mkServerErrResp defaultHandlers

defaultServerStreamExceptionHandle :: HE.ExceptionHandle (ServerResponse 'ServerStreaming a)
defaultServerStreamExceptionHandle = HE.mkExceptionHandle $
  HE.setRespType (ServerWriterResponse mempty) defaultHandlers

defaultBiDiStreamExceptionHandle :: HE.ExceptionHandle (ServerResponse 'BiDiStreaming a)
defaultBiDiStreamExceptionHandle = HE.mkExceptionHandle $
  HE.setRespType (ServerBiDiResponse mempty) defaultHandlers

defaultHandlers :: Handlers (StatusCode, StatusDetails)
defaultHandlers = HE.defaultHServerExHandlers
               ++ storeExceptionHandlers
               ++ HE.zkExceptionHandlers
               ++ finalExceptionHandlers

--------------------------------------------------------------------------------

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
