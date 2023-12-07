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
  , someNotFound
  , someAlreadyExists
  ) where

import           Control.Concurrent.Async      (AsyncCancelled (..))
import           Control.Exception             (Exception (..),
                                                Handler (Handler), IOException,
                                                SomeException, catches)
import           Data.Text                     (Text)
import qualified HsGrpc.Server.Types           as HsGrpc
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server (ServerResponse (ServerBiDiResponse, ServerWriterResponse))

import qualified HStream.Exception             as HE
import qualified HStream.Logger                as Log
import qualified HStream.Server.HStreamApi     as API
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
    return (StatusInternal, HE.mkStatusDetails $ HE.SomeServerInternal $ displayException err)
  ,
  Handler $ \(_ :: AsyncCancelled) -> do
    return (StatusOk, HE.mkStatusDetails $ HE.SomeServerInternal "")
  ,
  Handler $ \(err :: SomeException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusUnknown, HE.mkStatusDetails $ HE.SomeServerInternal $ "UnKnown exception: " <> displayException err)
  ]

storeExceptionHandlers :: [Handler (StatusCode, StatusDetails)]
storeExceptionHandlers = [
  Handler $ \(err :: Store.EXISTS) -> do
    Log.warning $ Log.buildString' err
    return (StatusAlreadyExists, HE.mkStatusDetails $ HE.SomeStoreInternal "Stream or view with same name already exists in store")
  ,
  Handler $ \(err :: Store.SomeHStoreException) -> do
    Log.warning $ Log.buildString' err
    return (StatusInternal, HE.mkStatusDetails (HE.SomeStoreInternal (displayException err)))
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

  , Handler $ \(err :: Store.TOOBIG) -> do
      Log.warning $ Log.buildString' err
      HsGrpc.throwGrpcError $ HE.mkGrpcStatus err HsGrpc.StatusInvalidArgument

  , Handler $ \(err :: Store.SomeHStoreException) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ HE.mkGrpcStatus err HsGrpc.StatusInternal
  ]

--------------------------------------------------------------------------------

someNotFound :: API.ResourceType -> Text -> SomeException
someNotFound API.ResourceTypeResStream       t = toException $ HE.StreamNotFound t
someNotFound API.ResourceTypeResSubscription t = toException $ HE.SubscriptionNotFound t
someNotFound API.ResourceTypeResShard        t = toException $ HE.ShardNotFound t
someNotFound API.ResourceTypeResConnector    t = toException $ HE.ConnectorNotFound t
someNotFound API.ResourceTypeResQuery        t = toException $ HE.QueryNotFound t
someNotFound API.ResourceTypeResView         t = toException $ HE.ViewNotFound t

someAlreadyExists :: API.ResourceType -> Text -> SomeException
someAlreadyExists API.ResourceTypeResStream       t = toException $ HE.StreamExists t
someAlreadyExists API.ResourceTypeResSubscription t = toException $ HE.SubscriptionExists t
someAlreadyExists API.ResourceTypeResShardReader  t = toException $ HE.ShardReaderExists t
someAlreadyExists API.ResourceTypeResConnector    t = toException $ HE.ConnectorExists t
someAlreadyExists API.ResourceTypeResQuery        t = toException $ HE.QueryExists t
someAlreadyExists API.ResourceTypeResView         t = toException $ HE.ViewExists t
