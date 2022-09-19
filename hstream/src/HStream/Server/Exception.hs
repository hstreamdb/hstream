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
import           Control.Exception             (Exception (..),
                                                Handler (Handler), IOException,
                                                SomeException)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server (ServerResponse (ServerBiDiResponse, ServerWriterResponse))
import           ZooKeeper.Exception

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
               ++ zooKeeperExceptionHandler
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

zooKeeperExceptionHandler :: Handlers (StatusCode, StatusDetails)
zooKeeperExceptionHandler = [
  Handler $ \(e :: ZCONNECTIONLOSS    ) -> handleZKException e StatusUnavailable,
  Handler $ \(e :: ZBADARGUMENTS      ) -> handleZKException e StatusInvalidArgument,
  Handler $ \(e :: ZSSLCONNECTIONERROR) -> handleZKException e StatusFailedPrecondition,
  Handler $ \(e :: ZRECONFIGINPROGRESS) -> handleZKException e StatusUnavailable,
  Handler $ \(e :: ZINVALIDSTATE      ) -> handleZKException e StatusUnavailable,
  Handler $ \(e :: ZOPERATIONTIMEOUT  ) -> handleZKException e StatusAborted,
  Handler $ \(e :: ZDATAINCONSISTENCY ) -> handleZKException e StatusAborted,
  Handler $ \(e :: ZRUNTIMEINCONSISTENCY) -> handleZKException e StatusAborted,
  Handler $ \(e :: ZSYSTEMERROR       ) -> handleZKException e StatusInternal,
  Handler $ \(e :: ZMARSHALLINGERROR  ) -> handleZKException e StatusUnknown,
  Handler $ \(e :: ZUNIMPLEMENTED     ) -> handleZKException e StatusUnknown,
  Handler $ \(e :: ZNEWCONFIGNOQUORUM ) -> handleZKException e StatusUnknown,
  Handler $ \(e :: ZNODEEXISTS        ) -> handleZKException e StatusAlreadyExists,
  Handler $ \(e :: ZNONODE            ) -> handleZKException e StatusNotFound,
  Handler $ \(e :: ZooException       ) -> handleZKException e StatusInternal
  ]

handleZKException :: Exception a => a -> StatusCode -> IO (StatusCode, StatusDetails)
handleZKException e status = do
  Log.fatal $ Log.buildString' e
  return (status, "Zookeeper exception: " <> HE.mkStatusDetails e)
