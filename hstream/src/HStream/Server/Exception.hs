{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module HStream.Server.Exception where

import           Control.Concurrent.Async          (AsyncCancelled (..))
import           Control.Exception                 (Exception (..),
                                                    Handler (Handler),
                                                    IOException, SomeException,
                                                    catches, displayException)
import qualified Data.ByteString.Char8             as BS
import           Data.Text                         (Text)
import           Data.Text.Encoding                (encodeUtf8)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Server     (ServerResponse (ServerBiDiResponse, ServerWriterResponse))
import           ZooKeeper.Exception

import qualified HStream.Logger                    as Log
import           HStream.Server.MetaData.Exception (PersistenceException)
import qualified HStream.Store                     as Store
import           HStream.Utils                     (mkServerErrResp)

defaultExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
defaultExceptionHandle = mkExceptionHandle $ setRespType mkServerErrResp defaultHandlers

defaultServerStreamExceptionHandle :: ExceptionHandle (ServerResponse 'ServerStreaming a)
defaultServerStreamExceptionHandle = mkExceptionHandle $ setRespType (ServerWriterResponse mempty) defaultHandlers

defaultBiDiStreamExceptionHandle :: ExceptionHandle (ServerResponse 'BiDiStreaming a)
defaultBiDiStreamExceptionHandle = mkExceptionHandle $ setRespType (ServerBiDiResponse mempty) defaultHandlers

--------------------------------------------------------------------------------
-- Exceptions shared by most of the handlers

newtype InvalidArgument = InvalidArgument String
  deriving (Show)
instance Exception InvalidArgument

newtype UnexpectedError = UnexpectedError String
  deriving (Show)
instance Exception UnexpectedError

newtype WrongServer = WrongServer Text
  deriving (Show)
instance Exception WrongServer

newtype OperationNotSupported = OperationNotSupported String
  deriving (Show)
instance Exception OperationNotSupported

data ObjectNotExist = ObjectNotExist
  deriving (Show)
instance Exception ObjectNotExist

data StreamNotExist = StreamNotExist
  deriving (Show)
instance Exception StreamNotExist

newtype SubscriptionIdNotFound = SubscriptionIdNotFound Text
  deriving (Show)
instance Exception SubscriptionIdNotFound

data ServerNotAvailable = ServerNotAvailable
  deriving (Show)
instance Exception ServerNotAvailable

newtype WrongOffset = WrongOffset String
  deriving (Show)
instance Exception WrongOffset

--------------------------------------------------------------------------------

type MkResp t a = StatusCode -> StatusDetails -> ServerResponse t a
type Handlers a = [Handler a]
type ExceptionHandle a = IO a -> IO a

setRespType :: MkResp t a -> Handlers (StatusCode, StatusDetails) -> Handlers (ServerResponse t a)
setRespType mkResp = map (uncurry mkResp <$>)

serverExceptionHandlers :: [Handler (StatusCode, StatusDetails)]
serverExceptionHandlers = [
  Handler $ \(err :: ServerNotAvailable) -> do
    Log.warning $ Log.buildString' err
    return (StatusUnavailable, "Server is still starting"),
  Handler $ \(err :: StreamNotExist) -> do
    Log.warning $ Log.buildString' err
    return (StatusNotFound, mkStatusDetails err),
  Handler $ \(err :: InvalidArgument) -> do
    Log.warning $ Log.buildString' err
    return (StatusInvalidArgument, mkStatusDetails err),
  Handler $ \(err :: ObjectNotExist) -> do
    Log.warning $ Log.buildString' err
    return (StatusNotFound, "Object not found"),
  Handler $ \(err@(SubscriptionIdNotFound subId) :: SubscriptionIdNotFound) -> do
    Log.warning $ Log.buildString' err
    return (StatusNotFound, StatusDetails ("Subscription ID " <> encodeUtf8 subId <> " can not be found")),
  Handler $ \(err :: PersistenceException) -> do
    Log.warning $ Log.buildString' err
    return (StatusAborted, mkStatusDetails err),
  Handler $ \(err :: UnexpectedError) -> do
    return (StatusInternal, mkStatusDetails err),
  Handler $ \(err :: WrongOffset) -> do
    return (StatusInternal, mkStatusDetails err)
  ]

finalExceptionHandlers :: [Handler (StatusCode, StatusDetails)]
finalExceptionHandlers = [
  Handler $ \(err :: IOException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInternal, mkStatusDetails err)
  ,
  Handler $ \(_ :: AsyncCancelled) -> do
    return (StatusOk, "")
  ,
  Handler $ \(err :: SomeException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusUnknown, "UnKnown exception: " <> mkStatusDetails err)
  ]

storeExceptionHandlers :: [Handler (StatusCode, StatusDetails)]
storeExceptionHandlers = [
  Handler $ \(err :: Store.EXISTS) -> do
    Log.warning $ Log.buildString' err
    return (StatusAlreadyExists, "Stream or view with same name already exists in store")
  ,
  Handler $ \(err :: Store.SomeHStoreException) -> do
    Log.warning $ Log.buildString' err
    return (StatusInternal, mkStatusDetails err)
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

defaultHandlers :: Handlers (StatusCode, StatusDetails)
defaultHandlers = serverExceptionHandlers
               ++ storeExceptionHandlers
               ++ zooKeeperExceptionHandler
               ++ finalExceptionHandlers

mkExceptionHandle :: Handlers (ServerResponse t a)
                  -> IO (ServerResponse t a)
                  -> IO (ServerResponse t a)
mkExceptionHandle = flip catches

mkExceptionHandle' :: (forall e. Exception e => e -> IO ())
                   -> Handlers (ServerResponse t a)
                   -> IO (ServerResponse t a)
                   -> IO (ServerResponse t a)
mkExceptionHandle' whileEx handlers f =
  let handlers' = map (\(Handler h) -> Handler (\e -> whileEx e >> h e)) handlers
   in f `catches` handlers'

mkStatusDetails :: Exception a => a -> StatusDetails
mkStatusDetails = StatusDetails . BS.pack . displayException

handleZKException :: Exception a => a -> StatusCode -> IO (StatusCode, StatusDetails)
handleZKException e status = do
  Log.fatal $ Log.buildString' e
  return (status, "Zookeeper exception: " <> mkStatusDetails e)
