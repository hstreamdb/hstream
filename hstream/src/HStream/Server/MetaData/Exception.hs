module HStream.Server.MetaData.Exception where

import           Control.Exception        (Handler (..), throwIO)
import           Data.Text                (Text)
import qualified ZooKeeper.Exception      as ZK

import           HStream.Exception
import qualified HStream.Logger           as Log
import           HStream.Server.Exception
import           HStream.Utils

rqExceptionHandlers :: ResourceType -> Text -> [Handler a]
rqExceptionHandlers rtype msg =
  [ Handler $ \(err :: RQLiteRowNotFound) -> do
      Log.fatal $ Log.buildString' err
      throwIO $ someNotFound rtype msg
  , Handler $ \(err :: RQLiteRowAlreadyExists) -> do
      Log.fatal $ Log.buildString' err
      throwIO $ someAlreadyExists rtype msg
  ]

zkExceptionHandlers :: ResourceType -> Text -> [Handler a]
zkExceptionHandlers rtype msg =
  [ Handler $ \(err :: ZK.ZNONODE) -> do
      Log.fatal $ Log.buildString' err
      throwIO $ someNotFound rtype msg
  , Handler $ \(err :: ZK.ZNODEEXISTS) -> do
      Log.fatal $ Log.buildString' err
      throwIO $ someAlreadyExists rtype msg
  ]

-- RQLiteTableNotFound
-- RQLiteRowNotFound
-- RQLiteTableAlreadyExists
-- RQLiteRowAlreadyExists
-- RQLiteRowBadVersion
-- RQLiteNetworkErrs
-- RQLiteDecodeErrs
-- RQLiteUnspecifiedErr
-- RQLiteNoAuth

-- ZCONNECTIONLOSS
-- ZBADARGUMENTS
-- ZINVALIDSTATE
-- ZOPERATIONTIMEOUT
-- ZDATAINCONSISTENCY
-- ZRUNTIMEINCONSISTENCY
-- ZMARSHALLINGERROR

-- ZNODEEXISTS
-- ZNONODE

-- XXNotFound
-- XXExists
-- XXInternalError
-- XX
