module HStream.Server.Core.Common where

import           Control.Exception             (throwIO)
import qualified HStream.Logger                as Log
import           HStream.Server.Exception      (ObjectNotExist (ObjectNotExist))
import           HStream.Server.Handler.Common (terminateQueryAndRemove,
                                                terminateRelatedQueries)
import           HStream.Server.Types          (ServerContext (..))
import qualified HStream.Store                 as HS
import           HStream.ThirdParty.Protobuf   (Empty (Empty))

deleteStoreStream
  :: ServerContext
  -> HS.StreamId
  -> Bool
  -> IO Empty
deleteStoreStream sc@ServerContext{..} s checkIfExist = do
  streamExists <- HS.doesStreamExist scLDClient s
  if streamExists then clean >> return Empty else ignore checkIfExist
  where
    clean = do
      terminateQueryAndRemove sc (HS.streamName s)
      terminateRelatedQueries sc (HS.streamName s)
      HS.removeStream scLDClient s
    ignore True  = return Empty
    ignore False = do
      Log.warning $ "Drop: tried to remove a nonexistent object: "
                 <> Log.buildCBytes (HS.streamName s)
      throwIO ObjectNotExist
