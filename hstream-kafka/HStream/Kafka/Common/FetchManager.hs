module HStream.Kafka.Common.FetchManager
  ( FetchContext (reader)
  , FetchLogContext (..)
  , getFetchLogCtx
  , setFetchLogCtx
  , clearFetchLogCtx
  , getAllFetchLogs

  , fakeFetchContext
  , initFetchContext
  ) where

import qualified Data.HashMap.Strict               as HM
import           Data.Int
import           Data.IORef
import           Data.Vector                       (Vector)
import qualified Data.Vector                       as V
import           Foreign.ForeignPtr                (newForeignPtr_)
import           Foreign.Ptr                       (nullPtr)

import qualified HStream.Kafka.Common.RecordFormat as K
import qualified HStream.Store                     as S

data FetchLogContext = FetchLogContext
  { nextOffset :: Int64
    -- ^ Expect next offset to be fetched
  , remRecords :: Vector K.Record
    -- ^ Remaining records of the batch
  } deriving (Show)

-- Thread-unsafe
data FetchContext = FetchContext
  { reader    :: !S.LDReader
  , logCtxMap :: !(IORef (HM.HashMap S.C_LogID FetchLogContext))
    -- ^ FetchLogContext for each partition/log
  }

getAllFetchLogs :: FetchContext -> IO [S.C_LogID]
getAllFetchLogs ctx = HM.keys <$> readIORef ctx.logCtxMap

getFetchLogCtx :: FetchContext -> S.C_LogID -> IO (Maybe FetchLogContext)
getFetchLogCtx ctx logid = HM.lookup logid <$> readIORef ctx.logCtxMap

setFetchLogCtx :: FetchContext -> S.C_LogID -> FetchLogContext -> IO ()
setFetchLogCtx ctx logid logctx =
  modifyIORef' ctx.logCtxMap $ HM.insert logid logctx

clearFetchLogCtx :: FetchContext -> IO ()
clearFetchLogCtx ctx = writeIORef ctx.logCtxMap $! HM.empty

-- Must be initialized later
fakeFetchContext :: IO FetchContext
fakeFetchContext = do
  -- Trick to avoid use maybe, must be initialized later
  reader <- newForeignPtr_ nullPtr
  FetchContext reader <$> newIORef HM.empty

initFetchContext :: S.LDClient -> IO FetchContext
initFetchContext ldclient = do
  -- Reader used for fetch.
  --
  -- Currently, we only need one reader per connection because there will be
  -- only one thread to fetch data.
  --
  -- TODO: also considering the following:
  --
  -- - use a pool of readers.
  -- - create a reader(or pool of readers) for each consumer group.
  --
  -- NOTE: the maxLogs is set to 1000, which means the reader will fetch at most
  -- 1000 logs.
  -- TODO: maybe we should set maxLogs dynamically according to the max number
  -- of all fetch requests in this connection.
  !reader <- S.newLDReader ldclient 1000{-maxLogs-} (Just 10){-bufferSize-}
  FetchContext reader <$> newIORef HM.empty
