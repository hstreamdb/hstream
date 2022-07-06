module HStream.Server.ReaderPool
  (
    ReaderPool,
    mkReaderPool,
    getReader,
    putReader,
    ReaderPoolOverSize
  )
where

import           Control.Concurrent.STM (TVar, atomically, check, newTVarIO,
                                         readTVar, throwSTM, writeTVar)
import           Control.Exception      (Exception, try)
import           Data.Maybe             (fromJust)
import           Data.Vector            (Vector)
import qualified Data.Vector            as V
import qualified HStream.Store          as S

data Pool = Pool
  { pool      :: Vector S.LDReader
  , maxReader :: Int
  }

type ReaderPool = TVar Pool

mkReaderPool :: S.LDClient -> Int -> IO ReaderPool
mkReaderPool client size = do
  let ldReaderBufferSize = 10
  readers <- V.replicateM size $ S.newLDReader client 1 (Just ldReaderBufferSize)
  newTVarIO $ Pool
    { pool      = readers
    , maxReader = size
    }

getReader :: ReaderPool -> IO S.LDReader
getReader tv = atomically $ do
  readerPool@Pool{..} <- readTVar tv
  check . not . V.null $ pool
  let (reader, remain) = fromJust . V.uncons $ pool
      newReaderPool = readerPool { pool = remain }
  writeTVar tv newReaderPool
  return reader

putReader :: ReaderPool -> S.LDReader -> IO (Either ReaderPoolOverSize ())
putReader tv reader = try $ atomically $ do
  readerPool@Pool{..} <- readTVar tv
  if V.length pool == maxReader
     then throwSTM ReaderPoolOverSize
     else writeTVar tv $ readerPool { pool = V.snoc pool reader }

data ReaderPoolOverSize = ReaderPoolOverSize
  deriving Show
instance Exception ReaderPoolOverSize where
