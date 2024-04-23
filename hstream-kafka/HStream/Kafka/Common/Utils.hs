{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}

module HStream.Kafka.Common.Utils where

import           Control.Concurrent
import           Control.Exception                   (throw)
import qualified Control.Monad                       as M
import qualified Control.Monad.ST                    as ST
import qualified Data.Base64.Types                   as Base64
import qualified Data.ByteString                     as BS
import qualified Data.ByteString.Base64              as Base64
import qualified Data.HashTable.IO                   as H
import qualified Data.HashTable.ST.Basic             as HB
import qualified Data.IORef                          as IO
import           Data.Maybe                          (fromMaybe)
import qualified Data.Text                           as T
import qualified Data.Text.Encoding                  as T
import qualified Data.Vector                         as V
import           HStream.Kafka.Common.KafkaException (ErrorCodeException (ErrorCodeException))
import qualified Kafka.Protocol.Encoding             as K
import qualified System.Timeout                      as Timeout

type HashTable k v = H.BasicHashTable k v

hashtableGet hashTable key errorCode = H.lookup hashTable key >>= \case
  Nothing -> throw (ErrorCodeException errorCode)
  Just v -> return v

hashtableDeleteAll hashTable = do
  lst <- H.toList hashTable
  M.forM_ lst $ \(key, _) -> H.delete hashTable key

-- O(1)
hashtableSize :: HB.HashTable ST.RealWorld k v -> IO Int
hashtableSize hashTable = ST.stToIO (HB.size hashTable)

-- O(1)
hashtableNull :: HB.HashTable ST.RealWorld k v -> IO Bool
hashtableNull hashTable = (== 0) <$> ST.stToIO (HB.size hashTable)

kaArrayToList :: K.KaArray a -> [a]
kaArrayToList = V.toList . fromMaybe V.empty . K.unKaArray

listToKaArray :: [a] -> K.KaArray a
listToKaArray = K.KaArray . Just . V.fromList

kaArrayToVector :: K.KaArray a -> V.Vector a
kaArrayToVector kaArray = fromMaybe V.empty (K.unKaArray kaArray)

vectorToKaArray :: V.Vector a -> K.KaArray a
vectorToKaArray vec = K.KaArray (Just vec)

mapKaArray :: (a -> b) -> K.KaArray a -> K.KaArray b
mapKaArray f arr = K.KaArray (fmap (V.map f) (K.unKaArray arr))

forKaArray :: K.KaArray a -> (a -> b) -> K.KaArray b
forKaArray = flip mapKaArray

mapKaArrayM :: (a -> IO b) -> K.KaArray a -> IO (K.KaArray b)
mapKaArrayM f arr = case K.unKaArray arr of
  Nothing  -> return (K.KaArray Nothing)
  Just vec -> K.KaArray . Just <$> V.mapM f vec

forKaArrayM :: K.KaArray a -> (a -> IO b) -> IO (K.KaArray b)
forKaArrayM = flip mapKaArrayM

filterKaArray :: (a -> Bool) -> K.KaArray a -> K.KaArray a
filterKaArray f arr = case K.unKaArray arr of
  Nothing  -> K.KaArray Nothing
  Just vec -> K.KaArray (Just (V.filter f vec))

filterKaArrayM :: (a -> IO Bool) -> K.KaArray a -> IO (K.KaArray a)
filterKaArrayM f arr = case K.unKaArray arr of
  Nothing  -> return (K.KaArray Nothing)
  Just vec -> do
    vec' <- V.filterM f vec
    return $ K.KaArray (Just vec')

emptyKaArray :: K.KaArray a
emptyKaArray = K.KaArray (Just V.empty)

whenEqM :: (Eq a, Monad m) => m a -> a -> m () -> m ()
whenEqM valM expected action = do
  valM >>= \val -> do
    M.when (expected == val) $ action

whenIORefEq :: (Eq a) => IO.IORef a -> a -> IO () -> IO ()
whenIORefEq ioRefVal = whenEqM (IO.readIORef ioRefVal)

unlessIORefEq :: (Eq a) => IO.IORef a -> a -> (a -> IO ()) -> IO ()
unlessIORefEq ioRefVal expected action = do
  IO.readIORef ioRefVal >>= \val -> do
    M.unless (expected == val) $ action val

encodeBase64 :: BS.ByteString -> T.Text
encodeBase64 = Base64.extractBase64 . Base64.encodeBase64

decodeBase64 :: T.Text -> BS.ByteString
decodeBase64 = Base64.decodeBase64Lenient . T.encodeUtf8

-- | Perform the action when the predicate is true or timeout is reached.
onOrTimeout :: IO Bool -> Int -> IO b -> IO b
onOrTimeout p timeoutMs action =
  Timeout.timeout (timeoutMs * 1000) loop >>= \case
    Nothing -> action
    Just a  -> return a
  where
    loop = p >>= \case
      True  -> action
      -- FIXME: Hardcoded constant (check every 1ms)
      False -> threadDelay 1000 >> loop
