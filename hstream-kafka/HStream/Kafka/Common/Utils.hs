{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}

module HStream.Kafka.Common.Utils where

import           Data.Maybe              (fromMaybe)
import qualified Data.Vector             as V
import qualified Kafka.Protocol.Encoding as K

kaArrayToList :: K.KaArray a -> [a]
kaArrayToList = undefined

listToKaArray :: [a] -> K.KaArray a
listToKaArray = undefined

kaArrayToVector :: K.KaArray a -> V.Vector a
kaArrayToVector kaArray = fromMaybe V.empty (K.unKaArray kaArray)

vectorToKaArray :: V.Vector a -> K.KaArray a
vectorToKaArray vec = K.KaArray (Just vec)

mapKaArray :: (a -> b) -> K.KaArray a -> K.KaArray b
mapKaArray f arr = K.KaArray (fmap (V.map f) (K.unKaArray arr))

mapKaArrayM :: (a -> IO b) -> K.KaArray a -> IO (K.KaArray b)
mapKaArrayM f arr = case K.unKaArray arr of
  Nothing  -> return (K.KaArray Nothing)
  Just vec -> K.KaArray . Just <$> V.mapM f vec

forKaArrayM :: K.KaArray a -> (a -> IO b) -> IO (K.KaArray b)
forKaArrayM = flip mapKaArrayM
