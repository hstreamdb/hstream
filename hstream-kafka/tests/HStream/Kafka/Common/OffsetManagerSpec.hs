module HStream.Kafka.Common.OffsetManagerSpec where

import           Control.Monad
import           Data.IORef
import           Data.Word
import           Test.Hspec

import           HStream.Base.Concurrent            (runConc)
import           HStream.Kafka.Common.OffsetManager
import qualified HStream.Kafka.Common.RecordFormat  as K
import           HStream.Kafka.Common.TestUtils     (ldclient)
import qualified HStream.Store                      as S
import qualified HStream.Utils                      as U
import qualified Kafka.Protocol.Encoding            as K

initOm :: Bool -> IO (OffsetManager, Word64)
initOm shouldTrim = do
  om <- initOffsetReader =<< newOffsetManager ldclient
  let logid = 50    -- we already has logids from 1 to 100
  when shouldTrim $
    S.trimLast ldclient logid  -- clear the log
  pure (om, logid)

spec :: Spec
spec = describe "OffsetManagerSpec" $ do
  it "withOffsetN" $ do
    (om, logid) <- initOm True
    r <- newIORef 0
    withOffsetN om logid 10 $ writeIORef r
    readIORef r `shouldReturn` 9
    withOffsetN om logid 10 $ writeIORef r
    readIORef r `shouldReturn` 19

  it "withOffsetN multi-thread" $ do
    (om, logid) <- initOm True
    r <- newIORef []
    runConc 100 $
      withOffsetN om logid 10 $ \o -> modifyIORef' r (++ [o])
    offsets <- readIORef r
    offsets `shouldBe` [9, 19..999]

  it "withOffsetN multi-thread with existent data" $ do
    (_, logid) <- initOm True
    let existOffset = 11
        appendKey = U.intToCBytesWithPadding existOffset
        appendAttrs = Just [(S.KeyTypeFindKey, appendKey)]
        storedRecord = K.runPut $ K.RecordFormat 0{- version -}
                                                 existOffset
                                                 10 {-batchLength-}
                                                 (K.CompactBytes "xxx")
    void $ S.appendCompressedBS ldclient logid storedRecord S.CompressionNone appendAttrs

    (om, _) <- initOm False
    r <- newIORef []
    runConc 100 $ do
      withOffsetN om logid 10 $ \o -> modifyIORef' r (++ [o])
    offsets <- readIORef r
    offsets `shouldBe` [21, 31..1011]
