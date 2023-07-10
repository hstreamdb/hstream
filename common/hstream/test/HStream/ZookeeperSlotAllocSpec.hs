module HStream.ZookeeperSlotAllocSpec where

import           Control.Exception
import           Control.Monad                     (void)
import qualified Data.Map.Strict                   as Map
import           Data.Maybe                        (fromMaybe)
import qualified Data.Set                          as Set
import qualified Data.Text                         as T
import           System.Environment                (lookupEnv)
import           Test.Hspec
import           ZooKeeper                         (withResource,
                                                    zookeeperResInit)
import           ZooKeeper.Exception               (ZNODEEXISTS)
import           ZooKeeper.Types                   (ZHandle)

import           HStream.Common.ZookeeperSlotAlloc
import           HStream.Exception
import           HStream.Utils                     (textToCBytes)

spec :: Spec
spec = do
  let host = "127.0.0.1"
  port <- runIO $ fromMaybe "2181" <$> lookupEnv "ZOOKEEPER_LOCAL_PORT"
  let url = textToCBytes $ T.pack $ host <> ":" <> port
      res = zookeeperResInit url Nothing 5000 Nothing 0
  runIO $ withResource res $ \zh -> hspec $ runTests zh

-- TODO:
-- 1. properties tests
-- 2. concurrent tests
runTests :: ZHandle -> Spec
runTests zh = describe "HStream.ZookeeperSlotAllocSpec" $ do
  let c1 = SlotConfig{slotRoot="/tmp_a", slotZkHandler=zh, slotOffset=1, slotMaxCapbility=50}
      c2 = SlotConfig{slotRoot="/tmp_b", slotZkHandler=zh, slotOffset=1, slotMaxCapbility=2}
      valAttrs1 = SlotValueAttrs (Map.fromList [("k1", "v1")])
      valAttrs2 = SlotValueAttrs (Map.fromList [("k2", "v2")])

  aroundAll (withSlot c1) $ do
    it "smoke" $ \c -> do
      checkSlot c "name_a" [valAttrs1] [1]  -- start from (0 + slotOffset)
      allocateSlot c "name_a" Map.empty [valAttrs1] `shouldThrow` existException
      checkSlot c "name_b" [valAttrs1] [2]

      void $ deallocateSlot c "name_a"

      checkSlot c "name_a" [valAttrs1] [1]
      checkSlot c "name_c" [valAttrs1] [3]

    it "multi slot values" $ \c -> do
      checkSlot c "name_d" [valAttrs1, valAttrs2] [4, 5]
      checkSlot c "name_e" [valAttrs1, valAttrs2] [6, 7]

      void $ deallocateSlot c "name_d"

      slotvals_f <- allocateSlot c "name_f" Map.empty [valAttrs1]
      slotvals_g <- allocateSlot c "name_g" Map.empty [valAttrs1]
      slot_f <- getSlotByName c "name_f"
      slot_g <- getSlotByName c "name_g"

      let fg = Set.unions [Map.keysSet (slotVals slot_f), Map.keysSet (slotVals slot_g)]
          fg' = Set.unions [Set.fromList (map fst slotvals_f), Set.fromList (map fst slotvals_g)]
      fg `shouldBe` fg'
      fg `shouldBe` Set.fromList [4, 5]

      checkSlot c "name_h" [valAttrs1] [8]

  aroundAll (withSlot c2) $ do
    it "run out of slots" $ \c -> do
      checkSlot c "name_a" [valAttrs1] [1]
      checkSlot c "name_b" [valAttrs1] [2]
      allocateSlot c "name_c" Map.empty [valAttrs1] `shouldThrow` noMoreSlots

      void $ deallocateSlot c "name_b"

      checkSlot c "name_c" [valAttrs1] [2]
      allocateSlot c "name_d" Map.empty [valAttrs1] `shouldThrow` noMoreSlots

      void $ deallocateSlot c "name_a"
      void $ deallocateSlot c "name_c"

      allocateSlot c "name_e" Map.empty [valAttrs1, valAttrs1, valAttrs2] `shouldThrow` noMoreSlots
      checkSlot c "name_c" [valAttrs1, valAttrs1] [1, 2]

withSlot :: SlotConfig -> (SlotConfig -> IO ()) -> IO ()
withSlot c action = bracket_ (initSlot c) (clearSlot c) (action c)

existException :: Selector ZNODEEXISTS
existException = const True

noMoreSlots :: Selector NoMoreSlots
noMoreSlots = const True

checkSlot c name attrs vals = do
  slotvals <- allocateSlot c name Map.empty attrs
  slot <- getSlotByName c name
  Map.keysSet (slotVals slot) `shouldBe` Set.fromList (map fst slotvals)
  Set.fromList (map fst slotvals) `shouldBe` Set.fromList vals
