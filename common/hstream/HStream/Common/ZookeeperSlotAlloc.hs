{-# LANGUAGE BangPatterns #-}

module HStream.Common.ZookeeperSlotAlloc
  ( SlotConfig (..)
  , HT.Slot (..)
  , HT.SlotValueAttrs (..)
  , initSlot
  , clearSlot
  , allocateSlot
  , deallocateSlot
  , doesSlotExist
  , doesSlotValueExist
  , getSlotByName
  , getSlotValueByName
  ) where

import           Control.Exception         (throwIO, try)
import           Control.Monad
import qualified Data.ByteString.Lazy      as BSL
import qualified Data.Map.Strict           as Map
import           Data.Maybe                (isJust)
import           Data.Text                 (Text)
import           Data.Word                 (Word64)
import           GHC.Stack                 (HasCallStack)
import qualified Proto3.Suite              as PB
import qualified Z.Data.Builder            as ZB
import qualified Z.Data.CBytes             as CBytes
import           Z.Data.CBytes             (CBytes)
import qualified Z.Data.Parser             as ZP
import qualified Z.Data.Vector             as ZV
import qualified Z.Foreign                 as Z
import qualified ZooKeeper                 as ZK
import qualified ZooKeeper.Exception       as ZK
import qualified ZooKeeper.Recipe          as ZK
import qualified ZooKeeper.Types           as ZK

import qualified HStream.Common.ProtoTypes as HT
import qualified HStream.Exception         as HE
import qualified HStream.Logger            as Log

-------------------------------------------------------------------------------

type SlotName = CBytes
type SlotValue = Word64
type SlotAttrs = Map.Map Text Text

data SlotConfig = SlotConfig
  { slotRoot         :: {-# UNPACK #-} !CBytes
    -- ^ NOTE: the root path should NOT have a trailing '/'
  , slotZkHandler    :: {-# UNPACK #-} !ZK.ZHandle
  , slotOffset       :: {-# UNPACK #-} !Word64
  , slotMaxCapbility :: {-# UNPACK #-} !Word64
  }

-- TODO:
--
-- 1. add comments to describe how this allocation work
-- 2. better handler zk exceptions
--
-- /hstream/streamgroup/group_a
--    - free:0
--    - table
--        - 0:1
--        - 1:2
--        - 2:3
--        - ...
--    - name
--        - name_a:<Slot>
--        - name_b:<Slot>
--        - ...
initSlot :: SlotConfig -> IO ()
initSlot SlotConfig{..} = do
  void $ ZK.zooCreateIfMissing slotZkHandler slotRoot Nothing ZK.zooOpenAclUnsafe ZK.ZooPersistent
  void $ ZK.zooCreateIfMissing slotZkHandler (slotRoot <> "/free") (Just $ encodeSlotValue 0) ZK.zooOpenAclUnsafe ZK.ZooPersistent
  void $ ZK.zooCreateIfMissing slotZkHandler (slotRoot <> "/table") Nothing ZK.zooOpenAclUnsafe ZK.ZooPersistent
  void $ ZK.zooCreateIfMissing slotZkHandler (slotRoot <> "/name") Nothing ZK.zooOpenAclUnsafe ZK.ZooPersistent

clearSlot :: SlotConfig -> IO ()
clearSlot SlotConfig{..} = ZK.zooDeleteAll slotZkHandler slotRoot

allocateSlot
  :: HasCallStack
  => SlotConfig
  -> SlotName
  -> SlotAttrs
  -> [HT.SlotValueAttrs]
  -> IO [(SlotValue, HT.SlotValueAttrs)]
allocateSlot _ _ _ [] = pure []
allocateSlot c@SlotConfig{..} name attrs valAttrs = withLock c $ do
  let name_path = slotRoot <> "/name/" <> name
      free_path = slotRoot <> "/free"

  free_slot <- getFree c
  (free_slot', rets) <- allocate free_slot valAttrs []

  let slot = HT.Slot{ HT.slotVals = Map.map Just (Map.fromList rets)
                    , HT.slotAttrs = attrs
                    }
  let op1 = ZK.zooCreateOpInit name_path (Just $ encodeSlot slot) 0 ZK.zooOpenAclUnsafe ZK.ZooPersistent
      op2 = ZK.zooSetOpInit free_path (Just $ encodeSlotValue free_slot') Nothing

  results <- ZK.zooMulti slotZkHandler [op1, op2]
  -- The length of the results should be 2
  ZK.assertZooOpResultOK $ results !! 0
  ZK.assertZooOpResultOK $ results !! 1

  -- slots (with offset)
  pure rets

  where
    allocate free_slot [] rets = pure (free_slot, rets)
    allocate free_slot !(x:xs) !rets = do
      free_slot' <- getTable c free_slot
      let returned = free_slot + slotOffset
      allocate free_slot' xs ((returned, x):rets)

deallocateSlot :: SlotConfig -> CBytes -> IO [SlotValue]
deallocateSlot c@SlotConfig{..} name = withLock c $ do
  HT.Slot{..} <- getSlotByName c name
  let vals = Map.keys slotVals
      name_node = (slotRoot <> "/name/" <> name)
      free_slot_node = (slotRoot <> "/free")

  if null vals
     then do Log.fatal "SlotValue should not be empty!"
             pure []
     else do
       free_slot_val <- getFree c
       (free_slot_val', table_set_ops) <- deallocate free_slot_val vals []
       let op2 = ZK.zooSetOpInit free_slot_node (Just $ encodeSlotValue free_slot_val') Nothing
           op3 = ZK.zooDeleteOpInit name_node Nothing
       results <- ZK.zooMulti slotZkHandler (table_set_ops ++ [op2, op3])
       forM_ results ZK.assertZooOpResultOK
       pure vals
  where
    deallocate free_slot_val [] rets = pure (free_slot_val, rets)
    deallocate free_slot_val !(val:vals) rets = do
      let real_val = val - slotOffset
          table_node = (slotRoot <> "/table/") <> CBytes.buildCBytes (ZB.int real_val)
          op = ZK.zooSetOpInit table_node (Just $ encodeSlotValue free_slot_val) Nothing
      deallocate real_val vals (op:rets)

doesSlotExist :: SlotConfig -> CBytes -> IO Bool
doesSlotExist SlotConfig{..} name = do
  let path = slotRoot <> "/name/" <> name
  isJust <$> ZK.zooExists slotZkHandler path

doesSlotValueExist :: SlotConfig -> CBytes -> SlotValue -> IO Bool
doesSlotValueExist c name value = do
  let path = slotRoot c <> "/name/" <> name
      errmsg = "Impossible empty value: " <> show path
  e <- try $ slotGet c path errmsg
  case e of
    Left (_ :: ZK.ZNONODE) -> pure False
    Right HT.Slot{..}      -> pure $ Map.member value slotVals

getSlotByName :: HasCallStack => SlotConfig -> CBytes -> IO HT.Slot
getSlotByName c name = do
  let path = slotRoot c <> "/name/" <> name
      errmsg = "Get slot failed, name: " <> show name
  slotGet c path errmsg

getSlotValueByName :: HasCallStack => SlotConfig -> CBytes -> IO [SlotValue]
getSlotValueByName c name = Map.keys . HT.slotVals <$> getSlotByName c name

-------------------------------------------------------------------------------

getFree :: HasCallStack => SlotConfig -> IO SlotValue
getFree c = do
  let path = slotRoot c <> "/free"
      errmsg = "Get FreeSlot falied, path: " <> show path
  slotValueGet c path errmsg

getTable :: HasCallStack => SlotConfig -> Word64 -> IO SlotValue
getTable SlotConfig{..} idx = do
  when (idx >= slotMaxCapbility) $ throwIO $ HE.NoMoreSlots

  let node = slotRoot <> "/table/" <> CBytes.buildCBytes (ZB.int idx)
      errmsg = "getTable failed: " <> show node
  e <- try $ ZK.dataCompletionValue <$> ZK.zooGet slotZkHandler node
  case e of
    Left (_ :: ZK.ZNONODE) -> do
      void $ ZK.zooCreate slotZkHandler node (Just $ encodeSlotValue $ idx + 1) ZK.zooOpenAclUnsafe ZK.ZooPersistent
      return (idx + 1)
    Right (Just a) -> decodeSlotValueThrow a
    Right Nothing -> throwIO $ HE.SlotAllocDecodeError errmsg

-------------------------------------------------------------------------------

slotValueGet :: HasCallStack => SlotConfig -> CBytes -> String -> IO SlotValue
slotValueGet SlotConfig{..} path errmsg = do
  m_bs <- ZK.dataCompletionValue <$> ZK.zooGet slotZkHandler path
  case m_bs of
    Just bs -> decodeSlotValueThrow bs
    Nothing -> throwIO $ HE.SlotAllocDecodeError errmsg
{-# INLINABLE slotValueGet #-}

slotGet :: HasCallStack => SlotConfig -> CBytes -> String -> IO HT.Slot
slotGet SlotConfig{..} path errmsg = do
  m_bs <- ZK.dataCompletionValue <$> ZK.zooGet slotZkHandler path
  case m_bs of
    Just bs -> decodeSlotThrow bs
    Nothing -> throwIO $ HE.SlotAllocDecodeError errmsg
{-# INLINABLE slotGet #-}

-- TODO: performance improvements
encodeSlot :: HT.Slot -> ZV.Bytes
encodeSlot = Z.fromByteString . BSL.toStrict . PB.toLazyByteString
{-# INLINE encodeSlot #-}

-- TODO: performance improvements
decodeSlotThrow :: HasCallStack => ZV.Bytes -> IO HT.Slot
decodeSlotThrow b = do
  let bs = Z.toByteString b
  either (throwIO . HE.SlotAllocDecodeError . show) pure (PB.fromByteString bs)
{-# INLINABLE decodeSlotThrow #-}

encodeSlotValue :: SlotValue -> ZV.Bytes
encodeSlotValue = ZB.build . ZB.int
{-# INLINE encodeSlotValue #-}

decodeSlotValueThrow :: HasCallStack => ZV.Bytes -> IO SlotValue
decodeSlotValueThrow b =
  either (throwIO . HE.SlotAllocDecodeError . show) pure (ZP.parse' ZP.uint b)
{-# INLINABLE decodeSlotValueThrow #-}

withLock :: SlotConfig -> IO a -> IO a
withLock SlotConfig{..} action = do
  clientid <- ZK.clientId <$> (ZK.peekClientId =<< ZK.zooClientID slotZkHandler)
  let clientid' = CBytes.buildCBytes $ ZB.hex clientid
  ZK.withLock slotZkHandler slotRoot clientid' action
