{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}
{-# LANGUAGE BangPatterns   #-}
{-# LANGUAGE CPP            #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE MagicHash      #-}
{-
Note that we need this UnboxedTuples to force ghci use -fobject-code for all
related modules. Or ghci will complain "panic".

Also, manual add @{-# OPTIONS_GHC -fobject-code #-}@ is possible, but need
to add all imported local modules. :(

Relatead ghc issues:
* https://gitlab.haskell.org/ghc/ghc/-/issues/19733
* https://gitlab.haskell.org/ghc/ghc/-/issues/15454
-}
{-# LANGUAGE UnboxedTuples  #-}

module HStream.Store.Internal.LogDevice.LogAttributes where

import           Control.Exception              (finally)
import           Data.Default                   (Default, def)
import           Data.Map.Strict                (Map)
import qualified Data.Map.Strict                as Map
import           Data.Primitive
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           GHC.Generics                   (Generic)
import           Z.Data.CBytes                  (CBytes)
import qualified Z.Foreign                      as Z

import           HStream.Foreign
import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.Types

-------------------------------------------------------------------------------

data Attribute a = Attribute
  { attrValue     :: !(Maybe a)
  , attrInherited :: !Bool
  } deriving (Show, Eq)

instance Default (Attribute a) where
  def = Attribute Nothing False

defAttr1 :: a -> Attribute a
defAttr1 x = Attribute (Just x) False

type ScopeReplicationFactors = [(NodeLocationScope, Int)]

data LogAttributes = LogAttributes
  { logReplicationFactor    :: Attribute Int
    -- ^ Number of nodes on which to persist a record.
    -- Optional if 'logReplicateAcross' is present.
  , logSyncedCopies         :: Attribute Int
    -- ^ The number of copies that must be acknowledged by storage nodes as
    -- synced to disk before the record is acknowledged to client as fully
    -- appended. Can be 0. Capped at replicationFactor.
  , logMaxWritesInFlight    :: Attribute Int
    -- ^ The largest number of records not released for delivery that the
    -- sequencer allows to be outstanding ('z' in the design doc).
  , logSingleWriter         :: Attribute Bool
    -- ^ Does LogDevice assume that there is a single writer for the log?
  , logSyncReplicationScope :: Attribute NodeLocationScope
    -- ^ The location scope to enforce failure domain properties, by default
    -- the scope is in the individual node level.
    -- logReplicateAcross provides a more general way to do the same thing.
  -- TODO: , logReplicateAcross      :: Attribute ScopeReplicationFactors
    -- ^ Defines cross-domain replication. A vector of replication factors
    -- at various scopes. When this option is given, replicationFactor_ is
    -- optional. This option is best explained by examples:
    --  - "node: 3, rack: 2" means "replicate each record to at least 3 nodes
    --    in at least 2 different racks".
    --  - "rack: 2" with replicationFactor_ = 3 mean the same thing.
    --  - "rack: 3, region: 2" with replicationFactor_ = 4 mean "replicate
    --    each record to at least 4 nodes in at least 3 different racks in at
    --    least 2 different regions"
    --  - "rack: 3" means "replicate each record to at least 3 nodes in
    --    at least 3 different racks".
    --  - "rack: 3" with replicationFactor_ = 3 means the same thing.
    -- Order of elements doesn't matter.
  , logBacklogDuration      :: Attribute (Maybe Int)
    -- ^ Duration that a record can exist in the log before it expires and
    -- gets deleted (in senconds). Valid value must be at least 1 second.
  -- TODO: , logScdEnabled           :: Attribute Bool
    -- ^ Indicate whether or not the Single Copy Delivery optimization should be
    -- used.
  -- TODO: , logLocalScdEnabled      :: Attribute Bool
    -- ^ Indicate whether or not to use Local Single Copy Delivery. This is
    -- ignored if scdEnabled_ is false.

  -- TODO
  -- nodeSetSize
  -- deliveryLatency
  -- writeToken
  -- stickyCopySets_
  -- mutablePerEpochLogMetadataEnabled
  -- permissions_
  -- acls_
  -- aclsShadow
  -- sequencerAffinity
  -- sequencerBatching
  -- sequencerBatchingTimeTrigger
  -- sequencerBatchingSizeTrigger
  -- sequencerBatchingCompression
  -- sequencerBatchingPassthruThreshold
  -- shadow
  -- tailOptimized

  , logAttrsExtras          :: Map CBytes CBytes
  } deriving (Show, Eq, Generic, Default)

pokeLogAttributes :: LogAttributes -> IO LDLogAttrs
pokeLogAttributes LogAttributes{..} =
#define _ARG(name) (attrValue name) $ \name##' ->
#define _MAYBE_ARG(name) (attrValue name) $ \name##_flag name##' ->
  withAllocMaybePrim fromIntegral _ARG(logReplicationFactor)
  withAllocMaybePrim fromIntegral _ARG(logSyncedCopies)
  withAllocMaybePrim fromIntegral _ARG(logMaxWritesInFlight)
  withAllocMaybePrim bool2cbool _ARG(logSingleWriter)
  withAllocMaybePrim unNodeLocationScope _ARG(logSyncReplicationScope)
  withAllocMaybePrim2 fromIntegral _MAYBE_ARG(logBacklogDuration)
  withHsCBytesMapUnsafe logAttrsExtras $ \l ks vs -> do
#define _ARG_TO(name) name##' (attrInherited name)
#define _MAYBE_ARG_TO(name) name##_flag name##' (attrInherited name)
    i <- poke_log_attributes _ARG_TO(logReplicationFactor)
                             _ARG_TO(logSyncedCopies)
                             _ARG_TO(logMaxWritesInFlight)
                             _ARG_TO(logSingleWriter)
                             _ARG_TO(logSyncReplicationScope)
                             _MAYBE_ARG_TO(logBacklogDuration)
                             l ks vs
    newForeignPtr free_log_attributes_fun i
#undef _ARG
#undef _ARG_TO
#undef _MAYBE_ARG
#undef _MAYBE_ARG_TO

peekLogAttributes :: Ptr LogDeviceLogAttributes -> IO LogAttributes
peekLogAttributes ptr = do
#define _ARG(name) name##_flag name##_val name##_inh
#define _MAYBE_ARG(name) name##_flag name##_val_flag name##_val name##_inh
  -- LogAttributes constructors
  (    logReplicationFactor
   , ( logSyncedCopies
   , ( logMaxWritesInFlight
   , ( logSingleWriter
   , ( logSyncReplicationScope
   , ( logBacklogDuration
   , _)))))) <-
    runPeek id $ \_ARG(replicationFactor) ->
    runPeek id $ \_ARG(syncedCopies) ->
    runPeek id $ \_ARG(maxWritesInFlight) ->
    runPeek cbool2bool $ \_ARG(singleWriter) ->
    runPeek NodeLocationScope $ \_ARG(syncReplicationScope) ->
    runPeekMaybe id $ \_MAYBE_ARG(backlogDuration) ->
      peek_log_attributes
        ptr
        _ARG(replicationFactor)
        _ARG(syncedCopies)
        _ARG(maxWritesInFlight)
        _ARG(singleWriter)
        _ARG(syncReplicationScope)
        _MAYBE_ARG(backlogDuration)
  logAttrsExtras <- peekLogAttributesExtras ptr
  return LogAttributes{..}
#undef _ARG
#undef _MAYBE_ARG

peekLogAttributesExtras :: Ptr LogDeviceLogAttributes -> IO (Map CBytes CBytes)
peekLogAttributesExtras attrs = do
  (len, (keys_ptr, (values_ptr, (keys_vec, (values_vec, _))))) <-
    Z.withPrimUnsafe (0 :: CSize) $ \len ->
    Z.withPrimUnsafe nullPtr $ \keys ->
    Z.withPrimUnsafe nullPtr $ \values ->
    Z.withPrimUnsafe nullPtr $ \keys_vec ->
    Z.withPrimUnsafe nullPtr $ \values_vec ->
      peek_log_attributes_extras
        attrs
        (MBA# len) (MBA# keys) (MBA# values) (MBA# keys_vec) (MBA# values_vec)
  finally
    (buildExtras (fromIntegral len) keys_ptr values_ptr)
    (delete_vector_of_string keys_vec <> delete_vector_of_string values_vec)
  where
    buildExtras len keys_ptr values_ptr = do
      keys <- peekStdStringToCBytesN len keys_ptr
      values <- peekStdStringToCBytesN len values_ptr
      return . Map.fromList $ zip keys values

-------------------------------------------------------------------------------

foreign import ccall unsafe "hs_logdevice.h poke_log_attributes"
  poke_log_attributes
    :: Ptr CInt -> Bool
    -- ^ logReplicationFactor
    -> Ptr CInt -> Bool
    -- ^ logSyncedCopies
    -> Ptr CInt -> Bool
    -- ^ logMaxWritesInFlight
    -> Ptr CBool -> Bool
    -- ^ logSingleWriter
    -> Ptr Word8 -> Bool
    -- ^ logSyncReplicationScope
    -> Bool -> Ptr CInt -> Bool
    -- ^ logBacklogDuration
    -> Int -> BAArray# Word8 -> BAArray# Word8
    -- ^ extras
    -> IO (Ptr LogDeviceLogAttributes)

foreign import ccall unsafe "hs_logdevice.h peek_log_attributes"
  peek_log_attributes
    :: Ptr LogDeviceLogAttributes
    -> MBA# CBool -> MBA# Int -> MBA# CBool
    -- ^ logReplicationFactor
    -> MBA# CBool -> MBA# Int -> MBA# CBool
    -- ^ logSyncedCopies
    -> MBA# CBool -> MBA# Int -> MBA# CBool
    -- ^ logMaxWritesInFlight
    -> MBA# CBool -> MBA# CBool -> MBA# CBool
    -- ^ logSingleWriter
    -> MBA# CBool -> MBA# Word8 -> MBA# CBool
    -- ^ logSyncReplicationScope
    -> MBA# CBool -> MBA# CBool -> MBA# Int -> MBA# CBool
    -- ^ logBacklogDuration
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h free_log_attributes"
  free_log_attributes :: Ptr LogDeviceLogAttributes -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_log_attributes"
  free_log_attributes_fun :: FunPtr (Ptr LogDeviceLogAttributes -> IO ())

-- TODO: merge into peek_log_attributes
foreign import ccall unsafe "hs_logdevice.h peek_log_attributes_extras"
  peek_log_attributes_extras
    :: Ptr LogDeviceLogAttributes
    -> MBA# CSize
    -> MBA# (Ptr Z.StdString)
    -> MBA# (Ptr Z.StdString)
    -> MBA# (Ptr (StdVector Z.StdString))
    -> MBA# (Ptr (StdVector Z.StdString))
    -> IO ()

-------------------------------------------------------------------------------

runPeek :: forall a b c. Prim a
        => (a -> b)
        -> (MBA# CBool -> MBA# a -> MBA# CBool -> IO c)
        -> IO (Attribute b, c)
runPeek t f = do
  (!flag, (val, (!inh, r))) <-
    Z.allocPrimUnsafe $ \flag'->
    Z.allocPrimUnsafe $ \val' ->
      Z.allocPrimUnsafe $ \inh' -> f (MBA# flag') (MBA# val') (MBA# inh')
  if (flag :: CInt) /= 0
     then pure (Attribute (Just $ t val) (cbool2bool inh), r)
     else pure (Attribute Nothing (cbool2bool inh), r)

runPeekMaybe
  :: forall a b c. Prim a
  => (a -> b)
  -> (MBA# CBool -> MBA# CBool -> MBA# a -> MBA# CBool -> IO c)
  -> IO (Attribute (Maybe b), c)
runPeekMaybe t f = do
  (!flag, (!val_flag, (val, (!inh, r)))) <-
    Z.allocPrimUnsafe $ \flag'->
    Z.allocPrimUnsafe $ \val_flag' ->
    Z.allocPrimUnsafe $ \val' ->
      Z.allocPrimUnsafe $ \inh' -> f (MBA# flag') (MBA# val_flag') (MBA# val') (MBA# inh')
  case (flag :: CInt, val_flag :: CInt) of
    (0, _) -> pure (Attribute Nothing (cbool2bool inh), r)
    (_, 0) -> pure (Attribute (Just Nothing) (cbool2bool inh), r)
    (_, _) -> pure (Attribute (Just $ Just $ t val) (cbool2bool inh), r)
