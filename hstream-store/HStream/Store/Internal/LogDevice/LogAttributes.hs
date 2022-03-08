{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}
{-# LANGUAGE BangPatterns   #-}
{-# LANGUAGE CPP            #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE MagicHash      #-}
{-# LANGUAGE NamedFieldPuns #-}
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

def1 :: a -> Attribute a
def1 x = Attribute (Just x) False

data LogAttributes = LogAttributes
  { logReplicationFactor :: Attribute Int
    -- ^ Number of nodes on which to persist a record.
    -- Optional if 'logReplicateAcross' is present.
  , logSyncedCopies      :: Attribute Int
    -- ^ The number of copies that must be acknowledged by storage nodes as
    -- synced to disk before the record is acknowledged to client as fully
    -- appended. Can be 0. Capped at replicationFactor.
  , logBacklogDuration   :: Attribute (Maybe Int)
    -- ^ Duration that a record can exist in the log before it expires and
    -- gets deleted (in senconds). Valid value must be at least 1 second.
  , logAttrsExtras       :: Map CBytes CBytes
  } deriving (Show, Eq, Generic, Default)

newLDLogAttrs :: LogAttributes -> IO LDLogAttrs
newLDLogAttrs LogAttributes{..} =
  withAllocMaybePrim (attrValue logReplicationFactor) $ \logReplicationFactor' ->
  withAllocMaybePrim (attrValue logSyncedCopies) $ \logSyncedCopies' ->
  withAllocMaybePrim2 (attrValue logBacklogDuration) $ \logBacklogDuration_flag logBacklogDuration' ->
  withHsCBytesMapUnsafe logAttrsExtras $ \l ks vs -> do
    i <- poke_log_attributes logReplicationFactor' (attrInherited logReplicationFactor)
                             logSyncedCopies' (attrInherited logSyncedCopies)
                             logBacklogDuration_flag logBacklogDuration' (attrInherited logBacklogDuration)
                             l ks vs
    newForeignPtr free_log_attributes_fun i

peekLogAttributes :: Ptr LogDeviceLogAttributes -> IO LogAttributes
peekLogAttributes ptr = do
  (logReplicationFactor, (logSyncedCopies, (logBacklogDuration, _))) <-
    runPeek id $ \replicationFactor_flag replicationFactor_val replicationFactor_inh ->
    runPeek id $ \syncedCopies_flag syncedCopies_val syncedCopies_inh ->
    runPeekMaybe id $ \backlogDuration_flag backlogDuration_val_flag backlogDuration_val backlogDuration_inh ->
        peek_log_attributes
          ptr
          replicationFactor_flag replicationFactor_val replicationFactor_inh
          syncedCopies_flag syncedCopies_val syncedCopies_inh
          backlogDuration_flag backlogDuration_val_flag backlogDuration_val backlogDuration_inh
  logAttrsExtras <- peekLogAttributesExtras ptr
  return $ LogAttributes { logReplicationFactor
                         , logSyncedCopies
                         , logBacklogDuration
                         , logAttrsExtras
                         }

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

-- Note: the args are actual "Ptr CInt" instead of "Ptr Int"
foreign import ccall unsafe "hs_logdevice.h poke_log_attributes"
  poke_log_attributes
    :: Ptr Int -> Bool
    -- ^ replicationFactor_
    -> Ptr Int -> Bool
    -- ^ syncedCopies_
    -> Bool -> Ptr Int -> Bool
    -- ^ backlogDuration_
    -> Int -> BAArray# Word8 -> BAArray# Word8
    -- extra
    -> IO (Ptr LogDeviceLogAttributes)

foreign import ccall unsafe "hs_logdevice.h peek_log_attributes"
  peek_log_attributes
    :: Ptr LogDeviceLogAttributes
    -> MBA# CBool -> MBA# Int -> MBA# CBool
    -- ^ replicationFactor_
    -> MBA# CBool -> MBA# Int -> MBA# CBool
    -- ^ syncedCopies_
    -> MBA# CBool -> MBA# CBool -> MBA# Int -> MBA# CBool
    -- ^ backlogDuration_
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h free_log_attributes"
  free_log_attributes :: Ptr LogDeviceLogAttributes -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_log_attributes"
  free_log_attributes_fun :: FunPtr (Ptr LogDeviceLogAttributes -> IO ())

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
