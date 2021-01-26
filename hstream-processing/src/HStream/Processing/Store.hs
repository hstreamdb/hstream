{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE NoImplicitPrelude         #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE StrictData                #-}

module HStream.Processing.Store
  ( KVStore (..),
    SessionStore (..),
    TimestampedKVStore (..),
    StateStore,
    InMemoryKVStore,
    -- mkInMemoryKVStore,
    mkInMemoryStateKVStore,
    mkDEKVStore,
    EKVStore,
    DEKVStore,
    InMemorySessionStore,
    -- mkInMemorySessionStore,
    mkInMemoryStateSessionStore,
    fromEStateStoreToKVStore,
    fromEStateStoreToSessionStore,
    wrapStateStore,
    EStateStore,
    ESessionStore,
    mkInMemoryStateTimestampedKVStore,
    ETimestampedKVStore,
    fromEStateStoreToTimestampedKVStore,
  )
where

import           Control.Exception                        (throw)
import           Data.Maybe
import           Data.Typeable
import           HStream.Processing.Error
import           HStream.Processing.Stream.SessionWindows
import           HStream.Processing.Stream.TimeWindows
import           HStream.Processing.Type
import           RIO
import qualified RIO.Map                                  as Map
import qualified RIO.Text                                 as T

data InMemoryKVStore k v
  = InMemoryKVStore
      { imksData :: IORef (Map k v)
      }

mkInMemoryKVStore :: IO (InMemoryKVStore k v)
mkInMemoryKVStore = do
  internalData <- newIORef Map.empty
  return
    InMemoryKVStore
      { imksData = internalData
      }

class KVStore s where
  ksGet :: Ord k => k -> s k v -> IO (Maybe v)
  ksPut :: Ord k => k -> v -> s k v -> IO ()
  ksRange :: Ord k => k -> k -> s k v -> IO [(k, v)]

instance KVStore InMemoryKVStore where
  ksGet k InMemoryKVStore {..} = do
    dict <- readIORef imksData
    return $ Map.lookup k dict

  ksPut k v InMemoryKVStore {..} = do
    dict <- readIORef imksData
    writeIORef imksData (Map.insert k v dict)

  ksRange fromKey toKey InMemoryKVStore {..} = do
    dict <- readIORef imksData
    let (_, ms, lm) = Map.splitLookup fromKey dict
    let (sm, ml, _) = Map.splitLookup toKey lm
    let r1 = do
          r0 <- fmap (\v -> Map.insert fromKey v sm) ms
          fmap (\v -> Map.insert toKey v r0) ml
    case r1 of
      Just rm -> return $ Map.toAscList rm
      Nothing -> return $ Map.toAscList sm

data EKVStore k v
  = forall s.
    KVStore s =>
    EKVStore
      (s k v)

data DEKVStore
  = forall k v.
    (Typeable k, Typeable v) =>
    DEKVStore
      (EKVStore k v)

instance KVStore EKVStore where
  ksGet k (EKVStore s) = ksGet k s

  ksPut k v (EKVStore s) = ksPut k v s

  ksRange fromKey toKey (EKVStore s) = ksRange fromKey toKey s

mkDEKVStore ::
  (KVStore s, Typeable k, Typeable v, Ord k) =>
  s k v ->
  DEKVStore
mkDEKVStore store = DEKVStore (EKVStore store)

fromDEKVStoreToEKVStore ::
  (Typeable k, Typeable v, Ord k) =>
  DEKVStore ->
  EKVStore k v
fromDEKVStoreToEKVStore (DEKVStore eStore) =
  case cast eStore of
    Just es -> es
    Nothing -> throw $ TypeCastError "fromDEKVStoreToEKVStore: type cast error"

data StateStore k v
  = KVStateStore (EKVStore k v)
  | SessionStateStore (ESessionStore k v)
  | TimestampedKVStateStore (ETimestampedKVStore k v)

data EStateStore
  = EKVStateStore DEKVStore
  | ESessionStateStore DESessionStore
  | ETimestampedKVStateStore DETimestampedKVStore

wrapStateStore ::
  (Typeable k, Typeable v) =>
  StateStore k v ->
  EStateStore
wrapStateStore stateStore =
  case stateStore of
    KVStateStore kvStore -> EKVStateStore (DEKVStore kvStore)
    SessionStateStore sessionStore -> ESessionStateStore (DESessionStore sessionStore)
    TimestampedKVStateStore timestampedKVStore -> ETimestampedKVStateStore (DETimestampedKVStore timestampedKVStore)

class SessionStore s where
  ssGet :: (Typeable k, Ord k) => SessionWindowKey k -> s k v -> IO (Maybe v)
  ssPut :: (Typeable k, Ord k) => SessionWindowKey k -> v -> s k v -> IO ()
  ssRemove :: (Typeable k, Ord k) => SessionWindowKey k -> s k v -> IO ()
  findSessions :: (Typeable k, Ord k) => k -> Timestamp -> Timestamp -> s k v -> IO [(SessionWindowKey k, v)]

data ESessionStore k v
  = forall s.
    SessionStore s =>
    ESessionStore
      (s k v)

instance SessionStore ESessionStore where
  ssGet k (ESessionStore s) = ssGet k s

  ssPut k v (ESessionStore s) = ssPut k v s

  ssRemove k (ESessionStore s) = ssRemove k s

  findSessions k ts1 ts2 (ESessionStore s) = findSessions k ts1 ts2 s

data DESessionStore
  = forall k v.
    (Typeable k, Typeable v) =>
    DESessionStore
      (ESessionStore k v)

fromDESessionStoreToESessionStore ::
  (Typeable k, Typeable v, Ord k) =>
  DESessionStore ->
  ESessionStore k v
fromDESessionStoreToESessionStore (DESessionStore eStore) =
  case cast eStore of
    Just es -> es
    Nothing ->
      throw
        $ TypeCastError
        $ "fromDESessionStoreToESessionStore: type cast error, actual eStore type is " `T.append` T.pack (show $ typeOf eStore)

data InMemorySessionStore k v
  = InMemorySessionStore
      { imssData :: IORef (Map Timestamp (IORef (Map k (IORef (Map Timestamp v)))))
      }

mkInMemorySessionStore :: IO (InMemorySessionStore k v)
mkInMemorySessionStore = do
  internalData <- newIORef Map.empty
  return
    InMemorySessionStore
      { imssData = internalData
      }

instance SessionStore InMemorySessionStore where
  ssGet TimeWindowKey {..} InMemorySessionStore {..} = do
    let ws = tWindowStart twkWindow
    let we = tWindowEnd twkWindow
    dict0 <- readIORef imssData
    case Map.lookup we dict0 of
      Just rd -> do
        dict1 <- readIORef rd
        case Map.lookup twkKey dict1 of
          Just rd1 -> do
            dict2 <- readIORef rd1
            return $ Map.lookup ws dict2
          Nothing -> return Nothing
      Nothing -> return Nothing

  ssPut TimeWindowKey {..} v InMemorySessionStore {..} = do
    let ws = tWindowStart twkWindow
    let we = tWindowEnd twkWindow
    dict0 <- readIORef imssData
    case Map.lookup we dict0 of
      Just rd -> do
        dict1 <- readIORef rd
        case Map.lookup twkKey dict1 of
          Just rd1 -> do
            dict2 <- readIORef rd1
            writeIORef rd1 (Map.insert ws v dict2)
          Nothing -> do
            rd1 <- newIORef $ Map.singleton ws v
            writeIORef rd (Map.insert twkKey rd1 dict1)
      Nothing -> do
        rd1 <- newIORef $ Map.singleton ws v
        rd2 <- newIORef $ Map.singleton twkKey rd1
        writeIORef imssData (Map.insert we rd2 dict0)

  ssRemove TimeWindowKey {..} InMemorySessionStore {..} = do
    let ws = tWindowStart twkWindow
    let we = tWindowEnd twkWindow
    dict0 <- readIORef imssData
    case Map.lookup we dict0 of
      Just rd -> do
        dict1 <- readIORef rd
        case Map.lookup twkKey dict1 of
          Just rd1 -> do
            dict2 <- readIORef rd1
            writeIORef rd1 (Map.delete ws dict2)
          Nothing -> return ()
      Nothing -> return ()

  findSessions key earliestSessionEndTime latestSessionStartTime InMemorySessionStore {..} = do
    dict0 <- readIORef imssData
    let (_, mv0, tailDict0') = Map.splitLookup earliestSessionEndTime dict0
    let tailDict0 =
          if isJust mv0
            then Map.insert earliestSessionEndTime (fromJust mv0) tailDict0'
            else tailDict0'
    Map.foldlWithKey'
      ( \macc et rd1 -> do
          acc <- macc
          dict1 <- readIORef rd1
          case Map.lookup key dict1 of
            Just rd2 -> do
              dict2 <- readIORef rd2
              let (headDict2', mv2, _) = Map.splitLookup latestSessionStartTime dict2
              let headDict2 =
                    if isJust mv2
                      then Map.insert latestSessionStartTime (fromJust mv2) headDict2'
                      else headDict2'
              return $
                Map.foldlWithKey'
                  ( \acc1 st v ->
                      acc1 ++ [(mkTimeWindowKey key (mkTimeWindow st et), v)]
                  )
                  acc
                  headDict2
            Nothing -> return acc
      )
      (return [])
      tailDict0

mkInMemoryStateKVStore :: IO (StateStore k v)
mkInMemoryStateKVStore = do
  store <- mkInMemoryKVStore
  return $ KVStateStore $ EKVStore store

mkInMemoryStateSessionStore :: IO (StateStore k v)
mkInMemoryStateSessionStore = do
  store <- mkInMemorySessionStore
  return $ SessionStateStore $ ESessionStore store

mkInMemoryStateTimestampedKVStore :: IO (StateStore k v)
mkInMemoryStateTimestampedKVStore = do
  store <- mkInMemoryTimestampedKVStore
  return $ TimestampedKVStateStore $ ETimestampedKVStore store

fromEStateStoreToKVStore ::
  (Typeable k, Typeable v, Ord k) =>
  EStateStore ->
  EKVStore k v
fromEStateStoreToKVStore eStore =
  case eStore of
    EKVStateStore s -> fromDEKVStoreToEKVStore s
    _ -> throw $ UnExpectedStateStoreType "expect KVStateStore"

fromEStateStoreToSessionStore ::
  (Typeable k, Typeable v, Ord k) =>
  EStateStore ->
  ESessionStore k v
fromEStateStoreToSessionStore eStore =
  case eStore of
    ESessionStateStore s -> fromDESessionStoreToESessionStore s
    _ -> throw $ UnExpectedStateStoreType "expect SessionStateStore"

fromEStateStoreToTimestampedKVStore ::
  (Typeable k, Typeable v, Ord k) =>
  EStateStore ->
  ETimestampedKVStore k v
fromEStateStoreToTimestampedKVStore eStore =
  case eStore of
    ETimestampedKVStateStore s -> fromDETimestampedKVStoreToETimestampedKVStore s
    _ -> throw $ UnExpectedStateStoreType "expect TimestampdKVStateStore"

class TimestampedKVStore s where
  tksGet :: Ord k => TimestampedKey k -> s k v -> IO (Maybe v)
  tksPut :: Ord k => TimestampedKey k -> v -> s k v -> IO ()
  tksRange :: Ord k => TimestampedKey k -> TimestampedKey k -> s k v -> IO [(TimestampedKey k, v)]

data InMemoryTimestampedKVStore k v
  = InMemoryTimestampedKVStore
      { imtksData :: IORef (Map Int64 (IORef (Map k v)))
      }

mkInMemoryTimestampedKVStore :: IO (InMemoryTimestampedKVStore k v)
mkInMemoryTimestampedKVStore = do
  internalData <- newIORef Map.empty
  return
    InMemoryTimestampedKVStore
      { imtksData = internalData
      }

instance TimestampedKVStore InMemoryTimestampedKVStore where
  tksGet TimestampedKey {..} InMemoryTimestampedKVStore {..} = do
    map1 <- readIORef imtksData
    case Map.lookup tkTimestamp map1 of
      Just rmap2 -> do
        map2 <- readIORef rmap2
        return $ Map.lookup tkKey map2
      Nothing -> return Nothing

  tksPut TimestampedKey {..} v InMemoryTimestampedKVStore {..} = do
    map1 <- readIORef imtksData
    case Map.lookup tkTimestamp map1 of
      Just rmap2 -> do
        map2 <- readIORef rmap2
        let newMap2 = Map.insert tkKey v map2
        writeIORef rmap2 newMap2
      Nothing -> do
        let map2 = Map.singleton tkKey v
        rmap2 <- newIORef map2
        let newMap1 = Map.insert tkTimestamp rmap2 map1
        writeIORef imtksData newMap1

  tksRange fromKey toKey InMemoryTimestampedKVStore {..} = do
    map1 <- readIORef imtksData
    let (_, ms, lm) = Map.splitLookup (tkTimestamp fromKey) map1
    let (sm, ml, _) = Map.splitLookup (tkTimestamp toKey) lm
    let r1 = do
          r0 <- fmap (\v -> Map.insert (tkTimestamp fromKey) v sm) ms
          fmap (\v -> Map.insert (tkTimestamp toKey) v r0) ml
    let innerKey = tkKey fromKey
    case r1 of
      Just rm -> genR innerKey rm
      Nothing -> genR innerKey sm
    where
      genR :: Ord k => k -> Map Int64 (IORef (Map k v)) -> IO [(TimestampedKey k, v)]
      -- 需要遍历这个 map,
      -- 找到 key 匹配的然后返回.
      genR key dict =
        Map.foldlWithKey'
          ( \macc ts rmap2 -> do
              acc <- macc
              map2 <- readIORef rmap2
              case Map.lookup key map2 of
                Just v -> return $ acc ++ [(mkTimestampedKey key ts, v)]
                Nothing -> return acc
          )
          (return [])
          dict

data ETimestampedKVStore k v
  = forall s.
    TimestampedKVStore s =>
    ETimestampedKVStore
      (s k v)

data DETimestampedKVStore
  = forall k v.
    (Typeable k, Typeable v) =>
    DETimestampedKVStore
      (ETimestampedKVStore k v)

fromDETimestampedKVStoreToETimestampedKVStore ::
  (Typeable k, Typeable v, Ord k) =>
  DETimestampedKVStore ->
  ETimestampedKVStore k v
fromDETimestampedKVStoreToETimestampedKVStore (DETimestampedKVStore eStore) =
  case cast eStore of
    Just es -> es
    Nothing -> throw $ TypeCastError "fromDEKVStoreToEKVStore: type cast error"

instance TimestampedKVStore ETimestampedKVStore where
  tksGet k (ETimestampedKVStore s) = tksGet k s

  tksPut k v (ETimestampedKVStore s) = tksPut k v s

  tksRange fromKey toKey (ETimestampedKVStore s) = tksRange fromKey toKey s
