module HStream.MetaStore.FileUtils where

import           Control.Exception    (catch, throw, throwIO)
import           Data.Aeson           (FromJSON, ToJSON)
import qualified Data.Aeson           as A
import qualified Data.Aeson.Text      as A
import           Data.Bifunctor       (first)
import           Data.ByteString      (hGetContents, hPut)
import qualified Data.ByteString.Lazy as BSL
import           Data.Map.Strict      (Map)
import qualified Data.Map.Strict      as M
import           Data.Maybe           (fromMaybe)
import           Data.Text            (Text)
import qualified Data.Text.Encoding   as T
import qualified Data.Text.Lazy       as TL
import           GHC.IO.Handle.FD     (openBinaryFile)
import           System.FileLock      (SharedExclusive (..), withFileLock)
import           System.IO            (IOMode (..), withBinaryFile)

import qualified HStream.Exception    as HE

type Table = Text
type Key = Text
type EncodedValue = Text
type Version = Int
type Contents = Map Table (Map Key (EncodedValue, Version))

data MetaOp
  = InsertOp Table Key EncodedValue
  | UpdateOp Table Key EncodedValue (Maybe Version)
  | DeleteOp Table Key (Maybe Version)
  | CheckOp  Table Key Version
  deriving (Show, Eq)

metaOpToAction :: MetaOp -> Contents -> Contents
metaOpToAction (InsertOp t k v)     = insertIntoTablePure t k v
metaOpToAction (UpdateOp t k v ver) = updateSetPure t k v ver
metaOpToAction (DeleteOp t k ver)   = deleteFromTablePure t k ver
metaOpToAction (CheckOp  t k ver)   = \x -> case selectFromPure t k x of
  Nothing -> throw $ HE.LocalMetaStoreObjectNotFound $ "Object with key " <> show k <> " does not exist"
  Just (_, ver') -> if ver == ver' then x else throw $ HE.LocalMetaStoreObjectBadVersion $ "Version " <> show ver <> "does not match " <> show ver'

metaOpsToAction :: [MetaOp] -> Contents -> Contents
metaOpsToAction = foldr ((.) . metaOpToAction) id . reverse

runOps :: [MetaOp] -> FilePath -> IO ()
runOps = actionWrap . metaOpsToAction

createTable :: Table -> FilePath -> IO ()
createTable = actionWrap . createTablePure

createTables :: [Table] -> FilePath -> IO ()
createTables = actionWrap . foldr f id
  where
    f x y = y . createTablePure x

deleteTable :: Table -> FilePath -> IO ()
deleteTable = actionWrap . deleteTablePure

createTablePure :: Table -> Contents -> Contents
createTablePure = flip (M.insertWith (<>)) mempty

deleteTablePure :: Table -> Contents -> Contents
deleteTablePure = M.delete

insertIntoTable :: ToJSON value => Table -> Key -> value -> FilePath -> IO ()
insertIntoTable t k = actionWrap . insertIntoTablePure t k . encode

insertIntoTablePure :: Table -> Key -> EncodedValue -> Contents -> Contents
insertIntoTablePure t k v = M.update (Just . M.insertWithKey insertHelp k (v, 0)) t

deleteFromTable :: Table -> Key -> Maybe Version -> FilePath -> IO ()
deleteFromTable t k = actionWrap . deleteFromTablePure t k

deleteFromTablePure :: Table -> Key -> Maybe Version -> Contents -> Contents
deleteFromTablePure t k ver = M.update (Just . M.alter (deleteHelp k ver) k) t

deleteAllFromTable :: Table -> FilePath -> IO ()
deleteAllFromTable = actionWrap . deleteAllFromTablePure

deleteAllFromTablePure :: Table -> Contents -> Contents
deleteAllFromTablePure t = M.insert t mempty

updateSet :: ToJSON value =>Table -> Key -> value -> Maybe Version -> FilePath -> IO ()
updateSet t k v = actionWrap . updateSetPure t k (encode v)

updateSetPure :: Table -> Key -> EncodedValue -> Maybe Version -> Contents -> Contents
updateSetPure t k v ver = M.update (Just . M.alter (Just . updateHelp k ver v) k) t

upsert :: ToJSON value => Table -> Key -> value -> FilePath -> IO ()
upsert t k  = actionWrap . upsertPure t k . encode

upsertPure :: Table -> Key -> EncodedValue -> Contents -> Contents
upsertPure t k v =  M.update (Just . M.alter (Just . upsertHelp v) k) t

selectFromPure :: Table -> Key -> Contents -> Maybe (EncodedValue, Version)
selectFromPure t key c = M.lookup key =<< M.lookup t c

selectAllFromPure ::  Table -> Contents -> Map Key (EncodedValue, Version)
selectAllFromPure = (fromMaybe mempty .) . M.lookup

selectAllFrom :: FromJSON value => Table -> FilePath -> IO (Map Key (value, Version))
selectAllFrom = getWrap . (fmap (first decode) .) . selectAllFromPure

selectFrom :: FromJSON value => Table -> Key -> FilePath -> IO (Maybe (value, Version))
selectFrom t = getWrap .  (fmap (first decode) .) . selectFromPure t

updateHelp :: Key -> Maybe Version -> EncodedValue -> Maybe (EncodedValue, Version) -> (EncodedValue, Version)
updateHelp key _           _  Nothing        = throw $ HE.LocalMetaStoreObjectNotFound $ "Object with " <> show key <> " not found"
updateHelp _ Nothing     v (Just (_, ver)  ) = (v, ver + 1)
updateHelp _ (Just ver1) v (Just (v2, ver2)) = if ver1 == ver2 then (v, ver1 + 1) else (v2, ver2)

deleteHelp :: Key -> Maybe Version -> Maybe (EncodedValue, Version) -> Maybe (EncodedValue, Version)
deleteHelp key _       Nothing = throw $ HE.LocalMetaStoreObjectNotFound $ "Object with key" <> show key <> "not found"
deleteHelp _ Nothing _       = Nothing
deleteHelp _ (Just ver1) (Just x@(_, ver2)) =
  if ver1 == ver2 then Nothing else throw $ HE.LocalMetaStoreObjectBadVersion $ show ver2 <> " does not match " <> show ver1

insertHelp :: Show k => k -> a -> a -> a
insertHelp key _ _ = throw $ HE.LocalMetaStoreObjectAlreadyExists $ "Object with "<> show key <> "already exists"

upsertHelp :: EncodedValue -> Maybe (EncodedValue, Version) -> (EncodedValue, Version)
upsertHelp v Nothing         = (v, 0)
upsertHelp v (Just (_, ver)) = (v, ver + 1)

actionWrap :: (Contents -> Contents) -> FilePath -> IO ()
actionWrap action fp = withFileLock fp Exclusive $ \_ -> do
  handle <- openBinaryFile fp ReadMode
  x <- hGetContents handle
  withBinaryFile fp WriteMode $ \handle' ->
    case A.decode (BSL.fromStrict x) of
      Nothing -> putStrLn "Failed to decode contents" >> throwIO (HE.LocalMetaStoreInternalErr "Failed to decode contents")
      Just contents -> catch (hPut handle' (BSL.toStrict . A.encode $ action contents)) (\(e::HE.SomeHServerException) -> hPut handle' x >> throw e)

getWrap :: (Contents -> a) -> FilePath -> IO a
getWrap get fp = withFileLock fp Shared $ \_ -> do
  handle <- openBinaryFile fp ReadMode
  x <- hGetContents handle
  case A.decode (BSL.fromStrict x) of
    Nothing -> throwIO $ HE.LocalMetaStoreInternalErr "Failed to decode contents"
    Just contents -> return $ get contents

encode :: ToJSON a => a -> EncodedValue
encode = TL.toStrict . A.encodeToLazyText

decode :: FromJSON a => EncodedValue -> a
decode = fromMaybe (throw $ HE.LocalMetaStoreInternalErr "Failed to decode encoded value") . A.decode . BSL.fromStrict . T.encodeUtf8
