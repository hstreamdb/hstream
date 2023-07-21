{-# LANGUAGE DeriveAnyClass       #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.SQL.Binder.Common where

import           Control.Monad.Reader
import           Control.Monad.State
import qualified Data.Aeson            as Aeson
import qualified Data.Bimap            as Bimap
import           Data.Function         (on)
import           Data.Hashable
import qualified Data.HashMap.Strict   as HM
import           Data.IntMap           (IntMap)
import qualified Data.IntMap           as IntMap
import           Data.Kind             (Type)
import qualified Data.List             as L
import           Data.Maybe            (fromMaybe)
import           Data.Set              (Set)
import qualified Data.Set              as Set
import           Data.Text             (Text)
import qualified Data.Text             as T
import           GHC.Generics          (Generic)
import           GHC.Stack

import           HStream.SQL.Abs
import           HStream.SQL.Exception

----------------------------------------
--             schema
----------------------------------------
data ColumnCatalog = ColumnCatalog
  { columnId         :: !Int
  , columnName       :: !Text
  , columnStreamId   :: !Int -- Note: Only used in Planning phase & Runtime
  , columnStream     :: !Text
  , columnType       :: !BoundDataType
  , columnIsNullable :: !Bool
  , columnIsHidden   :: !Bool
  }
  deriving ( Ord, Generic, Aeson.ToJSON, Aeson.FromJSON
           , Aeson.ToJSONKey, Aeson.FromJSONKey, Hashable)
instance Show ColumnCatalog where
  show ColumnCatalog{..} = "#" <> show columnId <> "(" <>
                            T.unpack columnStream <> "." <>
                            T.unpack columnName <> ")"

instance Eq ColumnCatalog where
  c1 == c2 = columnName   c1 == columnName   c2 &&
             columnStream c1 == columnStream c2

-- | Compose two 'IntMap's of schemaColumns. It assumes that the two 'IntMap's have
-- contiguous int keys starting from 0. The result 'IntMap' will still have contiguous
-- int keys starting from 0 and of course, end with sum of the two 'IntMap's length.
-- Example: m1 = {0: c1, 1: c2}, m2 = {0: c3, 1: c4, 2: c5}
--          m1 <:+:> m2 = {0: c1, 1: c2, 2: c3, 3: c4, 4: c5}

-- IMPORTANT: This function is not commutative.
-- IMPORTANT: This function DOES NOT change the `columnId` of the columns in
--            catalogs.
(<:+:>) :: IntMap ColumnCatalog
       -> IntMap ColumnCatalog
       -> IntMap ColumnCatalog
(<:+:>) m1 m2 =
  let tups1 = IntMap.toList m1
      tups2 = IntMap.toList m2
      maxId1 = if null tups1 then 0 else fst (L.maximumBy (compare `on` fst) tups1)
      tups2' = L.map (\(i,c) -> (i+maxId1+1, c)) tups2
   in IntMap.fromList (tups1 <> tups2')

regularizeColumnCatalogs :: Set ColumnCatalog -> Set ColumnCatalog
regularizeColumnCatalogs x = Set.fromList $
  map (\(catalog, i) -> catalog { columnId = i }) (Set.toList x `zip` [0..])

data Schema = Schema
  { schemaOwner   :: Text
  , schemaColumns :: IntMap ColumnCatalog
  }
  deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show Schema where
  show Schema{..} = "Schema { name=" <> T.unpack schemaOwner
                 <> ", columns=" <> show schemaColumns
                 <> "}"

mockGetSchema :: Text -> IO (Maybe Schema)
mockGetSchema s
  | s == "s1" = return . Just $ Schema { schemaOwner = "s1"
                                       , schemaColumns = IntMap.fromList [ (0, ColumnCatalog 0 "a" 0 "s1" BTypeInteger False False)
                                                                         , (1, ColumnCatalog 1 "b" 0 "s1" BTypeText    False False)
                                                                         ]
                                       }
  | s == "s2" = return . Just $ Schema { schemaOwner = "s2"
                                       , schemaColumns = IntMap.fromList [ (0, ColumnCatalog 0 "a" 0 "s2" BTypeInteger False False)
                                                                         , (1, ColumnCatalog 1 "c" 0 "s2" BTypeInteger False False)
                                                                         ]
                                       }
  | otherwise = return Nothing

----------------------------------------
--         binder context
----------------------------------------
data BindUnitType = BindUnitBase | BindUnitTemp | BindUnitDummy
  deriving (Show)

newtype BindContextLayer = BindContextLayer
  { layerColumnBindings
    :: HM.HashMap Text (HM.HashMap Text (Int, BindUnitType))
    -- columnName -> (streamName -> (columnId, bindType))
  } deriving (Show)

instance Semigroup BindContextLayer where
  BindContextLayer a <> BindContextLayer b = BindContextLayer (HM.unionWith HM.union a b)
instance Monoid BindContextLayer where
  mempty = BindContextLayer mempty

data BindContext = BindContext
  { bindContextLayers    :: [BindContextLayer]    -- a stack, head is top
  , bindContextAliases   :: Bimap.Bimap Text Text -- alias <-> original name
  , bindLocalTablesCount :: Int
  } deriving (Show)

defaultBindContext :: BindContext
defaultBindContext = BindContext
  { bindContextLayers    = []
  , bindContextAliases   = Bimap.empty
  , bindLocalTablesCount = 0
  }

-- get
-- NOTE: currently the column name inserted into the ctx is the original one
--       instead of 'AS'. So do not forget to look up the alias table.
lookupColumn :: BindContext -> Text -> Maybe (Text, Int, BindUnitType)
lookupColumn (BindContext [] _ _) _   = Nothing
lookupColumn (BindContext (x:xs) aliases n) col =
  case HM.lookup col (layerColumnBindings x) of
    Nothing -> lookupColumn (BindContext xs aliases n) col
    Just m  -> case HM.toList m of
      [(stream, (colId, bindType))] -> Just (stream, colId, bindType)
      _                             -> error $ "ambiguous column name: " <> T.unpack col

lookupColumnWithStream :: BindContext -> Text -> Text -> Maybe (Text, Int, BindUnitType)
lookupColumnWithStream (BindContext [] _ _) _ _ = Nothing
lookupColumnWithStream (BindContext (x:xs) aliases n) col stream =
  case HM.lookup col (layerColumnBindings x) of
    Nothing -> lookupColumnWithStream (BindContext xs aliases n) col stream
    Just m  -> case Bimap.lookup stream aliases of
      Nothing           -> error $ "stream alias not found: " <> T.unpack stream <> ", what happened...?"
      Just originalName -> case HM.lookup originalName m of
        Nothing -> lookupColumnWithStream (BindContext xs aliases n) col stream
        Just (colId, bindType) -> Just (originalName, colId, bindType)

listColumns :: BindContext -> [(Text, Text, Int)]
listColumns (BindContext xs _ _) =
  let hm = layerColumnBindings (mconcat xs)
   in concatMap (\(col, hm') ->
                   foldr (\(stream,(i, _)) acc -> (stream,col,i) : acc) [] (HM.toList hm')
                ) (HM.toList hm)

listColumnsByStream :: BindContext -> Text -> [(Text, Text, Int)]
listColumnsByStream (BindContext xs aliases _) stream =
  case Bimap.lookup stream aliases of
    Nothing           -> error $ "stream alias not found: " <> T.unpack stream <> ", what happened...?"
    Just originalName ->
      let hm = layerColumnBindings (mconcat xs)
      in foldr (\(col, hm') acc ->
                case HM.lookup originalName hm' of
                  Nothing         -> acc
                  Just (colId, _) -> (originalName, col, colId) : acc
               ) [] (HM.toList hm)

-- update
pushLayer :: MonadState BindContext m => BindContextLayer -> m ()
pushLayer layer = modify $ \ctx -> ctx { bindContextLayers = layer : bindContextLayers ctx }

popLayer :: MonadState BindContext m => m BindContextLayer
popLayer = do
  ctx <- get
  case bindContextLayers ctx of
    []     -> error "pop empty bind context"
    (x:xs) -> do
      put $ ctx { bindContextLayers = xs }
      return x

popNLayer :: MonadState BindContext m => Int -> m [BindContextLayer]
popNLayer n = replicateM n popLayer

joinLayer :: MonadState BindContext m => m ()
joinLayer = do
  layer1 <- popLayer
  layer2 <- popLayer
  pushLayer $ layer1 <> layer2

schemaToContextLayer :: Schema -> Maybe Text -> BindUnitType -> BindContextLayer
schemaToContextLayer Schema{..} alias_m bindType = BindContextLayer $ HM.fromList $
  L.map (\ColumnCatalog{..} -> (columnName, HM.singleton streamName (columnId, bindType)))
        (IntMap.elems schemaColumns)
  where streamName = fromMaybe schemaOwner alias_m

genSubqueryName :: MonadState BindContext m => m Text
genSubqueryName = do
  ctx <- get
  let n = bindLocalTablesCount ctx
  put $ ctx { bindLocalTablesCount = n + 1 }
  return $ T.pack ("__subquery#" <> show n)

genJoinName :: MonadState BindContext m => m Text
genJoinName = do
  ctx <- get
  let n = bindLocalTablesCount ctx
  put $ ctx { bindLocalTablesCount = n + 1 }
  return $ T.pack ("__join#" <> show n)

--
scanColumns :: BindContextLayer -> BindContextLayer -> [Text]
scanColumns layer1 layer2 =
  let
    colNames1 = HM.keys $ layerColumnBindings layer1
    colNames2 = HM.keys $ layerColumnBindings layer2
  in
    colNames1 `L.intersect` colNames2

----------------------------------------
--           Bind class
----------------------------------------
type family BoundType a :: Type
class Bind a where
  bind' :: ( HasCallStack
           , MonadIO m
           , MonadReader (Text -> IO (Maybe Schema)) m
           , MonadState BindContext m
           ) => a -> m (BoundType a, Int)
  bind  :: ( HasCallStack
           , MonadIO m
           , MonadReader (Text -> IO (Maybe Schema)) m
           , MonadState BindContext m
           ) => a -> m (BoundType a)
  bind a = fst <$> bind' a
  bind' a = do
    result <- bind a
    return (result, 0)
  {-# MINIMAL bind | bind' #-}

----------------------------------------
--          other classes
----------------------------------------
class HasName a where
  getName :: a -> String

----------------------------------------
--          booting types
----------------------------------------
data BoundDataType
  = BTypeInteger  | BTypeFloat | BTypeBoolean
  | BTypeBytea    | BTypeText
  | BTypeDate     | BTypeTime | BTypeTimestamp
  | BTypeInterval | BTypeJsonb
  | BTypeArray BoundDataType
  deriving ( Eq, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON
           , Aeson.ToJSONKey, Aeson.FromJSONKey, Hashable)
instance Show BoundDataType where
  show typ = case typ of
    BTypeInteger   -> "int"
    BTypeFloat     -> "float"
    BTypeBoolean   -> "bool"
    BTypeBytea     -> "bytea"
    BTypeText      -> "text"
    BTypeDate      -> "date"
    BTypeTime      -> "time"
    BTypeTimestamp-> "timestamp"
    BTypeInterval  -> "interval"
    BTypeJsonb     -> "jsonb"
    BTypeArray t   -> "[" <> show t <> "]"

type instance BoundType DataType = BoundDataType
instance Bind DataType where
  bind TypeInteger  {} = return BTypeInteger
  bind TypeFloat    {} = return BTypeFloat
  bind TypeBoolean  {} = return BTypeBoolean
  bind TypeByte     {} = return BTypeBytea
  bind TypeText     {} = return BTypeText
  bind TypeDate     {} = return BTypeDate
  bind TypeTime     {} = return BTypeTime
  bind TypeTimestamp{} = return BTypeTimestamp
  bind TypeInterval {} = return BTypeInterval
  bind TypeJson     {} = return BTypeJsonb
  bind (TypeArray _ t) = bind t >>= return . BTypeArray

----------------------------------------
--          exceptions
----------------------------------------
throwImpossible :: a
throwImpossible = throwSQLException BindException Nothing "Impossible happened"
