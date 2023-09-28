{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MagicHash         #-}
{-# LANGUAGE TupleSections     #-}

module HStream.Common.Query
  ( QueryBaseRep (..)
  , QueryResult (..)
  , ActiveQueryMetadata (..)
  , FailedNodeDetails (..)
    -- * Show tables
  , showTables, getTables, formatTables
    -- * Describe table
  , showTableColumns, getTableColumns, formatTableColumns
    -- * Run queries
  , runQuery, execStatement, formatQueryResults
  ) where

import           Control.Exception
import           Control.Monad         (forM)
import           Data.Bifunctor        (second)
import           Data.Default          (def)
import           Data.IntMap.Strict    (IntMap)
import qualified Data.IntMap.Strict    as IntMap
import           Data.Primitive
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Marshal.Alloc (free)
import           Foreign.Ptr
import qualified Text.Layout.Table     as Table
import qualified Z.Data.CBytes         as CBytes
import           Z.Data.CBytes         (CBytes)
import qualified Z.Foreign             as Z

import           HStream.Base.Table    (simpleShowTable)
import           HStream.Foreign

-------------------------------------------------------------------------------
-- QueryBase

data C_QueryBase
type QueryBase = ForeignPtr C_QueryBase

class QueryBaseRep a where
  eqQueryBaseRep :: a -> QueryBase

instance QueryBaseRep QueryBase where
  eqQueryBaseRep = id

data C_QueryResults
type QueryResults = ForeignPtr C_QueryResults

data FailedNodeDetails = FailedNodeDetails
  { fndAddress :: CBytes
  , fndReason  :: CBytes
  } deriving (Show)

data ActiveQueryMetadata = ActiveQueryMetadata
  { metadataFailures       :: IntMap FailedNodeDetails
  , metadataContactedNodes :: Word64
  , metadataLatency        :: Word64
  } deriving (Show)

data QueryResult = QueryResult
  { resultHeaders     :: [CBytes]
  , resultRows        :: [[CBytes]]
  , resultColsMaxSize :: [CSize]
  , resultMetadata    :: ActiveQueryMetadata
  } deriving (Show)

showTables :: QueryBaseRep a => a -> IO String
showTables = (formatTables <$>) . getTables

getTables :: QueryBaseRep a => a -> IO [(CBytes, CBytes)]
getTables ldq = withForeignPtr (eqQueryBaseRep ldq) $ \ldq' -> do
  (len, (name_val, (name_del, (desc_val, (desc_del, _))))) <-
    Z.withPrimUnsafe (0 :: CSize) $ \len' ->
    Z.withPrimUnsafe nullPtr $ \name_val' ->
    Z.withPrimUnsafe nullPtr $ \name_del' ->
    Z.withPrimUnsafe nullPtr $ \desc_val' ->
    Z.withPrimUnsafe nullPtr $ \desc_del' ->
      query_show_tables
        ldq'
        (MBA# len') (MBA# name_val') (MBA# name_del')
        (MBA# desc_val') (MBA# desc_del')
  finally
    (zip <$> peekStdStringToCBytesN (fromIntegral len) name_val
         <*> peekStdStringToCBytesN (fromIntegral len) desc_val)
    (c_delete_vector_of_string name_del <> c_delete_vector_of_string desc_del)

formatTables :: [(CBytes, CBytes)] -> String
formatTables tables =
  let titles  = ["Table", "Description" :: String]
      width   = 40
      format  = formatRow <$> tables
      formatRow (name, desc) = [[CBytes.unpack name], Table.justifyText width $ CBytes.unpack desc]
      colSpec = [Table.column Table.expand Table.left def def, Table.fixedLeftCol width]
   in Table.tableString $ Table.columnHeaderTableS
        colSpec Table.asciiS
        (Table.titlesH titles)
        (Table.colsAllG Table.center <$> format)

showTableColumns :: QueryBaseRep a => a -> CBytes -> IO String
showTableColumns ldq tableName = do
  formatTableColumns (CBytes.unpack tableName) <$> getTableColumns ldq tableName

getTableColumns :: QueryBaseRep a => a -> CBytes -> IO ([CBytes], [CBytes], [CBytes])
getTableColumns ldq tableName =
  withForeignPtr (eqQueryBaseRep ldq) $ \ldq' ->
  CBytes.withCBytesUnsafe tableName $ \tableName' -> do
    (len, (name_val, (name_del, (type_val, (type_del, (desc_val, (desc_del, _))))))) <-
      Z.withPrimUnsafe (0 :: CSize) $ \len' ->
      Z.withPrimUnsafe nullPtr $ \name_val' ->
      Z.withPrimUnsafe nullPtr $ \name_del' ->
      Z.withPrimUnsafe nullPtr $ \type_val' ->
      Z.withPrimUnsafe nullPtr $ \type_del' ->
      Z.withPrimUnsafe nullPtr $ \desc_val' ->
      Z.withPrimUnsafe nullPtr $ \desc_del' ->
        query_show_table_columns ldq' (BA# tableName') (MBA# len')
                                      (MBA# name_val') (MBA# name_del')
                                      (MBA# type_val') (MBA# type_del')
                                      (MBA# desc_val') (MBA# desc_del')
    finally
      ((,,) <$> peekStdStringToCBytesN (fromIntegral len) name_val
            <*> peekStdStringToCBytesN (fromIntegral len) type_val
            <*> peekStdStringToCBytesN (fromIntegral len) desc_val)
      (c_delete_vector_of_string name_del <> c_delete_vector_of_string type_del
                                          <> c_delete_vector_of_string desc_del)

formatTableColumns :: String -> ([CBytes], [CBytes], [CBytes]) -> String
formatTableColumns tableName ([], [], []) = "Unknown table `" <> tableName <> "`"
formatTableColumns _ (cols, typs, descs) =
  let titles = ["Column", "Type", "Description" :: String]
      width  = 40
      format = zipWith3 (\col typ desc -> [[CBytes.unpack col], [CBytes.unpack typ], Table.justifyText width $ CBytes.unpack desc])
                        cols typs descs
      colSpec = [ Table.column Table.expand Table.left def def
                , Table.column Table.expand Table.left def def
                , Table.fixedLeftCol width
                ]
   in Table.tableString $ Table.columnHeaderTableS
        colSpec Table.asciiS (Table.titlesH titles) (Table.colsAllG Table.center <$> format)

runQuery :: QueryBaseRep a => a -> CBytes -> IO (Either String [String])
runQuery ldq query = second formatQueryResults <$> execStatement ldq query

execStatement :: QueryBaseRep a => a -> CBytes -> IO (Either String [QueryResult])
execStatement ldq query = fetchQueryResults ldq query >>= either (pure . Left) (fmap Right . peekResults)
  where
    peekResults (n, results) =
      if n >= 1
         then forM [0..n-1] $ \i ->
                QueryResult <$> queryResultHeaders results i
                            <*> queryResultRows results i
                            <*> queryResultColsMaxSize results i
                            <*> queryResultMetadata results i
         else return []

formatQueryResults :: [QueryResult] -> [String]
formatQueryResults = map formatter
  where
    formatter result =
      if null (resultRows result)
        then "No records were retrieved."
        else let titles = CBytes.unpack <$> resultHeaders result
                 rows   = fmap CBytes.unpack <$> resultRows result
                 maxSize = fromIntegral . max 40 <$> resultColsMaxSize result
                 colconf = zipWith (,, Table.left) titles maxSize
              in simpleShowTable colconf rows

fetchQueryResults
  :: QueryBaseRep a
  => a -> CBytes -> IO (Either String (Int, QueryResults))
fetchQueryResults ldq query =
  withForeignPtr (eqQueryBaseRep ldq) $ \ldq' ->
  CBytes.withCBytes query $ \query' -> do
    (len, (results_ptr, (exinfo_ptr, _))) <-
      Z.withPrimSafe 0 $ \len' ->
      Z.withPrimSafe nullPtr $ \results' ->
      Z.withPrimSafe nullPtr $ \exinfo' ->
        run_query ldq' query' len' results' exinfo'
    if exinfo_ptr == nullPtr
       then do i <- newForeignPtr delete_query_results_fun results_ptr
               return $ Right (fromIntegral len, i)
       else do desc <- finally (CBytes.fromCString exinfo_ptr) (free exinfo_ptr)
               return $ Left (CBytes.unpack desc)

queryResultHeaders :: QueryResults -> Int -> IO [CBytes]
queryResultHeaders results idx = withForeignPtr results $ \results_ptr -> do
  (len, (val, _)) <-
    Z.withPrimUnsafe 0 $ \len' -> Z.withPrimUnsafe nullPtr $ \val' ->
      queryResults__headers results_ptr idx (MBA# len') (MBA# val')
  peekStdStringToCBytesN len val

queryResultRows :: QueryResults -> Int -> IO [[CBytes]]
queryResultRows results idx = withForeignPtr results $ \results_ptr -> do
  n <- queryResults__rows_len results_ptr idx
  if n >= 1
     then forM [0..n-1] $ \rowIdx -> do
            (len, (val, _)) <-
              Z.withPrimUnsafe 0 $ \len' ->
              Z.withPrimUnsafe nullPtr $ \val' ->
                queryResults__rows_val results_ptr idx (fromIntegral rowIdx) (MBA# len') (MBA# val')
            peekStdStringToCBytesN len val
     else return []

queryResultColsMaxSize :: QueryResults -> Int -> IO [CSize]
queryResultColsMaxSize results idx = withForeignPtr results $ \results_ptr -> do
  (len, (val, _)) <-
    Z.withPrimUnsafe @CSize 0 $ \len' -> Z.withPrimUnsafe nullPtr $ \val' ->
      queryResults__cols_max_size results_ptr idx (MBA# len') (MBA# val')
  peekN (fromIntegral len) val

queryResultMetadata :: QueryResults -> Int -> IO ActiveQueryMetadata
queryResultMetadata results idx = withForeignPtr results $ \results_ptr -> do
  contacted_nodes <- queryResults__metadata_contacted_nodes results_ptr idx
  latency <- queryResults__metadata_latency results_ptr idx
  (len, (key_val, (key_del, (addr_val, (addr_del, (reason_val, (reason_del, _))))))) <-
    Z.withPrimUnsafe @CSize 0 $ \len' ->
    Z.withPrimUnsafe nullPtr $ \key_val' ->
    Z.withPrimUnsafe nullPtr $ \key_del' ->
    Z.withPrimUnsafe nullPtr $ \addr_val' ->
    Z.withPrimUnsafe nullPtr $ \addr_del' ->
    Z.withPrimUnsafe nullPtr $ \reason_val' ->
    Z.withPrimUnsafe nullPtr $ \reason_del' ->
      queryResults__metadata_failures results_ptr idx (MBA# len')
                                      (MBA# key_val') (MBA# key_del')
                                      (MBA# addr_val') (MBA# addr_del')
                                      (MBA# reason_val') (MBA# reason_del')
  failures <- if len >= 1
    then do keys    <- finally (peekN (fromIntegral len) key_val)
                               (c_delete_vector_of_cint key_del)
            addrs   <- finally (peekStdStringToCBytesN (fromIntegral len) addr_val)
                               (c_delete_vector_of_string addr_del)
            reasons <- finally (peekStdStringToCBytesN (fromIntegral len) reason_val)
                               (c_delete_vector_of_string reason_del)
            let details = zipWith FailedNodeDetails addrs reasons
            return $ IntMap.fromList $ zip keys details
    else return IntMap.empty
  return $ ActiveQueryMetadata
    { metadataFailures       = failures
    , metadataContactedNodes = contacted_nodes
    , metadataLatency        = latency
    }

-------------------------------------------------------------------------------

foreign import ccall unsafe "hs_common.h query_show_tables"
  query_show_tables
    :: Ptr C_QueryBase
    -> MBA# CSize
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -> IO ()

foreign import ccall unsafe "hs_common.h query_show_table_columns"
  query_show_table_columns
    :: Ptr C_QueryBase
    -> BA# Word8
    -> MBA# CSize
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -- ^ name
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -- ^ type
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -- ^ description
    -> IO ()

foreign import ccall safe "hs_common.h run_query"
  run_query
    :: Ptr C_QueryBase
    -> Ptr Word8
    -> Ptr CSize -> Ptr (Ptr C_QueryResults)
    -> Ptr CString
    -> IO ()

foreign import ccall unsafe "hs_common.h &delete_query_results"
  delete_query_results_fun :: FunPtr (Ptr C_QueryResults -> IO ())

foreign import ccall unsafe "hs_common.h queryResults__headers"
  queryResults__headers
    :: Ptr C_QueryResults
    -> Int
    -> MBA# CSize
    -> MBA# (Ptr Z.StdString)
    -> IO ()

foreign import ccall unsafe "hs_common.h queryResults__cols_max_size"
  queryResults__cols_max_size
    :: Ptr C_QueryResults
    -> Int
    -> MBA# CSize
    -> MBA# (Ptr CSize)
    -> IO ()

foreign import ccall unsafe "hs_common.h queryResults__rows_len"
  queryResults__rows_len :: Ptr C_QueryResults -> Int -> IO CSize

foreign import ccall unsafe "hs_common.h queryResults__rows_val"
  queryResults__rows_val
    :: Ptr C_QueryResults -> Int -> Int
    -> MBA# CSize
    -> MBA# (Ptr Z.StdString)
    -> IO ()

foreign import ccall unsafe "hs_common.h queryResults__metadata_contacted_nodes"
  queryResults__metadata_contacted_nodes :: Ptr C_QueryResults -> Int -> IO Word64

foreign import ccall unsafe "hs_common.h queryResults__metadata_latency"
  queryResults__metadata_latency :: Ptr C_QueryResults -> Int -> IO Word64

foreign import ccall unsafe "hs_common.h queryResults__metadata_failures"
  queryResults__metadata_failures
    :: Ptr C_QueryResults -> Int
    -> MBA# CSize
    -> MBA# (Ptr CInt) -> MBA# (Ptr (StdVector CInt))
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -> IO ()
