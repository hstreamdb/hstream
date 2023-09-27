{-# LANGUAGE MagicHash #-}

module HStream.Store.Internal.LogDevice.LDQuery where

import           Control.Exception
import           Control.Monad           (forM)
import           Data.Int
import           Data.IntMap.Strict      (IntMap)
import qualified Data.IntMap.Strict      as IntMap
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Marshal.Alloc   (free)
import           Foreign.Ptr
import           GHC.Stack
import qualified Z.Data.CBytes           as CBytes
import           Z.Data.CBytes           (CBytes)
import qualified Z.Foreign               as Z

import           HStream.Foreign
import           HStream.Store.Exception (throwStoreError)

data C_LDQuery
data C_QueryResults
type LDQuery = ForeignPtr C_LDQuery
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

newLDQuery
  :: CBytes
  -- ^ Path to the LD tier's config.
  -> Int64
  -- ^ Timeout when retrieve data from a LD node through its admin command port,
  -- milliseconds
  -> Bool
  -- ^ Indicates that ldquery should connect to admin command port using SSL/TLS
  -> IO LDQuery
newLDQuery config timeout use_ssl = CBytes.withCBytes config $ \config' -> do
  ldq_ptr <- new_ldquery config' timeout use_ssl
  newForeignPtr delete_ldquery_fun ldq_ptr

showTables :: LDQuery -> IO [(CBytes, CBytes)]
showTables ldq = withForeignPtr ldq $ \ldq' -> do
  (len, (name_val, (name_del, (desc_val, (desc_del, _))))) <-
    Z.withPrimUnsafe (0 :: CSize) $ \len' ->
    Z.withPrimUnsafe nullPtr $ \name_val' ->
    Z.withPrimUnsafe nullPtr $ \name_del' ->
    Z.withPrimUnsafe nullPtr $ \desc_val' ->
    Z.withPrimUnsafe nullPtr $ \desc_del' ->
      ldquery_show_tables ldq' (MBA# len') (MBA# name_val') (MBA# name_del')
                          (MBA# desc_val') (MBA# desc_del')
  finally
    (zip <$> peekStdStringToCBytesN (fromIntegral len) name_val
         <*> peekStdStringToCBytesN (fromIntegral len) desc_val)
    (c_delete_vector_of_string name_del <> c_delete_vector_of_string desc_del)

showTableColumns :: LDQuery -> CBytes -> IO ([CBytes], [CBytes], [CBytes])
showTableColumns ldq tableName =
  withForeignPtr ldq $ \ldq' ->
  CBytes.withCBytesUnsafe tableName $ \tableName' -> do
    (len, (name_val, (name_del, (type_val, (type_del, (desc_val, (desc_del, _))))))) <-
      Z.withPrimUnsafe (0 :: CSize) $ \len' ->
      Z.withPrimUnsafe nullPtr $ \name_val' ->
      Z.withPrimUnsafe nullPtr $ \name_del' ->
      Z.withPrimUnsafe nullPtr $ \type_val' ->
      Z.withPrimUnsafe nullPtr $ \type_del' ->
      Z.withPrimUnsafe nullPtr $ \desc_val' ->
      Z.withPrimUnsafe nullPtr $ \desc_del' ->
        ldquery_show_table_columns ldq' (BA# tableName') (MBA# len')
                                   (MBA# name_val') (MBA# name_del')
                                   (MBA# type_val') (MBA# type_del')
                                   (MBA# desc_val') (MBA# desc_del')
    finally
      ((,,) <$> peekStdStringToCBytesN (fromIntegral len) name_val
            <*> peekStdStringToCBytesN (fromIntegral len) type_val
            <*> peekStdStringToCBytesN (fromIntegral len) desc_val)
      (c_delete_vector_of_string name_del <> c_delete_vector_of_string type_del
                                          <> c_delete_vector_of_string desc_del)

runQuery :: LDQuery -> CBytes -> IO [QueryResult]
runQuery ldq query = do
  (n, results) <- fetchQueryResults ldq query
  if n >= 1
     then forM [0..n-1] $ \i ->
            QueryResult <$> queryResultHeaders results i
                        <*> queryResultRows results i
                        <*> queryResultColsMaxSize results i
                        <*> queryResultMetadata results i
     else return []

fetchQueryResults :: HasCallStack => LDQuery -> CBytes -> IO (Int, QueryResults)
fetchQueryResults ldq query =
  withForeignPtr ldq $ \ldq' ->
  CBytes.withCBytes query $ \query' -> do
    (len, (results_ptr, (exinfo_ptr, _))) <-
      Z.withPrimSafe 0 $ \len' ->
      Z.withPrimSafe nullPtr $ \results' ->
      Z.withPrimSafe nullPtr $ \exinfo' ->
        ldquery_query ldq' query' len' results' exinfo'
    if exinfo_ptr == nullPtr
       then do i <- newForeignPtr delete_query_results_fun results_ptr
               return (fromIntegral len, i)
       else do desc <- finally (CBytes.fromCString exinfo_ptr) (free exinfo_ptr)
               throwStoreError (CBytes.toText desc) callStack

queryResultHeaders :: QueryResults -> Int -> IO [CBytes]
queryResultHeaders results idx = withForeignPtr results $ \results_ptr -> do
  (len, (val, _)) <-
    Z.withPrimUnsafe 0 $ \len' -> Z.withPrimUnsafe nullPtr $ \val' ->
      queryResults__headers results_ptr idx (MBA# len') (MBA# val')
  peekStdStringToCBytesN len val

queryResultRows :: QueryResults -> Int -> IO [[CBytes]]
queryResultRows results idx = withForeignPtr results $ \results_ptr -> do
  n <- queryResults_rows_len results_ptr idx
  if n >= 1
     then forM [0..n-1] $ \rowIdx -> do
            (len, (val, _)) <-
              Z.withPrimUnsafe 0 $ \len' ->
              Z.withPrimUnsafe nullPtr $ \val' ->
                queryResults_rows_val results_ptr idx (fromIntegral rowIdx) (MBA# len') (MBA# val')
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

foreign import ccall safe "hs_logdevice.h new_ldquery"
  new_ldquery :: Ptr Word8 -> Int64 -> Bool -> IO (Ptr C_LDQuery)

foreign import ccall unsafe "hs_logdevice.h delete_ldquery"
  delete_ldquery :: Ptr C_LDQuery -> IO ()

foreign import ccall unsafe "hs_logdevice.h &delete_ldquery"
  delete_ldquery_fun :: FunPtr (Ptr C_LDQuery -> IO ())

foreign import ccall unsafe "hs_logdevice.h ldquery_show_tables"
  ldquery_show_tables
    :: Ptr C_LDQuery
    -> MBA# CSize
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h ldquery_show_table_columns"
  ldquery_show_table_columns
    :: Ptr C_LDQuery
    -> BA# Word8
    -> MBA# CSize
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -- ^ name
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -- ^ type
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -- ^ description
    -> IO ()

foreign import ccall safe "hs_logdevice.h ldquery_query"
  ldquery_query
    :: Ptr C_LDQuery
    -> Ptr Word8
    -> Ptr CSize -> Ptr (Ptr C_QueryResults)
    -> Ptr CString
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h delete_query_results_"
  delete_query_results :: Ptr C_QueryResults -> IO ()

foreign import ccall unsafe "hs_logdevice.h &delete_query_results_"
  delete_query_results_fun :: FunPtr (Ptr C_QueryResults -> IO ())

foreign import ccall unsafe "hs_logdevice.h queryResults__headers_"
  queryResults__headers
    :: Ptr C_QueryResults
    -> Int
    -> MBA# CSize
    -> MBA# (Ptr Z.StdString)
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h queryResults__cols_max_size_"
  queryResults__cols_max_size
    :: Ptr C_QueryResults
    -> Int
    -> MBA# CSize
    -> MBA# (Ptr CSize)
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h queryResults_rows_len"
  queryResults_rows_len :: Ptr C_QueryResults -> Int -> IO CSize

foreign import ccall unsafe "hs_logdevice.h queryResults_rows_val"
  queryResults_rows_val
    :: Ptr C_QueryResults -> Int -> Int
    -> MBA# CSize
    -> MBA# (Ptr Z.StdString)
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h queryResults__metadata_contacted_nodes_"
  queryResults__metadata_contacted_nodes :: Ptr C_QueryResults -> Int -> IO Word64

foreign import ccall unsafe "hs_logdevice.h queryResults__metadata_latency_"
  queryResults__metadata_latency :: Ptr C_QueryResults -> Int -> IO Word64

foreign import ccall unsafe "hs_logdevice.h queryResults__metadata_failures_"
  queryResults__metadata_failures
    :: Ptr C_QueryResults -> Int
    -> MBA# CSize
    -> MBA# (Ptr CInt) -> MBA# (Ptr (StdVector CInt))
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -> MBA# (Ptr Z.StdString) -> MBA# (Ptr (StdVector Z.StdString))
    -> IO ()
