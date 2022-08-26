module HStream.Utils.Compression
  ( decompress
  , compress
  , UnknownCompressionType (..)
  , ZstdCompresstionErr (..)
  )
where

import qualified Codec.Compression.GZip    as GZ
import qualified Codec.Compression.Zstd    as Z
import           Control.Exception         (Exception, displayException, throw)
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Lazy      as BSL
import           HStream.Server.HStreamApi (CompressionType (..))
import           Proto3.Suite              (Enumerated (..))

getCompressionType :: Enumerated CompressionType -> Either String CompressionType
getCompressionType tp = case tp of
  (Enumerated (Right CompressionTypeGzip)) -> Right CompressionTypeGzip
  (Enumerated (Right CompressionTypeNone)) -> Right CompressionTypeNone
  (Enumerated (Right CompressionTypeZstd)) -> Right CompressionTypeZstd
  _                                        -> Left "unknown type"

decompress :: Enumerated CompressionType -> BS.ByteString -> BSL.ByteString
decompress tp payload =
  let compressTp = getCompressionType tp
    in case compressTp of
         Right CompressionTypeGzip -> GZ.decompress . BSL.fromStrict $ payload
         Right CompressionTypeNone -> BSL.fromStrict payload
         Right CompressionTypeZstd -> case Z.decompress payload of
           Z.Skip         -> BSL.empty
           Z.Error e      -> throw $ ZstdCompresstionErr (show e)
           Z.Decompress s -> BSL.fromStrict s
         Left _                    -> throw UnknownCompressionType

compress :: Enumerated CompressionType -> BSL.ByteString -> BS.ByteString
compress tp payload =
  let compressTp = getCompressionType tp
    in case compressTp of
         Right CompressionTypeGzip -> BSL.toStrict $ GZ.compress payload
         Right CompressionTypeNone -> BSL.toStrict payload
         Right CompressionTypeZstd -> Z.compress 1 $ BSL.toStrict payload
         Left _                    -> throw UnknownCompressionType

data UnknownCompressionType = UnknownCompressionType
  deriving(Show)
instance Exception UnknownCompressionType

newtype ZstdCompresstionErr = ZstdCompresstionErr String
  deriving (Show)
instance Exception ZstdCompresstionErr where
  displayException (ZstdCompresstionErr s) = show s
