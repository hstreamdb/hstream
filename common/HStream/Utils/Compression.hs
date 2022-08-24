module HStream.Utils.Compression
  ( decompress
  , compress
  , UnknownCompressionType (..)
  )
where

import qualified Codec.Compression.GZip    as GZ
import           Control.Exception         (Exception, throw)
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Lazy      as BSL
import           HStream.Server.HStreamApi (CompressionType (..))
import           Proto3.Suite              (Enumerated (..))

getCompressionType :: Enumerated CompressionType -> Either String CompressionType
getCompressionType tp = case tp of
  (Enumerated (Right CompressionTypeGzip)) -> Right CompressionTypeGzip
  (Enumerated (Right CompressionTypeNone)) -> Right CompressionTypeNone
  _                                        -> Left "unknown type"

decompress :: Enumerated CompressionType -> BS.ByteString -> BSL.ByteString
decompress tp payload =
  let compressTp = getCompressionType tp
    in case compressTp of
         Right CompressionTypeGzip -> GZ.decompress . BSL.fromStrict $ payload
         Right CompressionTypeNone -> BSL.fromStrict payload
         Left _                    -> throw UnknownCompressionType

compress :: Enumerated CompressionType -> BSL.ByteString -> BS.ByteString
compress tp payload =
  let compressTp = getCompressionType tp
    in case compressTp of
         Right CompressionTypeGzip -> BSL.toStrict $ GZ.compress payload
         Right CompressionTypeNone -> BSL.toStrict payload
         Left _                    -> throw UnknownCompressionType

data UnknownCompressionType = UnknownCompressionType
  deriving(Show)
instance Exception UnknownCompressionType
