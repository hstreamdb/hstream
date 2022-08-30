{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeApplications  #-}

module HStream.CodecSpec (spec) where
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Char8     as BSC
import qualified Data.ByteString.Lazy      as BSL
import qualified Data.Map.Strict           as M
import qualified Data.Text                 as T
import qualified Data.Vector               as V
import           HStream.Server.HStreamApi
import           HStream.Utils             (EncoderType (ProtoEncoder),
                                            compress, decompress,
                                            decompressBatchedRecord,
                                            deserializeHStreamRecords,
                                            mkBatchedRecord,
                                            serializeHStreamRecords)
import           Proto3.Suite
import           Test.Hspec
import           Test.Hspec.QuickCheck     (prop)
import           Test.QuickCheck           (Arbitrary (..), Gen, chooseEnum,
                                            elements, generate, vectorOf)

genChar :: Gen Char
genChar = elements ['a'..'z']

instance Arbitrary HStreamRecordHeader where
  arbitrary = do
    flag <- Enumerated . Right <$> elements [HStreamRecordHeader_FlagJSON, HStreamRecordHeader_FlagRAW]
    key <- T.pack <$> vectorOf 5 genChar
    return $ HStreamRecordHeader flag M.empty key

instance Arbitrary HStreamRecord where
  arbitrary = do
    header <- arbitrary
    HStreamRecord (Just header) <$> genRandomPayload

genBatchedRecord :: V.Vector HStreamRecord -> Gen BatchedRecord
genBatchedRecord payloads = do
  tp <- genCompressionType
  return $ mkBatchedRecord tp Nothing (fromIntegral $ V.length payloads) payloads

genCompressionType :: Gen (Enumerated CompressionType)
genCompressionType = elements [ Enumerated (Right CompressionTypeGzip)
                              , Enumerated (Right CompressionTypeNone)
                              , Enumerated (Right CompressionTypeZstd)
                              ]

genRandomPayload :: Gen BS.ByteString
genRandomPayload = do
  size <- chooseEnum (100, 4096)
  BSC.pack <$> vectorOf size genChar

spec :: SpecWith ()
spec = parallel $ do
  codecSpec
  compressSpec
  codecBatchRecordSpec

codecSpec :: SpecWith ()
codecSpec = describe "Codec spec" $ do
  prop "test HStreamRecords codec" $ do
    \(records :: V.Vector HStreamRecord) -> do
      deserializeHStreamRecords @'ProtoEncoder (serializeHStreamRecords @'ProtoEncoder records) `shouldBe` records

compressSpec :: SpecWith ()
compressSpec = describe "Compress spec" $ do
  prop "test compression and decompression" $ do
    \(payload :: BS.ByteString) -> do
      compressTp <- generate genCompressionType
      decompress compressTp (compress compressTp $ BSL.fromStrict payload) `shouldBe` BSL.fromStrict payload

codecBatchRecordSpec :: SpecWith ()
codecBatchRecordSpec = describe "BatchRecord codec spec" $
  prop "test BatchRecord codec" $ do
    \(payloads :: V.Vector HStreamRecord) -> do
      batchRecord <- generate $ genBatchedRecord payloads
      decompressBatchedRecord batchRecord `shouldBe` payloads
