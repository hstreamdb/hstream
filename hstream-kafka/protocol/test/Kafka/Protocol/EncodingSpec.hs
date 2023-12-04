{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Protocol.EncodingSpec (spec) where

import           Control.Exception
import           Data.ByteString            (ByteString)
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Lazy       as BL
import           Data.Int
import           Data.Text                  (Text)
import           Data.Word
import           GHC.Generics
import           Test.Hspec
import           Test.Hspec.QuickCheck
import           Test.QuickCheck
import           Test.QuickCheck.Instances  ()
import           Test.QuickCheck.Special

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Message
import           Kafka.QuickCheck.Instances ()

spec :: Spec
spec = do
  baseSpec
  genericSpec
  realSpec

encodingProp :: (Eq a, Show a, Serializable a) => a -> Property
encodingProp x = ioProperty $ (x ===) <$> runGet (runPut x)

-------------------------------------------------------------------------------

-- We need quickcheck-special to generate edge cases
-- See: https://github.com/nick8325/quickcheck/issues/98

baseSpec :: Spec
baseSpec = describe "Kafka.Protocol.Encoding" $ do
  prop "Bool" $ \(x :: Bool) -> encodingProp x
  prop "Int8" $ \(Special @Int8 x) -> encodingProp x
  prop "Int16" $ \(Special @Int16 x) -> encodingProp x
  prop "Int32" $ \(Special @Int32 x) -> encodingProp x
  prop "Int64" $ \(Special @Int64 x) -> encodingProp x
  prop "Word32" $ \(Special @Word32 x) -> encodingProp x
  prop "Double" $ \(Special @Double x) ->
    if isNaN x then ioProperty $ isNaN @Double <$> runGet (runPut x)
               else encodingProp x
  prop "NullableString" $ \(Special @NullableString x) -> encodingProp x
  prop "NullableBytes" $ \(Special @NullableBytes x) -> encodingProp x
  prop "Text" $ \(Special @Text x) -> encodingProp x
  prop "ByteString" $ \(Special @ByteString x) -> encodingProp x
  prop "VarInt32" $ \(Special @VarInt32 x) -> encodingProp x
  prop "VarInt64" $ \(Special @VarInt64 x) -> encodingProp x
  prop "CompactString" $ \(Special @CompactString x) -> encodingProp x
  prop "CompactNullableString" $ \(Special @CompactNullableString x) -> encodingProp x
  prop "CompactBytes" $ \(Special @CompactBytes x) -> encodingProp x
  prop "CompactNullableBytes" $ \(Special @CompactNullableBytes x) -> encodingProp x
  -- KaArray
  prop "KaArray Bool" $ \(x :: KaArray Bool) -> encodingProp x
  prop "KaArray Int8" $ \(x :: KaArray Int8) -> encodingProp x
  prop "KaArray NullableBytes" $ \(x :: KaArray NullableBytes) -> encodingProp x
  prop "KaArray NullableString" $ \(x :: KaArray NullableString) -> encodingProp x
  prop "KaArray VarInt32" $ \(x :: KaArray VarInt32) -> encodingProp x
  prop "KaArray CompactBytes" $ \(x :: KaArray CompactBytes) -> encodingProp x
  prop "KaArray KaArray CompactBytes" $ \(x :: KaArray (KaArray CompactBytes)) -> encodingProp x
  -- CompactKaArray
  prop "CompactKaArray Bool" $ \(x :: CompactKaArray Bool) -> encodingProp x
  prop "CompactKaArray Int8" $ \(x :: CompactKaArray Int8) -> encodingProp x
  prop "CompactKaArray NullableBytes" $ \(x :: CompactKaArray NullableBytes) -> encodingProp x
  prop "CompactKaArray NullableString" $ \(x :: CompactKaArray NullableString) -> encodingProp x
  prop "CompactKaArray VarInt32" $ \(x :: CompactKaArray VarInt32) -> encodingProp x
  prop "CompactKaArray CompactBytes" $ \(x :: CompactKaArray CompactBytes) -> encodingProp x
  prop "CompactKaArray KaArray CompactBytes" $ \(x :: CompactKaArray (KaArray CompactBytes)) -> encodingProp x

-------------------------------------------------------------------------------

data SomeMsg = SomeMsg
  { msgA :: Int32
  , msgB :: NullableString
  , msgC :: VarInt32
  , msgD :: KaArray Int32
  , msgE :: KaArray (KaArray CompactBytes)
  } deriving (Show, Eq, Generic)

instance Serializable SomeMsg

putSomeMsg :: SomeMsg -> ByteString
putSomeMsg SomeMsg{..} =
  let msg = put msgA <> put msgB <> put msgC <> put msgD <> put msgE
   in BL.toStrict $ toLazyByteString msg

getSomeMsg :: ByteString -> IO SomeMsg
getSomeMsg bs =
  let p = SomeMsg <$> get <*> get <*> get <*> get <*> get
   in do result <- runParser p bs
         case result of
           Done "" r  -> pure r
           Done l _   -> throwIO $ DecodeError $ "Done, but left " <> show l
           Fail _ err -> throwIO $ DecodeError $ "Fail, " <> err
           More _     -> throwIO $ DecodeError "Need more"

genericSpec :: Spec
genericSpec = describe "Kafka.Protocol.Encoding" $ do
  it "Generic instance" $
    let someMsg = SomeMsg 10 (Just "x") 10
                          (KaArray $ Just [1, 1])
                          (KaArray $ Just [KaArray $ Just ["x"]])
     in do runGet (runPut someMsg) `shouldReturn` someMsg
           runPut someMsg `shouldBe` putSomeMsg someMsg
           getSomeMsg (runPut someMsg) `shouldReturn` someMsg
           runGet (putSomeMsg someMsg) `shouldReturn` someMsg

-------------------------------------------------------------------------------

-- Real world tests
realSpec :: Spec
realSpec = describe "Kafka.Protocol.Encoding" $ do
  it "From real world kafka-python: request header v1" $ do
    let clientReqBs = "\NUL\NUL\NUL \NUL\DC2\NUL\NUL\NUL\NUL\NUL\SOH\NUL\SYN"
                   <> "kafka-python-2.0.3-dev"
    let reqHeader = RequestHeader (ApiKey 18) 0 1
                                  (Just $ Just "kafka-python-2.0.3-dev")
                                  Nothing
        reqBody = ApiVersionsRequestV0
        reqHeaderBs = runPut reqHeader
        reqBodyBs = runPut reqBody
        reqBs = reqHeaderBs <> reqBodyBs
        reqLen = fromIntegral (BS.length reqBs) :: Int32
    (runPut reqLen <> reqBs) `shouldBe` clientReqBs
    runGet clientReqBs `shouldReturn` (reqLen, reqHeader, reqBody)

  it "From real world confluent-kafka-python: request header v2" $ do
    let clientReqBs = "\NUL\NUL\NULM\NUL\DC2\NUL\ETX\NUL\NUL\NUL\SOH\NUL\SYN"
                   <> "confluent_kafka_client\NUL\ETBconfluent-kafka-python\DC4"
                   <> "2.2.0-rdkafka-2.2.0\NUL"
    let reqHeader = RequestHeader
                      (ApiKey 18) 3 1
                      (Just $ Just "confluent_kafka_client")
                      (Just EmptyTaggedFields)
        reqBody = ApiVersionsRequestV3
                    (CompactString "confluent-kafka-python")
                    (CompactString "2.2.0-rdkafka-2.2.0")
                    EmptyTaggedFields
        reqHeaderBs = runPut reqHeader
        reqBodyBs = runPut reqBody
        reqBs = reqHeaderBs <> reqBodyBs
        reqLen = fromIntegral (BS.length reqBs) :: Int32

    (runPut reqLen <> reqBs) `shouldBe` clientReqBs
    runGet clientReqBs `shouldReturn` (reqLen, reqHeader, reqBody)

  it "Message Format V2 (Record Batch)" $ do
    let clientReqBs =
            "\NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL\NUL\139\NUL\NUL\NUL\NUL"
         <> "\STX\164\239\196*\NUL\NUL\NUL\NUL\NUL\SOH\NUL\NUL\SOH\140\&3~\176"
         <> "\DLE\NUL\NUL\SOH\140\&3~\176\DLE\255\255\255\255\255\255\255\255"
         <> "\255\255\255\255\255\255\NUL\NUL\NUL\STXX\NUL\NUL\NUL\SOH\FS"
         <> "some_message_0\EOT\SOheader1\ACKfoo\SOheader2\ACKbarX\NUL\NUL\STX"
         <> "\SOH\FSsome_message_1\EOT\SOheader1\ACKfoo\SOheader2\ACKbar"
        reqRecords = KaArray $ Just $
          [RecordV2{ length = 44
                   , attributes = 0
                   , timestampDelta = 0
                   , offsetDelta = 0
                   , key = RecordKey Nothing
                   , value = RecordValue $ Just "some_message_0"
                   , headers = RecordArray $
                       [ ("header1", RecordHeaderValue $ Just "foo")
                       , ("header2", RecordHeaderValue $ Just "bar")
                       ]
                   }
          , RecordV2{ length = 44
                    , attributes = 0
                    , timestampDelta = 0
                    , offsetDelta = 1
                    , key = RecordKey Nothing
                    , value = RecordValue $ Just "some_message_1"
                    , headers = RecordArray $
                        [ ("header1", RecordHeaderValue $ Just "foo")
                        , ("header2", RecordHeaderValue $ Just "bar")
                        ]
                    }
          ]
        reqRecordBatch = RecordBatch
          { baseOffset = 0
          , batchLength = 139
          , partitionLeaderEpoch = 0
          , magic = 2
          , crc = -1527790550
          , attributes = 0
          , lastOffsetDelta = 1
          , baseTimestamp = 1701670989840
          , maxTimestamp = 1701670989840
          , producerId = -1
          , producerEpoch = -1
          , baseSequence = -1
          , records = reqRecords
          }
        clientReq = [BatchRecordV2 reqRecordBatch]
    decodeBatchRecords True clientReqBs `shouldReturn` clientReq
    encodeBatchRecords clientReq `shouldBe` clientReqBs
