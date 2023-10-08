module HStream.Kafka.Common.Read
  ( readOneRecord
  ) where

import           Control.Exception
import           Control.Monad
import           Data.ByteString                   (ByteString)
import           Data.Word
import           GHC.Stack                         (HasCallStack)

import           HStream.Kafka.Common.RecordFormat
import qualified HStream.Logger                    as Log
import qualified HStream.Store                     as S
import qualified HStream.Store.Internal.LogDevice  as S
import qualified Kafka.Protocol.Encoding           as K

-- Return the first read RecordFormat
readOneRecord
  :: HasCallStack
  => S.LDClient
  -> S.LDReader
  -> Word64
  -> IO (S.LSN, S.LSN)
  -> IO (Maybe (S.LSN, S.LSN, RecordFormat))
readOneRecord store reader logid getLsn = do
  -- FIXME: This method is blocking until the state can be determined or an
  -- error occurred. Directly read without check isLogEmpty will also block a
  -- while for the first time since the state can be determined.
  isEmpty <- S.isLogEmpty store logid
  if isEmpty
     then pure Nothing
     else do (start, end) <- getLsn
             finally (acquire start end) release
  where
    acquire start end = do
      S.readerStartReading reader logid start end
      dataRecords <- S.readerReadAllowGap @ByteString reader 1
      case dataRecords of
        Right [S.DataRecord{..}] -> (Just . (start, end, )) <$> K.runGet recordPayload
        _ -> do Log.fatal $ "readOneRecord read " <> Log.build logid
                         <> "with lsn (" <> Log.build start <> " "
                         <> Log.build end <> ") "
                         <> "get unexpected result "
                         <> Log.buildString' dataRecords
                ioError $ userError $ "Invalid reader result " <> show dataRecords
    release = do
      isReading <- S.readerIsReading reader logid
      when isReading $ S.readerStopReading reader logid
