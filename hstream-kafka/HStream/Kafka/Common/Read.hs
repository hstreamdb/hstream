module HStream.Kafka.Common.Read
  ( readOneRecord
  , readOneRecordBypassGap
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

readOneRecord
  :: HasCallStack
  => S.LDClient
  -> S.LDReader
  -> Word64
  -> IO (S.LSN, S.LSN)
  -> IO (Maybe (S.LSN, S.LSN, RecordFormat))
readOneRecord = readOneRecord_ False

readOneRecordBypassGap
  :: HasCallStack
  => S.LDClient
  -> S.LDReader
  -> Word64
  -> IO (S.LSN, S.LSN)
  -> IO (Maybe (S.LSN, S.LSN, RecordFormat))
readOneRecordBypassGap = readOneRecord_ True

-- Return the first read RecordFormat
readOneRecord_
  :: HasCallStack
  => Bool
  -> S.LDClient
  -> S.LDReader
  -> Word64
  -> IO (S.LSN, S.LSN)
  -> IO (Maybe (S.LSN, S.LSN, RecordFormat))
readOneRecord_ bypassGap store reader logid getLsn = do
  -- FIXME: This method is blocking until the state can be determined or an
  -- error occurred. Directly read without check isLogEmpty will also block a
  -- while for the first time since the state can be determined.
  isEmpty <- S.isLogEmpty store logid
  if isEmpty
     then pure Nothing
     else do (start, end) <- getLsn
             if start == S.LSN_INVALID || end == S.LSN_INVALID
                then pure Nothing
                else finally (acquire start end) release
  where
    acquire start end = do
      -- the log is not empty
      S.readerStartReading reader logid start end
      if bypassGap
         then do
           [S.DataRecord{..}] <- S.readerRead @ByteString reader 1
           (Just . (start, end, )) <$> K.runGet recordPayload
         else do
          dataRecords <- S.readerReadAllowGap @ByteString reader 1
          case dataRecords of
            Right [S.DataRecord{..}] ->
              (Just . (start, end, )) <$> K.runGet recordPayload
            Right [] -> do
              Log.fatal "readOneRecord got an empty results!"
              error "Invalid reader result"
            Right da -> do
              Log.fatal $ "readOneRecord: unexpected happened, "
                       <> "got " <> Log.build (length da) <> " records"
              error "Invalid reader result"
            _ -> do Log.fatal $ "readOneRecord read " <> Log.build logid
                             <> " with lsn (" <> Log.build start <> ", "
                             <> Log.build end <> ") "
                             <> "get unexpected result "
                             <> Log.buildString' dataRecords
                    ioError $ userError $ "Invalid reader result"
    release = do
      isReading <- S.readerIsReading reader logid
      when isReading $ S.readerStopReading reader logid
