module HStream.Kafka.Common.Read
  ( readOneRecord
  , readOneRecordBypassGap
  , readLastOneRecord
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
      let exitErr r = do
            Log.fatal $ "readOneRecord read " <> Log.build logid
                     <> " with lsn (" <> Log.build start <> ", "
                     <> Log.build end <> ") "
                     <> "get unexpected result "
                     <> Log.buildString' r
            ioError $ userError $ "Invalid read result"
      if bypassGap
         then do
           dataRecords <- S.readerRead @ByteString reader 1
           case dataRecords of
             [S.DataRecord{..}] ->
               (Just . (start, end, )) <$> K.runGet recordPayload
             ds -> exitErr ds
         else do
          dataRecords <- S.readerReadAllowGap @ByteString reader 1
          case dataRecords of
            Right [S.DataRecord{..}] ->
              (Just . (start, end, )) <$> K.runGet recordPayload
            ds -> exitErr ds
    release = do
      isReading <- S.readerIsReading reader logid
      when isReading $ S.readerStopReading reader logid

readLastOneRecord
  :: HasCallStack
  => S.LDClient -> S.LDReader -> Word64 -> IO (Maybe (S.LSN, RecordFormat))
readLastOneRecord store reader logid = do
  isEmpty <- S.isLogEmpty store logid
  if isEmpty
     then pure Nothing
     else do tailLsn <- S.getTailLSN store logid
             finally (acquire tailLsn) release
  where
    acquire lsn = do
      -- the log is not empty
      S.readerStartReading reader logid lsn lsn
      dataRecords <- S.readerReadAllowGap @ByteString reader 1
      case dataRecords of
        Right [S.DataRecord{..}] -> (Just . (lsn, )) <$> K.runGet recordPayload
        -- tailsn is a gap
        Left S.GapRecord{..} -> do
          -- FIXME: should we continue if the gap is a dataloss?
          when (gapType == S.GapTypeDataloss) $ do
            Log.fatal $ "readLastOneRecord read " <> Log.build logid
                     <> " with lsn (" <> Log.build lsn <> ", "
                     <> Log.build lsn <> ") "
                     <> "got a dataloss!"
            ioError $ userError $ "Invalid read result"
          Log.info $ "readLastOneRecord read " <> Log.build logid
                  <> " with lsn (" <> Log.build lsn <> ", "
                  <> Log.build lsn <> ") "
                  <> "got a gap " <> Log.buildString' gapType
                  <> ", use last released lsn instead."
          -- This should not be gap, so there won't be a forever loop
          tailAttrLsn <- S.getLogTailAttrsLSN =<< S.getLogTailAttrs store logid
          Log.debug1 $ "Got last released lsn " <> Log.build tailAttrLsn
          acquire tailAttrLsn
        -- should not reach here
        ds -> do
          Log.fatal $ "readLastOneRecord read " <> Log.build logid
                   <> " with lsn (" <> Log.build lsn <> ", "
                   <> Log.build lsn <> ") "
                   <> "got unexpected result "
                   <> Log.buildString' ds
          ioError $ userError $ "Invalid read result"
    release = do
      isReading <- S.readerIsReading reader logid
      when isReading $ S.readerStopReading reader logid
