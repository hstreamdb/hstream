module HStream.Store.Admin.Command.Logs
  ( runLogsCmd
  ) where
import           Data.Bits                        (shiftR, (.&.))
import qualified Data.Map.Strict                  as Map
import           System.IO.Unsafe                 (unsafePerformIO)
import           Z.IO.Time                        (SystemTime (..),
                                                   formatSystemTime,
                                                   simpleDateFormat)

import qualified HStream.Store                    as S
import           HStream.Store.Admin.API
import           HStream.Store.Admin.Types
import qualified HStream.Store.Internal.LogDevice as S

runLogsCmd :: HeaderConfig AdminAPI -> LogsConfigCmd -> IO ()
runLogsCmd conf (InfoCmd logid) = runLogsInfo conf logid
runLogsCmd conf ShowCmd         = error "not implemented yet"

runLogsInfo :: HeaderConfig AdminAPI -> S.C_LogID -> IO ()
runLogsInfo conf logid = do
  let client' = buildLDClientRes conf Map.empty
  withResource client' $ \client -> do
    putStrLn "Tail Info:"
    lsn <- S.getTailLSN client logid
    putStrLn $ "  Tail lsn: " ++ prettyPrintLSN lsn
    tail_attr <- S.getLogTailAttrs client logid
    tail_lsn <- S.getLogTailAttrsLSN tail_attr
    putStrLn $ "  Last released real (record) lsn: "
      ++ prettyPrintLSN tail_lsn
    tail_time_stamp <- S.getLogTailAttrsLastTimeStamp tail_attr
    putStrLn $ "  Approximate timestamp of last released real lsn: "
      ++ prettyPrintTimeStamp tail_time_stamp
    tail_offset <- S.getLogTailAttrsBytesOffset tail_attr
    putStrLn $ "  Approximate byte offset of the tail of the log: "
      ++ show tail_offset

    putStrLn "Head Info:"
    head_attr <- S.getLogHeadAttrs client logid
    head_lsn <- S.getLogHeadAttrsTrimPoint head_attr
    putStrLn $ "  Trim point: " ++ prettyPrintLSN head_lsn
    head_time_stamp <- S.getLogHeadAttrsTrimPointTimestamp head_attr
    putStrLn $ "  Approximate timestamp of trim point (ms): "
      ++ prettyPrintTimeStamp head_time_stamp
  where
    prettyPrintLSN lsn =
      "e" ++ show (lsn `shiftR` 32) ++ "n" ++ show (lsn .&. 0xFFFFFFFF)
    prettyPrintTimeStamp ts = unsafePerformIO $ do
      time <- formatSystemTime simpleDateFormat (MkSystemTime (ts `div` 1000) 0)
      return $ show ts ++ " -> " ++ show time
