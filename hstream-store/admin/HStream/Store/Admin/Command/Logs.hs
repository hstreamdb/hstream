{-# LANGUAGE OverloadedStrings #-}
module HStream.Store.Admin.Command.Logs
  ( runLogsCmd
  ) where
import           Control.Monad                    (guard, unless, when)
import           Data.Bits                        (shiftR, (.&.))
import           Data.Char                        (toUpper)
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust, isNothing)
import           System.IO.Unsafe                 (unsafePerformIO)
import           Z.Data.CBytes                    (CBytes, unpack)
import           Z.IO.Exception                   (tryJust)
import           Z.IO.Time                        (SystemTime (..),
                                                   formatSystemTime,
                                                   simpleDateFormat)

import qualified HStream.Store                    as S
import           HStream.Store.Admin.API
import           HStream.Store.Admin.Types
import qualified HStream.Store.Internal.LogDevice as S


runLogsCmd :: HeaderConfig AdminAPI -> LogsConfigCmd -> IO ()
runLogsCmd conf (InfoCmd logid)          = runLogsInfo conf logid
runLogsCmd conf (CreateCmd createOpt)    = createLogs conf createOpt
runLogsCmd conf (RenameCmd old new warn) = runLogsRename conf old new warn
runLogsCmd conf (RemoveCmd removeOpt)    = runLogsRemove conf removeOpt
runLogsCmd conf ShowCmd                  = error "not implemented yet"

runLogsRemove :: HeaderConfig AdminAPI -> RemoveLogsOpts -> IO ()
runLogsRemove conf RemoveLogsOpts{..} = do
  putStrLn $ "Are you sure you want to rename " <> show rmPath <> "? [y/n]"
  c <- getChar
  when (toUpper c == 'Y') $ do
    let client' = buildLDClientRes conf Map.empty
    withResource client' $ \client -> do
      res <- tryJust (guard . S.isNotFound) $ S.removeLogDirectory client rmPath rmRecursive
      case res of
        Right version ->
          putStrLn $ "directory " <> show rmPath <> " has been removed in version " <> show version
        Left _ -> do
          version <- S.removeLogGroup client rmPath
          putStrLn $ "log group " <> show rmPath <> " has been removed in version " <> show version

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

runLogsRename :: HeaderConfig AdminAPI -> CBytes -> CBytes -> Bool -> IO ()
runLogsRename conf old new warn = do
  c <- if warn then putStrLn "Are you sure you want to rename? [y/n]" >> getChar else return 'Y'
  when (toUpper c == 'Y') $ do
    unless (comparePath old new) $ error "You can only rename node, but not move node"
    let client' = buildLDClientRes conf Map.empty
    withResource client' $ \client -> do
      version <- S.renameLogGroup client old new
      putStr . unpack $ "Path \'" <> old <> "\' has been renamed to \'" <> new <> "\' in version "
      print version
  where
    comparePath x y = getPath x == getPath y
    getPath x = if '/' `elem` unpack x then dropWhile (/= '/') . reverse . unpack $ x else ""

createLogs :: HeaderConfig AdminAPI -> CreateLogsOpts -> IO ()
createLogs conf CreateLogsOpts{..} = do
  when (isDirectory && (isJust fromId || isJust toId)) $ errorWithoutStackTrace
    "Cannot create a directory and set from/to log range at same time."
  when (not isDirectory && (isNothing fromId || isNothing toId)) $ errorWithoutStackTrace $
    "If you are trying to create log-groups, use --from and --to, " <>
    "otherwise use --directory to create a directory."

  withResource (buildLDClientRes conf Map.empty) $ \client -> do
    let attrs = S.LogAttrs logsAttributes
    if isDirectory
       then S.makeLogDirectory client path attrs True >> putStrLn "Create log directory successfully!" >> return ()
       else let start = fromJust fromId
                end = fromJust toId
             in S.makeLogGroup client path start end attrs True >> putStrLn "Create log group successfully!" >> return ()
