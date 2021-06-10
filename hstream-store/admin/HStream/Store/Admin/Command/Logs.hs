{-# LANGUAGE OverloadedStrings #-}
module HStream.Store.Admin.Command.Logs
  ( runLogsCmd
  ) where
import           Control.Monad                    (forM_, guard, unless, when)
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
runLogsCmd conf (ShowCmd showOpt)        = runLogsShow conf showOpt

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

runLogsShow :: HeaderConfig AdminAPI -> ShowLogsOpts -> IO ()
runLogsShow conf ShowLogsOpts{..} = do
  let client' = buildLDClientRes conf Map.empty
  withResource client' $ \client -> do
    let printLogDir = printLogDirectory client showMaxDepth showVerbose
    case (showLogID, showPath) of
      -- if the log ID is present, the path is ignored
      (Just logid, _) -> S.getLogGroupByID client logid >>= printLogGroup showVerbose
      -- otherwise, look for a log group or directory according to the path
      (_, Just path)  -> do
        tryJust (guard . S.isNotFound) (S.getLogGroup client path) >>=
          (\case
              Right loggroup -> printLogGroup showVerbose loggroup
              Left _         -> S.getLogDirectory client path >>= printLogDir)
      -- when both are missing, print the full tree
      _               -> S.getLogDirectory client "/" >>= printLogDir

shift :: Int
shift = 2

printLogGroup :: Bool -> S.LDLogGroup -> IO ()
printLogGroup = printLogGroup' 0

printLogGroup' :: Int -> Bool -> S.LDLogGroup -> IO ()
printLogGroup' level verbose logGroup = do
  let emit s = putStrLn $ replicate (level * shift) ' ' <> s
  fullName <- S.logGroupGetFullName logGroup
  (lo, hi) <- S.logGroupGetRange logGroup
  emit $ "* " <> unpack fullName <> " (" <> show lo <> ".." <> show hi <> ")"
  when verbose $ do
    version <- S.logGroupGetVersion logGroup
    emit $ "  version: " <> show version

printLogDirectory :: S.LDClient -> Int -> Bool -> S.LDDirectory -> IO ()
printLogDirectory = flip printLogDirectory' 0

printLogDirectory' :: S.LDClient -> Int -> Int -> Bool -> S.LDDirectory -> IO ()
printLogDirectory' client level maxDepth verbose logDirectory = do
  let emit s = putStrLn $ replicate (level * shift) ' ' <> s
  fullName <- S.logDirectoryGetFullName logDirectory
  emit $ "- " <> unpack fullName
  when verbose $ do
    version <- S.logDirectoryGetVersion logDirectory
    emit $ "  version: " <> show version
  when (level < maxDepth) $ do
    logGroupNames <- S.logDirLogsNames logDirectory
    forM_ logGroupNames $ \logGroupName ->
      S.logDirLogFullName logDirectory logGroupName >>= S.getLogGroup client >>=
      printLogGroup' (level + 1) verbose
    subDirNames <- S.logDirChildrenNames logDirectory
    forM_ subDirNames $ \subDirName ->
      S.logDirChildFullName logDirectory subDirName >>= S.getLogDirectory client >>=
      printLogDirectory' client (level + 1) maxDepth verbose
