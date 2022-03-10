{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}
{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
module HStream.Admin.Store.Command.Logs
  ( runLogsCmd
  ) where

import           Colourista                       (formatWith, green, red,
                                                   yellow)
import           Control.Monad                    (forM_, guard, unless, when,
                                                   (>=>))
import           Data.Bits                        (shiftR, (.&.))
import           Data.Char                        (toUpper)
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, isJust, isNothing)
import           Foreign.ForeignPtr               (withForeignPtr)
import           System.IO                        (hFlush, stdout)
import           System.IO.Unsafe                 (unsafePerformIO)
import qualified Text.Casing                      as Casting
import           Z.Data.CBytes                    (CBytes, unpack)
import           Z.IO.Exception                   (try, tryJust)
import           Z.IO.Time                        (SystemTime (..),
                                                   formatSystemTime,
                                                   simpleDateFormat)

import           HStream.Admin.Store.API
import           HStream.Admin.Store.Types
import qualified HStream.Store                    as S
import           HStream.Store.Internal.LogDevice (LogAttributes (..))
import qualified HStream.Store.Internal.LogDevice as S

runLogsCmd :: HeaderConfig AdminAPI -> LogsConfigCmd -> IO ()
runLogsCmd conf (InfoCmd logid)           = runLogsInfo conf logid
runLogsCmd conf (CreateCmd createOpt)     = createLogs conf createOpt
runLogsCmd conf (RenameCmd old new warn)  = runLogsRename conf old new warn
runLogsCmd conf (RemoveCmd removeOpt)     = runLogsRemove conf removeOpt
runLogsCmd conf (ShowCmd showOpt)         = runLogsShow conf showOpt
runLogsCmd conf (SetRangeCmd setRangeOpt) = runLogsSetRange conf setRangeOpt
runLogsCmd conf (UpdateCmd updateOpt)     = runLogsUpdate conf updateOpt
runLogsCmd conf (LogsTrimCmd logid lsn )  = trimLog conf logid lsn

runLogsUpdate :: HeaderConfig AdminAPI -> UpdateLogsOpts -> IO ()
runLogsUpdate conf UpdateLogsOpts{..} = do
  let client' = buildLDClientRes conf Map.empty
  withResource client' $ \client -> do
    res <- try $ do
      let warn = "Are you sure you want to update the attributes at "
                 <> show updatePath <> "? (y/n)"
      c <- putStrLn warn >> getChar
      guard (toUpper c == 'Y')
      res <- try $ S.getLogGroup client updatePath
      attrs@S.LogAttributes{..} <-
        case res of
          Right loggroup         -> S.logGroupGetAttrs loggroup
          Left (_ :: S.NOTFOUND) -> S.logDirectoryGetAttrs =<< S.getLogDirectory client updatePath
      let attrs' =
            attrs { logReplicationFactor = maybe logReplicationFactor S.defAttr1 updateReplicationFactor
                  , logSyncedCopies = maybe logSyncedCopies S.defAttr1 updateSyncedCopies
                  , logBacklogDuration = maybe logBacklogDuration (S.defAttr1 . Just) updateBacklogDuration
                  , logAttrsExtras = updateExtras `Map.union` logAttrsExtras
                  }
      attrsPtr <- S.pokeLogAttributes attrs'
      withForeignPtr attrsPtr $ S.ldWriteAttributes client updatePath
    case res of
      Right version                     ->
        putStrLn $ "Attributes for " <> show updatePath
        <> " has been updated in version " <> show version
      Left (e :: S.SomeHStoreException) ->
        putStrLn . formatWith [red] $ "Cannot update attributes for "
        <> show updatePath <> " for reason: " <> show e

runLogsInfo :: HeaderConfig AdminAPI -> S.C_LogID -> IO ()
runLogsInfo conf logid = do
  let client' = buildLDClientRes conf Map.empty
  withResource client' $ \client -> do
    isExist <- S.logIdHasGroup client logid
    if isExist then info client
               else putStrLn "No log-group has this ID!"
  where
    info client = do
      putStrLn "Tail Info:"
      lsn <- S.getTailLSN client logid
      putStrLn $ "  Tail lsn: " <> prettyPrintLSN lsn <> " (" <> show lsn <> ")"
      tail_attr <- S.getLogTailAttrs client logid
      tail_lsn <- S.getLogTailAttrsLSN tail_attr
      putStrLn $ "  Last released real (record) lsn: "
              <> prettyPrintLSN tail_lsn <> " (" <> show tail_lsn <> ")"
      tail_time_stamp <- S.getLogTailAttrsLastTimeStamp tail_attr
      putStrLn $ "  Approximate timestamp of last released real lsn: "
              <> prettyPrintTimeStamp tail_time_stamp
      tail_offset <- S.getLogTailAttrsBytesOffset tail_attr
      putStrLn $ "  Approximate byte offset of the tail of the log: "
              <> show tail_offset

      putStrLn "Head Info:"
      head_attr <- S.getLogHeadAttrs client logid
      head_lsn <- S.getLogHeadAttrsTrimPoint head_attr
      putStrLn $ "  Trim point: "
              <> prettyPrintLSN head_lsn <> "(" <> show head_lsn <> ")"
      head_time_stamp <- S.getLogHeadAttrsTrimPointTimestamp head_attr
      putStrLn $ "  Approximate timestamp of trim point (ms): "
              <> prettyPrintTimeStamp head_time_stamp
      empty <- S.isLogEmpty client logid
      putStrLn $ "Log Empty?: " <> show empty

    prettyPrintLSN lsn =
      "e" ++ show (lsn `shiftR` 32) ++ "n" ++ show (lsn .&. 0xFFFFFFFF)

    prettyPrintTimeStamp ts = unsafePerformIO $ do
      time_str <- if ts /= maxBound then show <$> formatSystemTime simpleDateFormat (MkSystemTime (ts `div` 1000) 0)
                                    else return "INVALID!"
      return $ show ts ++ " -> " ++ time_str

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
    if isDirectory
       then do _ <- S.makeLogDirectory client path createLogsOptsAttrs True
               putStrLn "Create log directory successfully!"
       else let start = fromJust fromId
                end = fromJust toId
             in do _ <- S.makeLogGroup client path start end createLogsOptsAttrs True
                   putStrLn "Create log group successfully!"

runLogsRemove :: HeaderConfig AdminAPI -> RemoveLogsOpts -> IO ()
runLogsRemove conf RemoveLogsOpts{..} = do
  putStr $ "Are you sure you want to remove " <> show rmPath <> "? [y/n] "
  hFlush stdout
  c <- getChar
  if toUpper c == 'Y'
     then do
       let client' = buildLDClientRes conf Map.empty
       withResource client' $ \client -> do
         res <- tryJust (guard . S.isNOTFOUND) $ S.removeLogDirectory client rmPath rmRecursive
         case res of
           Right version ->
             putStrLn $ "directory " <> show rmPath <> " has been removed in version " <> show version
           Left _ -> do
             version <- S.removeLogGroup client rmPath
             putStrLn $ "log group " <> show rmPath <> " has been removed in version " <> show version
     else putStrLn "Ignoring"

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
        tryJust (guard . S.isNOTFOUND) (S.getLogGroup client path) >>=
          (\case
              Right loggroup -> printLogGroup showVerbose loggroup
              Left _         -> S.getLogDirectory client path >>= printLogDir)
      -- when both are missing, print the full tree
      _               -> S.getLogDirectory client "/" >>= printLogDir

shift :: Int
shift = 2

printLogGroup :: Bool -> S.LDLogGroup -> IO ()
printLogGroup = printLogGroup' 0

printLogGroup'
  :: Int
  -- ^ level of indentation
  -> Bool
  -- ^ whether to print all the attributes
  -> S.LDLogGroup
  -- ^ the log group to print
  -> IO ()
printLogGroup' level verbose logGroup = do
  let emit s = putStrLn $ replicate (level * shift) ' ' <> s
  fullName <- S.logGroupGetFullName logGroup
  (lo, hi) <- S.logGroupGetRange logGroup
  emit . formatWith [yellow] $ "\9678 " <> unpack fullName <> " (" <> show lo <> ".." <> show hi <> ")"
  when verbose $ do
    version <- S.logGroupGetVersion logGroup
    emit $ "  Version: " <> show version
    attrs <- S.logGroupGetAttrs logGroup
    emit "  Attributes:"
    printLogAttributes level attrs

printLogDirectory :: S.LDClient -> Int -> Bool -> S.LDDirectory -> IO ()
printLogDirectory = flip printLogDirectory' 0

printLogDirectory' :: S.LDClient
  -> Int
  -- ^ level of indentation
  -> Int
  -- ^ maximum depth of the tree to print
  -> Bool
  -- ^ whether to print all the attributes
  -> S.LDDirectory
  -- ^ the directory to print
  -> IO ()
printLogDirectory' client level maxDepth verbose logDirectory = do
  let emit s = putStrLn $ replicate (level * shift) ' ' <> s
  fullName <- S.logDirectoryGetFullName logDirectory
  emit . formatWith [green] $ "\9660 " <> unpack fullName
  when verbose $ do
    version <- S.logDirectoryGetVersion logDirectory
    emit $ "  Version: " <> show version
    -- TODO: fix the issue when the rep factor of a directory is empty
    -- extraAttrs <- S.logDirectoryGetHsLogAttrs logDirectory
    -- emit "Attributes:"
    -- printExtraAttributes level extraAttrs
  when (level < maxDepth) $ do
    logGroupNames <- S.logDirLogsNames logDirectory
    forM_ logGroupNames $
      S.logDirLogFullName logDirectory >=> S.getLogGroup client >=>
      printLogGroup' (level + 1) verbose
    subDirNames <- S.logDirChildrenNames logDirectory
    forM_ subDirNames $
      S.logDirChildFullName logDirectory >=> S.getLogDirectory client >=>
      printLogDirectory' client (level + 1) maxDepth verbose

printLogAttributes :: Int -> LogAttributes -> IO ()
printLogAttributes level LogAttributes{..} = do
  let cast = Casting.toKebab . Casting.dropPrefix . Casting.fromAny
      emit s = putStrLn $ replicate ((level + 1) * shift) ' ' <> "- " <> s
#define _SHOW_ATTR(x) cast #x <> ": " <> showAttributeVal x
  emit $ _SHOW_ATTR(logReplicationFactor)
  emit $ _SHOW_ATTR(logSyncedCopies)
  emit $ _SHOW_ATTR(logBacklogDuration)
  forM_ (Map.toList logAttrsExtras) $ \(k, v) ->
    emit $ unpack k <> ": " <> unpack v
#undef _SHOW_ATTR

showAttributeVal :: Show a => S.Attribute a -> String
showAttributeVal S.Attribute{..} =
  -- TODO
  let overridden = if not attrInherited then "    (Overridden)" else ""
   in (show . fromJust $ attrValue) <> overridden

runLogsSetRange :: HeaderConfig AdminAPI -> SetRangeOpts -> IO ()
runLogsSetRange conf SetRangeOpts{..} = do
  let client' = buildLDClientRes conf Map.empty
  withResource client' $ \client -> do
    res <- try $ do
      oldRange <- S.logGroupGetRange =<< S.getLogGroup client setRangePath
      let
        printRange :: (S.C_LogID, S.C_LogID) -> String
        printRange (s, e) = "(" <> show s <> ".." <> show e <> ")"
        warn = "Are you sure you want to set the log range at "
          <> unpack setRangePath <> " to be "
          <> printRange (setRangeStartId, setRangeEndId)
          <> " instead of " <> printRange oldRange <> "? (y/n)"
      c <- putStrLn warn >> getChar
      guard (toUpper c == 'Y')
      S.logGroupSetRange client setRangePath (setRangeStartId, setRangeEndId)
    case res of
      Left (e :: S.NOTFOUND) ->
        putStrLn . formatWith [red] $ "Cannot update range for "
          <> unpack setRangePath <> ": " <> show e
      Right version ->
        putStrLn $ "Log group " <> unpack setRangePath
          <> " has been updated in the version " <> show version

trimLog :: HeaderConfig AdminAPI -> S.C_LogID -> S.LSN -> IO ()
trimLog conf logid lsn = do
  let client' = buildLDClientRes conf Map.empty
  withResource client' $ \client -> do
    putStr $ "Are you sure you want to trim log " <> show logid <> "?\n"
          <> "Type \"YES\" for triming other for cancelling: "
    hFlush stdout
    str <- getLine
    if str == "YES" then S.trim client logid lsn else putStrLn "ingoring"
