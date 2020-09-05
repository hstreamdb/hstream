{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Command
  ( onRecvMsg
  ) where

import qualified Colog
import           Control.Exception               (SomeException, throw, try)
import           Control.Monad.Reader            (liftIO)
import           Data.ByteString                 (ByteString)
import qualified Data.ByteString.Char8           as BSC
import           Data.CaseInsensitive            (CI)
import qualified Data.CaseInsensitive            as CI
import qualified Data.Sequence                   as Seq
import qualified Data.Text                       as Text
import           Data.Text.Encoding              (decodeUtf8)
import           Data.Vector                     (Vector)
import qualified Data.Vector                     as V
import           Data.Word                       (Word64)
import           GHC.Exts                        (IsList (..))
import           Network.Socket                  (Socket)
import           Text.Read                       (readMaybe)

import           HStream.LogStore.Base           (Context, EntryID,
                                                  LogStoreException (..))
import qualified HStream.Server.Command.Response as I
import qualified HStream.Server.Store            as Store
import           HStream.Server.Types            (App)
import           HStream.Utils                   ((.|.))
import qualified HStream.Utils                   as U
import qualified Network.HESP                    as HESP
import qualified Network.HESP.Commands           as HESP

-------------------------------------------------------------------------------

-- | Client request types
data RequestType
  = XAdd ByteString ByteString
  | XRange ByteString (Maybe EntryID) (Maybe EntryID) (Maybe Integer)
  deriving (Show, Eq)

-- | Parse client request and then send response to client.
onRecvMsg :: Socket
          -> Context
          -> Either String HESP.Message
          -> App (Maybe ())
onRecvMsg sock _ (Left errmsg) = do
  Colog.logError $ "Failed to parse message: " <> Text.pack errmsg
  HESP.sendMsg sock $ I.mkGeneralError $ U.str2bs errmsg
  -- Outer function will catch all exceptions and do a clean job.
  errorWithoutStackTrace "Invalid TCP message!"
onRecvMsg sock ctx (Right msg) =
  case parseRequest msg of
    Left e    -> do
      Colog.logWarning $ decodeUtf8 e
      HESP.sendMsg sock $ I.mkGeneralError e
      return Nothing
    Right req -> processRequest sock ctx req

parseRequest :: HESP.Message
             -> Either ByteString RequestType
parseRequest msg = do
  (n, paras) <- HESP.commandParser msg
  case CI.mk n of
    ("xadd"   :: CI ByteString) -> parseXAdd paras
    ("xrange" :: CI ByteString) -> parseXRange paras
    _                           -> Left $ "Unrecognized request " <> n <> "."

processRequest :: Socket -> Context -> RequestType -> App (Maybe ())
processRequest sock ctx rt = processXAdd   sock ctx rt
                         .|. processXRange sock ctx rt

-------------------------------------------------------------------------------
-- Parse client requests

parseXAdd :: Vector HESP.Message -> Either ByteString RequestType
parseXAdd paras = do
  topic <- HESP.extractBulkStringParam "Topic" paras 0
  reqid <- HESP.extractBulkStringParam "Entry ID" paras 1
  _     <- validateBSSimple "*" reqid "Invalid stream ID specified as stream command argument"
  let kvs = V.drop 2 paras
  case V.length kvs > 0 && even (V.length kvs) of
    -- XXX: separate payload generating process
    True  -> return $ XAdd topic (HESP.serialize $ HESP.mkArray kvs)
    False -> Left "wrong number of arguments for 'xadd' command"

parseXRange :: Vector HESP.Message -> Either ByteString RequestType
parseXRange paras = do
  topic <- HESP.extractBulkStringParam "Topic"     paras 0
  sids  <- HESP.extractBulkStringParam "Start ID"  paras 1
  sid   <- validateEntryIDSimple sids "-" "Invalid stream ID specified as stream command argument"
  eids  <- HESP.extractBulkStringParam "End ID"    paras 2
  eid   <- validateEntryIDSimple eids "+" "Invalid stream ID specified as stream command argument"
  case V.length paras of
    3 -> return $ XRange topic sid eid Nothing
    5 -> do
      count  <- HESP.extractBulkStringParam "Option" paras 3
      _      <- validateBSSimple "count" count "syntax error"
      maxns  <- HESP.extractBulkStringParam "Maxn"   paras 4
      e_maxn <- validateIntSimple maxns "" "value is not an integer or out of range"
      case e_maxn of
        Just n -> return $ XRange topic sid eid (Just $ toInteger n)
        _      -> Left "value is not an integer or out of range"
    _ -> Left "syntax error"

-------------------------------------------------------------------------------
-- Process client requests

processXAdd :: Socket -> Context -> RequestType -> App (Maybe ())
processXAdd sock ctx (XAdd topic payload) = do
  r <- liftIO $ Store.sput ctx topic payload
  case r of
    Right entryID -> do
      liftIO $ HESP.sendMsg sock $ (HESP.mkBulkString . U.encodeUtf8) entryID
    Left (e :: SomeException) -> do
      let errmsg = "Message storing failed: " <> (U.str2bs . show) e
      Colog.logWarning $ decodeUtf8 errmsg
      liftIO $ HESP.sendMsg sock $ I.mkGeneralError errmsg
  return $ Just ()
processXAdd _ _ _ = return Nothing

processXRange :: Socket -> Context -> RequestType -> App (Maybe ())
processXRange sock ctx (XRange topic sid eid maxn) = do
  Colog.logDebug $ "Processing XRANGE request."
                <> " Start: " <> (U.textShow sid)
                <> " End: "   <> (U.textShow eid)
                <> " COUNT: " <> (U.textShow maxn)
  let cut = case maxn of
        Nothing -> id
        Just n  -> Seq.take (fromInteger n)
  e_s <- liftIO . try $ cut <$> Store.readEntries ctx topic sid eid
  case e_s of
    Left (LogStoreLogNotFoundException _) -> do
      HESP.sendMsg sock $ HESP.mkArrayFromList []
    Left e  -> throw e
    Right s -> do
      let respMsg = HESP.mkArrayFromList . toList $ I.mkSimpleElementResp <$> s
      HESP.sendMsg sock respMsg
  return $ Just ()
processXRange _ _ _ = return Nothing

-------------------------------------------------------------------------------
-- Helpers

-- Return value of `validateIntSimple`, `validateEntryID` and `validateEntryIDSimple`:
-- Left bs: validation failed
-- Right Nothing: it matched default value
-- Right (Just n): validation succeeded
validateIntSimple :: ByteString -> ByteString -> ByteString -> Either ByteString (Maybe Word64)
validateIntSimple s defaultVal errMsg
  | CI.mk s == CI.mk defaultVal = Right Nothing
  | otherwise = case readMaybe (BSC.unpack s) of
      Nothing -> Left errMsg
      Just x  -> Right (Just x)

validateBSSimple :: ByteString -> ByteString -> ByteString -> Either ByteString ByteString
validateBSSimple expected real errMsg
  | CI.mk real == CI.mk expected = Right real
  | otherwise = Left errMsg

validateEntryIDSimple
  :: ByteString
  -> ByteString
  -> ByteString
  -> Either ByteString (Maybe EntryID)
validateEntryIDSimple s defaultVal errMsg
  | CI.mk s == CI.mk defaultVal = Right Nothing
  | otherwise = case readMaybe (BSC.unpack s) of
      Nothing      -> Left errMsg
      Just entryID -> Right (Just entryID)
