{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Network.HESP.Commands
  ( CommandName
  , CommandParams
  , CommandAction (CommandAction)
  , CommandBox
  , mkCommandsFromList
  , commandRegister
  , getCommand
  , commandParser
  , replyParser
  , extractBulkStringParam
  , extractIntegerParam
  , extractBulkStringParam2
  , extractBulkStringArrayParam
  , extractMapParam
  , extractMapField
  , getBulkStringParam
  , getIntegerParam
  ) where

import           Control.Applicative (liftA2)
import           Data.ByteString     (ByteString)
import           Data.Map.Strict     (Map)
import qualified Data.Map.Strict     as Map
import qualified Data.Text           as Text
import qualified Data.Text.Encoding  as Text
import           Data.Vector         (Vector, (!?))
import qualified Data.Vector         as V

import           Network.HESP.Types  (Message (..))
import qualified Network.HESP.Types  as T

-------------------------------------------------------------------------------

type CommandName = ByteString
type CommandParams = Vector Message

newtype CommandAction a = CommandAction (CommandParams -> a)

instance Show (CommandAction a) where
  show _ = "<CommandAction>"

newtype CommandBox a = CommandBox (Map CommandName (CommandAction a))
  deriving (Semigroup, Monoid, Show)

mkCommandsFromList :: [(CommandName, CommandAction a)] -> CommandBox a
mkCommandsFromList = CommandBox . Map.fromList

commandRegister :: CommandName -> CommandAction a -> CommandBox a -> CommandBox a
commandRegister name action (CommandBox cmds) =
  CommandBox $ Map.insert name action cmds

getCommand :: CommandBox a -> CommandName -> Maybe (CommandAction a)
getCommand (CommandBox cmds) name = Map.lookup name cmds

commandParser :: Message -> Either ByteString (CommandName, CommandParams)
commandParser msg = validateCmdProtoType msg >>= validateCommand

replyParser :: Message -> Either ByteString (CommandName, CommandParams)
replyParser = validateReply

-------------------------------------------------------------------------------

getBulkStringParam :: CommandParams -> Int -> Maybe ByteString
getBulkStringParam params idx = T.getBulkString =<< (params !? idx)

getIntegerParam :: CommandParams -> Int -> Maybe Integer
getIntegerParam params idx = T.getInterger =<< (params !? idx)

extractBulkStringParam
  :: ByteString      -- ^ label
  -> CommandParams   -- ^ vector of params
  -> Int             -- ^ index
  -> Either ByteString ByteString
  -- ^ Either error message or bulk string
extractBulkStringParam label params idx =
  case params !? idx of
    Just (MatchBulkString x) -> Right x
    Just _                   -> Left $ label <> " must be a bulk string."
    Nothing                  -> Left $ label <> " can not be empty."

extractBulkStringParam2
  :: (ByteString, Int)
  -> (ByteString, Int)
  -> CommandParams
  -> Either ByteString (ByteString, ByteString)
extractBulkStringParam2 (l, i) (l', i') params =
  let r = extractBulkStringParam l params i
      r' = extractBulkStringParam l' params i'
   in liftA2 (,) r r'

extractIntegerParam
  :: ByteString      -- ^ label
  -> CommandParams   -- ^ vector of params
  -> Int             -- ^ index
  -> Either ByteString Integer
extractIntegerParam label params idx =
  case params !? idx of
    Just (Integer x) -> Right x
    Just _           -> Left $ label <> " must be an integer."
    Nothing          -> Left $ label <> " can not be empty."

extractBulkStringArrayParam
  :: ByteString      -- ^ label
  -> CommandParams   -- ^ vector of params
  -> Int             -- ^ index
  -> Either ByteString (V.Vector ByteString)
extractBulkStringArrayParam label params idx =
  case params !? idx of
    Just (MatchArray ms) -> do
      bs <- mapM (extractBulkStringParam "Payload item" ms)
                 (V.enumFromN 0 $ V.length ms)
      Right bs
    Just _               -> Left $ label <> " must be an array."
    Nothing              -> Left $ label <> " can not be empty."

extractMapParam
  :: ByteString      -- ^ label
  -> CommandParams   -- ^ vector of params
  -> Int             -- ^ index
  -> Either ByteString (Map Message Message)
extractMapParam label params idx =
  case params !? idx of
    Just (MatchMap m) -> Right m
    Just _            -> Left $ label <> " must be a map."
    Nothing           -> Left $ label <> " can not be empty."

extractMapField
  :: Map Message Message
  -> Message
  -> Either ByteString Message
extractMapField dict field =
  case Map.lookup field dict of
    Nothing -> Left $ (Text.encodeUtf8 . Text.pack . show $ field)
                   <> " field is not found in map."
    Just v  -> Right v

-------------------------------------------------------------------------------

validateCmdProtoType :: Message -> Either ByteString (Vector Message)
validateCmdProtoType (MatchArray ms) = Right ms
validateCmdProtoType _ = Left "Command must be sent through array type."
{-# INLINE validateCmdProtoType #-}

validateCommand
  :: Vector Message
  -> Either ByteString (CommandName, CommandParams)
validateCommand ms =
  let name = extractBulkStringParam "Command name" ms 0
      -- an empty vector is returned if @ms@ is empty, there is no exception.
      payloads = V.drop 1 ms
   in liftA2 (,) name (Right payloads)
{-# INLINE validateCommand #-}

validateReply :: Message -> Either ByteString (ByteString, Vector Message)
validateReply (MatchPush n args) = Right (n, args)
validateReply _ = Left "Reply must be sent through push type."
{-# INLINE validateReply #-}
