module HStream.Utils.Validation where

import           Control.Exception (throwIO)
import           Data.Char         (isAlphaNum, isLetter)
import qualified Data.Text         as T

import           HStream.Exception (InvalidObjectIdentifier (..))
import           HStream.Logger    as Log

-- FIXME: Currently CLI does not support
-- parsing "." or "-" in the stream name
validMarks :: String
validMarks = "-_"

validateNameAndThrow :: T.Text -> IO ()
validateNameAndThrow n =
  case validateNameText n of
    Left s   -> do
      Log.warning $ "Invalid Object Identifier:" <> Log.build s
      throwIO (InvalidObjectIdentifier s)
    Right () -> return ()

validateChar :: Char -> Either String ()
validateChar c
  | isAlphaNum c || elem c validMarks = Right ()
  | otherwise = Left $ "Illegal character \'" ++ (c : "\' found")

validateLength :: T.Text -> Either String ()
validateLength x = if T.length x <= 255 && T.length x > 0 then Right () else Left "The length must be between 1 and 255 characters."

validateHead :: T.Text -> Either String ()
validateHead x = if isLetter (T.head x) then Right () else Left "The first character of a name must be a letter"

validateReserved :: T.Text -> Either String ()
validateReserved x
  | x `elem` reservedName = Left $ "Name \"" ++ T.unpack x ++ "\" is reserved."
  | otherwise = Right ()

reservedName :: [T.Text]
reservedName = ["zookeeper"]

validateNameText :: T.Text -> Either String ()
validateNameText x = validateLength x >> validateHead x
  >> T.foldr ((>>) . validateChar) (Right ()) x
