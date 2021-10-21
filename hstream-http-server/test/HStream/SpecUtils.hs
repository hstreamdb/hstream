module HStream.SpecUtils where

import           Data.Aeson                 (decode)
import           Data.Maybe                 (fromJust, fromMaybe)
import qualified Data.Text                  as T
import           Data.Word                  (Word32)
import           Network.HTTP.Simple
import           System.Environment         (lookupEnv)

import           HStream.HTTP.Server.Stream

createStream :: String -> Word32 -> IO (Maybe StreamBO)
createStream sName rep = do
  request' <- buildRequest "POST" "streams"
  let request = setRequestBodyJSON (StreamBO (T.pack sName) rep) $ request'
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe StreamBO)

appendStream :: String -> Records -> IO AppendResult
appendStream sName recs = do
  req <- buildRequest "POST" $ "streams/" <> sName <> "/publish"
  let request = setRequestBodyJSON recs req
  resp <- httpLBS request
  pure $ fromJust (decode (getResponseBody resp) :: Maybe AppendResult)

deleteStream :: String -> IO (Maybe Bool)
deleteStream sName = do
  request <- buildRequest "DELETE" ("streams/" <> sName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe Bool)

getHTTPPort :: IO String
getHTTPPort = fromMaybe "8000" <$> lookupEnv "HTTP_LOCAL_PORT"

buildRequest :: String -> String -> IO Request
buildRequest method subPath = do
  port <- getHTTPPort
  parseRequest $ method <> " http://localhost:" <> port <> "/" <> subPath
