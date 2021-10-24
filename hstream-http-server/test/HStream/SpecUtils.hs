module HStream.SpecUtils where

import           Data.Aeson                 (decode)
import           Data.Maybe                 (fromJust, fromMaybe)
import qualified Data.Text                  as T
import           Data.Word                  (Word32)
import           Network.HTTP.Simple
import           System.Environment         (lookupEnv)

import           HStream.HTTP.Server.Stream

createStream :: String -> Word32 -> IO StreamBO
createStream sName rep = do
  request' <- buildRequest "POST" "streams"
  let request = setRequestBodyJSON (StreamBO (T.pack sName) rep) $ request'
  response <- httpLBS request
  pure $ fromJust (decode (getResponseBody response) :: Maybe StreamBO)

appendStream :: AppendBO -> IO AppendResult
appendStream appendBO = do
  req <- buildRequest "POST" $ "streams/publish"
  let request = setRequestBodyJSON appendBO req
  resp <- httpLBS request
  pure $ fromJust (decode (getResponseBody resp) :: Maybe AppendResult)

deleteStream :: String -> IO ()
deleteStream sName = do
  request <- buildRequest "DELETE" ("streams/" <> sName)
  response <- httpLBS request
  pure $ fromJust (decode (getResponseBody response) :: Maybe ())

getHTTPPort :: IO String
getHTTPPort = fromMaybe "8000" <$> lookupEnv "HTTP_LOCAL_PORT"

buildRequest :: String -> String -> IO Request
buildRequest method subPath = do
  port <- getHTTPPort
  parseRequest $ method <> " http://localhost:" <> port <> "/" <> subPath
