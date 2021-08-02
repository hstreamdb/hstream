{-# LANGUAGE OverloadedStrings #-}
module HStream.RunStreamSpec (spec) where

import           Data.Aeson                 (decode)
import qualified Data.Text                  as T
import           Network.HTTP.Simple
import           Test.Hspec

import           HStream.HTTP.Server.Stream (StreamBO (..))
import           HStream.SpecUtils          (buildRequest, createStream,
                                             deleteStream)

-- TODO: config the request url
listStreams :: IO [StreamBO]
listStreams = do
  request' <- buildRequest "GET" "streams/"
  response <- httpLBS request'
  let streams = decode (getResponseBody response) :: Maybe [StreamBO]
  case streams of
      Nothing       -> return []
      Just streams' -> return streams'

getStream :: String -> IO (Maybe StreamBO)
getStream sName = do
  request <- buildRequest "GET" ("streams/" <> sName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe StreamBO)

spec :: Spec
spec = describe "HStream.RunStreamSpec" $ do
  let sName = "teststream"

  it "create stream" $ do
    stream <- createStream sName 3
    stream `shouldBe` (Just (StreamBO (T.pack sName) 3))

  it "list streams" $ do
    streams <- listStreams
    (length streams >= 1) `shouldBe` True

  it "get stream" $ do
    stream <- getStream sName
    stream `shouldBe` (Just (StreamBO (T.pack sName) 3))

  it "delete stream" $ do
    res <- deleteStream sName
    res `shouldBe` (Just True)
