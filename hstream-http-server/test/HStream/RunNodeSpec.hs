{-# LANGUAGE OverloadedStrings #-}
module HStream.RunNodeSpec (spec) where

import           Data.Aeson               (Value, decode)
import           Data.Maybe               (isJust)
import           Network.HTTP.Simple
import           Test.Hspec

import           HStream.HTTP.Server.Node (NodeBO)
import           HStream.SpecUtils        (buildRequest)

-- TODO: config the request url
listNodes :: IO [NodeBO]
listNodes = do
  request <- buildRequest "GET" "nodes"
  response <- httpLBS request
  let nodes = decode (getResponseBody response) :: Maybe [NodeBO]
  case nodes of
      Nothing     -> return []
      Just nodes' -> return nodes'

getNode :: IO (Maybe NodeBO)
getNode = do
  request <- buildRequest "GET" "nodes/0"
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe NodeBO)

spec :: Spec
spec = describe "HStream.RunNodeSpec" $ do
  it "list nodes" $ do
    nodes <- listNodes
    (length nodes >= 1) `shouldBe` True

  it "get node" $ do
    node <- getNode
    node `shouldSatisfy` isJust
