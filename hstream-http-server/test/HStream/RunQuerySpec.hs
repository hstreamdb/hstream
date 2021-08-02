{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module HStream.RunQuerySpec (spec) where

import           Data.Aeson                (decode)
import           Data.Maybe                (isJust)
import qualified Data.Text                 as T
import           Network.HTTP.Simple
import           Test.Hspec

import           HStream.HTTP.Server.Query (QueryBO (..))
import           HStream.SpecUtils         (buildRequest, createStream,
                                            deleteStream)

createQuerySql :: String -> String
createQuerySql stream
  = "SELECT * FROM " <> stream <> " EMIT CHANGES;"

-- TODO: config the request url
listQueries :: IO [QueryBO]
listQueries = do
  request <- buildRequest "GET" "queries"
  response <- httpLBS request
  let queries = decode (getResponseBody response) :: Maybe [QueryBO]
  case queries of
      Nothing       -> return []
      Just queries' -> return queries'

getQuery :: String -> IO (Maybe QueryBO)
getQuery qName = do
  request <- buildRequest "GET" ("queries/" <> qName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe QueryBO)

cancelQuery :: String -> IO (Maybe Bool)
cancelQuery qName = do
  request <- buildRequest "POST" ("queries/cancel/" <> qName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe Bool)

createQuery :: String -> String -> IO (Maybe QueryBO)
createQuery qName sql = do
  request' <- buildRequest "POST" "queries"
  let request = setRequestBodyJSON (QueryBO (T.pack qName) Nothing Nothing (T.pack sql)) request'
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe QueryBO)

deleteQuery :: String -> IO (Maybe Bool)
deleteQuery qName = do
  request <- buildRequest "DELETE" ("queries/" <> qName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe Bool)

spec :: Spec
spec = describe "HStream.RunQuerySpec" $ do
  let qName = "testquery"
  let sName = "teststream"

  let sql = createQuerySql sName

  it "create query" $ do
    _ <- createStream sName 3
    query <- createQuery qName sql
    query `shouldSatisfy` isJust

  it "list queries" $ do
    queries <- listQueries
    (length queries >= 1) `shouldBe` True

  it "get query" $ do
    query <- getQuery qName
    query `shouldSatisfy` isJust

  it "cancel query" $ do
    query <- cancelQuery qName
    query `shouldBe` (Just True)

  it "delete query" $ do
    query <- deleteQuery qName
    _ <- deleteStream sName
    query `shouldBe` (Just True)
