{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module HStream.RunViewSpec (spec) where

import           Data.Aeson               (decode)
import           Data.Maybe               (isJust)
import qualified Data.Text                as T
import           Network.HTTP.Simple
import           Test.Hspec

import           HStream.HTTP.Server.View (ViewBO (..))
import           HStream.SpecUtils        (buildRequest, createStream,
                                           deleteStream)

createViewSql :: String -> String -> String
createViewSql vName sName
  = "CREATE VIEW " <> vName <> " AS SELECT SUM(a) FROM " <> sName <> " GROUP BY b EMIT CHANGES;"

-- TODO: config the request url
listViews :: IO [ViewBO]
listViews = do
  request <- buildRequest "GET" "views/"
  response <- httpLBS request
  let views = decode (getResponseBody response) :: Maybe [ViewBO]
  case views of
      Nothing     -> return []
      Just views' -> return views'

getView :: String -> IO (Maybe ViewBO)
getView vName = do
  request <- buildRequest "GET" ("views/" <> vName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe ViewBO)

createView :: String -> IO (Maybe ViewBO)
createView sql = do
  request' <- buildRequest "POST" "views/"
  let request = setRequestBodyJSON (ViewBO Nothing Nothing Nothing (T.pack sql)) request'
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe ViewBO)

deleteView :: String -> IO (Maybe Bool)
deleteView vName = do
  request <- buildRequest "DELETE" ("views/" <> vName)
  response <- httpLBS request
  return (decode (getResponseBody response) :: Maybe Bool)

spec :: Spec
spec = describe "HStream.RunViewSpec" $ do
  let vName = "testview"
  let sName = "teststream"

  let sql = createViewSql vName sName

  it "view crud test" $ do
    -- clear streams
    _ <- deleteStream sName
    _ <- deleteStream vName
    -- create source stream
    _ <- createStream sName 3
    view <- createView sql
    view `shouldSatisfy` isJust
    views <- listViews
    (length views >= 1) `shouldBe` True
    case (head views) of
      (ViewBO (Just vid) _ _ _) -> do
        view' <- getView (T.unpack vid)
        view' `shouldSatisfy` isJust
        res <- deleteView (T.unpack vid)
        _ <- deleteStream sName
        _ <- deleteStream vName
        res `shouldBe` (Just True)
      (ViewBO vid _ _ _) -> vid `shouldSatisfy` isJust
