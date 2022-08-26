{-# LANGUAGE DeriveAnyClass #-}
module HStream.RqliteUtilsSpec where

import qualified Data.Aeson                 as A
import           Data.Maybe                 (fromMaybe)
import qualified Data.Text                  as T
import           GHC.Generics               (Generic)
import           Network.HTTP.Client        (Manager, defaultManagerSettings,
                                             newManager)
import           System.Environment         (lookupEnv)
import           Test.Hspec
import           Test.QuickCheck

import           HStream.Common.RqliteUtils
import qualified HStream.Logger             as Log


data MetaExample = Meta {
    metaId      :: T.Text
  , metaState   :: StateExample
  , metaRelates :: T.Text
  }
  deriving (Show, Eq, Generic, A.FromJSON, A.ToJSON)

data StateExample
  = Starting
  | Running
  | Stopped
  deriving (Enum, Show, Eq, Generic, A.FromJSON, A.ToJSON)

instance Arbitrary StateExample where
  arbitrary = chooseEnum (Starting, Stopped)

instance Arbitrary MetaExample where
  arbitrary = do
    Meta <$> name <*> arbitrary <*> contents

data AB = AB {a :: T.Text, b ::Int}
  deriving (Show, Eq, Generic, A.FromJSON, A.ToJSON)

alphabet :: Gen Char
alphabet = elements $ ['a'..'z'] ++ ['A'..'Z']

alphaNum :: Gen Char
alphaNum = oneof [alphabet, elements ['0'..'9']]

name :: Gen T.Text
name = T.pack <$> do
  a <- listOf alphabet
  b <- listOf1 alphaNum
  return $ a <> b

contents :: Gen T.Text
contents = fmap T.pack $ listOf1 $
  frequency [ (1, elements "~!@#$%^&*()_+`<>?:;,./|[]{}")
            , (100, alphaNum)]

spec :: Spec
spec = do
  runIO $ Log.setLogLevel (Log.Level Log.INFO) True
  m <- runIO $ newManager defaultManagerSettings
  port <- runIO $ fromMaybe "4001" <$> lookupEnv "RQLITE_LOCAL_PORT"
  let host = "localhost"
  let url = T.pack $ host <> ":" <> port

  it "Smoke Test" $ do
    let table = "exampleTable"
    let v  = AB { a = "EXAMPLE-VALUE", b =1}
    let v2 = AB { a = "EXAMPLE-VALUE-2", b =2}
    createTable m url table `shouldReturn` True
    insertInto  m url table "my-id-1" v          `shouldReturn` True
    selectFrom  m url table (Just "my-id-1")     `shouldReturn` Just [v]
    updateSet   m url table "my-id-1" Nothing v2 `shouldReturn` True
    selectFrom  m url table (Just "my-id-1")     `shouldReturn` Just [v2]
    deleteFrom  m url table "my-id-1" Nothing    `shouldReturn` True
    selectFrom  m url table (Just "my-id-1")     `shouldReturn` (Nothing :: Maybe [AB])
    deleteTable m url table `shouldReturn` True

  aroundAll (runWithUrlAndTable m url) $ do
    describe "Detailed test" $ do
      it "Insert into table with random data" $ \table -> do
        meta@Meta{..} <- generate arbitrary
        insertInto m url table metaId meta `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

      it "Update with random data" $ \table -> do
        meta@Meta{..} <- generate arbitrary
        putStrLn "Update Empty"
        updateSet m url table metaId Nothing meta `shouldReturn` False
        insertInto m url table metaId meta `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Update with version"
        meta2 <- generate (arbitrary :: Gen MetaExample)
        updateSet m url table metaId (Just 1) meta2 `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta2]

        putStrLn "Update with no version"
        updateSet m url table metaId Nothing meta `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Update with invalid version"
        updateSet m url table metaId (Just 1) meta2 `shouldReturn` False
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

      it "Delete from with id" $ \table -> do
        meta@Meta{..} <- generate arbitrary

        insertInto m url table metaId meta `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Delete with wrong version"
        deleteFrom m url table metaId (Just 10) `shouldReturn` False
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Delete with no version"
        deleteFrom m url table metaId Nothing `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` (Nothing :: Maybe [MetaExample])

        insertInto m url table metaId meta `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` Just [meta]

        putStrLn "Delete with version"
        deleteFrom m url table metaId (Just 1) `shouldReturn` True
        selectFrom m url table (Just metaId) `shouldReturn` (Nothing :: Maybe [MetaExample])


runWithUrlAndTable :: Manager -> T.Text -> ActionWith T.Text -> IO ()
runWithUrlAndTable manager url action = do
  tableName <- generate name
  createTable manager url tableName `shouldReturn` True
  action tableName
  deleteTable manager url tableName `shouldReturn` True
