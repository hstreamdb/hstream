{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeSynonymInstances  #-}

module HStream.TestUtils where

import qualified Data.Aeson              as A
import qualified Data.Text               as T
import           GHC.Generics            (Generic)
import           HStream.MetaStore.Types (FHandle, HasPath (..), RHandle)
import           Test.QuickCheck         (Arbitrary (..), Gen, chooseEnum,
                                          elements, frequency, listOf, listOf1,
                                          oneof)
import           ZooKeeper.Types         (ZHandle)

data MetaExample = Meta {
    metaId      :: T.Text
  , metaState   :: StateExample
  , metaRelates :: T.Text
  }
  deriving (Ord, Show, Eq, Generic, A.FromJSON, A.ToJSON)

instance HasPath MetaExample ZHandle where
  myRootPath = "/testTemp"
instance HasPath MetaExample RHandle where
  myRootPath = "testTemp"
instance HasPath MetaExample FHandle where
  myRootPath = "testTemp"

data StateExample
  = Starting
  | Running
  | Stopped
  deriving (Ord, Enum, Show, Eq, Generic, A.FromJSON, A.ToJSON)

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
  a <- listOf1 alphabet
  b <- listOf alphaNum
  return $ a <> b

contents :: Gen T.Text
contents = fmap T.pack $ listOf1 $
  frequency [ (1, elements "~!@#$%^&*()_+`<>?:;,./|[]{}")
            , (100, alphaNum)]

metaGen :: Gen MetaExample
metaGen = Meta <$> name <*> arbitrary <*> contents
