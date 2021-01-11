{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.PubSub.Types where

import           Control.Exception
import           Control.Monad.Except
import           Data.HashMap.Strict     (HashMap)
import           Data.Hashable
import           Data.IORef
import           GHC.Generics            (Generic)
import           HStream.Store
import           HStream.Store.Exception
import           Z.Data.Text

-- | mqtt Topic
-- e: "a/a/a/a", "a/b"
newtype Topic = Topic Text deriving (Show, Eq, Ord, Generic)

instance Hashable Topic

-- | topic filter
-- a/a/a, a/+/a, a/a/#
newtype Filter = Filter Text deriving (Show, Eq, Ord)

-- Topic -> TopicID
type TopicMap = HashMap Topic TopicID

-- | global topic map
type GlobalTM = IORef TopicMap

type Message = Bytes

type EIO a = ExceptT SomeStreamException IO a

type ClientID = Text
