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

type Message = Bytes

type EIO a = ExceptT SomeStreamException IO a

type ClientID = Text
