{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.PubSub.TopicMap where

import           Data.HashMap.Strict  as HM
import           HStream.PubSub.Types
import           HStream.Store

emptyTM :: TopicMap
emptyTM = HM.empty

insertTM :: Topic -> TopicID -> TopicMap -> TopicMap
insertTM tp tid m = insert tp tid m

lookupTM :: Topic -> TopicMap -> Maybe TopicID
lookupTM t m = HM.lookup t m
