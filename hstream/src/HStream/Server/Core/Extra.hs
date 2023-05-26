{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}

module HStream.Server.Core.Extra where

import qualified Data.Text                 as T

import           Data.Maybe                (fromMaybe)
import           HStream.Server.HStreamApi as API
import           HStream.Version           (hstreamCommit, hstreamVersion)

getVersion :: IO API.HStreamVersion
getVersion = do
  let tVersion = fromMaybe "unknown version" . T.stripPrefix prefix $ T.pack hstreamVersion
      tCommit = T.pack hstreamCommit
      ver = API.HStreamVersion { hstreamVersionVersion = tVersion, hstreamVersionCommit = tCommit }
  return ver
 where
   prefix = T.singleton 'v'
