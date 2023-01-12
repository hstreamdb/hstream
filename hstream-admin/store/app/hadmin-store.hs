{-# LANGUAGE BangPatterns #-}

module Main (main) where

import qualified Data.Text                 as T
import qualified Data.Text.IO              as TIO
import           Numeric                   (showFFloat)
import           Options.Applicative       ((<**>))
import qualified Options.Applicative       as O

import           HStream.Admin.Store.Cli   (runStoreCli)
import qualified HStream.Admin.Store.Types as Store
import qualified HStream.Logger            as Log
import qualified HStream.Store.Logger      as CLog

main :: IO ()
main = runStoreCli =<< O.customExecParser (O.prefs O.showHelpOnEmpty) opts
  where
    opts = O.info (Store.cliParser <**> O.helper) O.fullDesc
