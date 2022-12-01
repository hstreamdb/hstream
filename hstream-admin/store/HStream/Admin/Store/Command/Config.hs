module HStream.Admin.Store.Command.Config
  ( dumpConfig
  ) where

import           Data.Text               (Text)

import qualified HStream.Admin.Store.API as AA

dumpConfig :: AA.HeaderConfig AA.AdminAPI ->  IO Text
dumpConfig conf = AA.sendAdminApiRequest conf AA.dumpServerConfigJson
