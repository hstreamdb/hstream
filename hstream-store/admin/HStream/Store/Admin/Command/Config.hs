module HStream.Store.Admin.Command.Config
  ( dumpConfig
  ) where

import           Data.Text               (Text)

import qualified HStream.Store.Admin.API as AA

dumpConfig :: AA.HeaderConfig AA.AdminAPI ->  IO Text
dumpConfig conf = AA.sendAdminApiRequest conf AA.dumpServerConfigJson
