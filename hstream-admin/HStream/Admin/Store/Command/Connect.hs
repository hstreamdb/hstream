module HStream.Admin.Store.Command.Connect
  ( connect
  ) where

import qualified HStream.Admin.Store.API as AA

-- TODO
-- Start a local proxy server to cache connection to admin server

connect :: AA.HeaderConfig a -> IO ()
connect = undefined
