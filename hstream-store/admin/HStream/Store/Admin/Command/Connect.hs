module HStream.Store.Admin.Command.Connect
  ( connect
  ) where

import qualified HStream.Store.Admin.API as AA

-- TODO
-- Start a local proxy server to cache connection to admin server

connect :: AA.HeaderConfig a -> IO ()
connect = undefined
