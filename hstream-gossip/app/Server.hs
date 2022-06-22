{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import           Control.Concurrent.Async     (wait)
import qualified Options.Applicative          as O

import qualified HStream.Gossip.HStreamGossip as API
import           HStream.Gossip.Start         (initGossipContext, startGossip)
import           HStream.Gossip.Types         (CliOptions (..), cliOpts)
import           HStream.Gossip.Utils         (defaultGossipOpts)
import qualified HStream.Logger               as Log

main :: IO ()
main = do
  Log.setLogLevel (Log.Level Log.INFO) True
  CliOptions{..} <- O.execParser $ O.info (cliOpts O.<**> O.helper) O.fullDesc
  let serverSelf = API.ServerNodeInternal {
        serverNodeInternalId   = _serverId
      , serverNodeInternalHost = _serverHost
      , serverNodeInternalPort = _serverPort
      , serverNodeInternalGossipPort = _serverGossipPort
      }
  gc <- initGossipContext defaultGossipOpts mempty serverSelf
  let target = case (_joinHost, _joinPort) of
        (Just host, Just port) -> [(host, port)]
        (Nothing  , Nothing  ) -> []
        _ -> error "Please specify a server with both host and port"
  startGossip _serverHost target gc >>= wait
