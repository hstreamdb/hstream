{-# OPTIONS_GHC -Werror=incomplete-patterns #-}

module HStream.Kafka.Server.Handler.AdminCommand
  ( handleHadminCommand
  ) where

import           Control.Concurrent            (tryReadMVar)
import           Data.Aeson                    ((.=))
import qualified Data.Aeson                    as Aeson
import qualified Data.HashMap.Strict           as HM
import qualified Data.Text                     as Text

import           HStream.Admin.Server.Types    (errorResponse, plainResponse,
                                                tableResponse)
import           HStream.Gossip                (GossipContext (clusterReady),
                                                getClusterStatus, initCluster)
import           HStream.Kafka.Common.AdminCli (AdminCommand (..),
                                                parseAdminCommand)
import           HStream.Kafka.Server.Types    (ServerContext (..))
import qualified HStream.Server.HStreamApi     as API
import           HStream.Utils                 (showNodeStatus)
import qualified Kafka.Protocol.Encoding       as K
import qualified Kafka.Protocol.Message        as K
import qualified Kafka.Protocol.Service        as K

handleHadminCommand
  :: ServerContext
  -> K.RequestContext
  -> K.HadminCommandRequest
  -> IO K.HadminCommandResponse
handleHadminCommand sc _ req = do
  let args = words (Text.unpack $ K.unCompactString req.command)
  adminCommand <- parseAdminCommand args
  case adminCommand of
    AdminInitCommand       -> runInit sc
    AdminCheckReadyCommand -> runCheckReady sc
    AdminStatusCommand     -> runStatus sc

runInit :: ServerContext -> IO K.HadminCommandResponse
runInit ServerContext{..} = do
  initCluster gossipContext
  return $ packResp $
    plainResponse "Server successfully received init signal"

runCheckReady :: ServerContext -> IO K.HadminCommandResponse
runCheckReady ServerContext{..} = do
  tryReadMVar (clusterReady gossipContext) >>= \case
    Just _  -> return $ packResp $ plainResponse "Cluster is ready"
    Nothing -> return $ packResp $ errorResponse "Cluster is not ready!"

runStatus :: ServerContext -> IO K.HadminCommandResponse
runStatus ServerContext{..} = do
  values <- HM.elems <$> getClusterStatus gossipContext
  let headers = ["Node ID" :: Text.Text, "State", "Address"]
      rows = map consRow values
      content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ packResp $ tableResponse content
  where
    show' = Text.pack . show
    consRow API.ServerNodeStatus{..} =
      let nodeID   = maybe "UNKNOWN" (show' . API.serverNodeId) serverNodeStatusNode
          nodeHost = maybe "UNKNOWN" API.serverNodeHost serverNodeStatusNode
          nodePort = maybe "UNKNOWN" (show' . API.serverNodePort) serverNodeStatusNode
       in [ nodeID
          , showNodeStatus serverNodeStatusState
          , nodeHost <> ":" <> nodePort
          ]

packResp :: Text.Text -> K.HadminCommandResponse
packResp r = K.HadminCommandResponse (K.CompactString r) K.EmptyTaggedFields
