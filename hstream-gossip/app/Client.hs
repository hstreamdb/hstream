{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import qualified Data.Vector                      as V
import qualified Network.GRPC.HighLevel.Generated as GRPC
import qualified Options.Applicative              as O

import           HStream.Client.Types             (CliOpts (..), Command (..),
                                                   eventName, eventPayload)
import           HStream.Gossip.HStreamGossip     (CliJoinReq (..),
                                                   Cluster (clusterMembers),
                                                   Empty (Empty),
                                                   HStreamGossip (..),
                                                   UserEvent (..),
                                                   hstreamGossipClient)
import           HStream.Gossip.Types             (serverHost, serverPort)
import           HStream.Gossip.Utils             (mkClientNormalRequest,
                                                   mkGRPCClientConf',
                                                   showNodesTable)

main :: IO ()
main = runCli =<< O.customExecParser (O.prefs O.showHelpOnEmpty) cliParser

join :: O.ParserInfo Command
join = O.info (Join <$> serverHost <*> serverPort) (O.progDesc "Join the cluster of specified node")

status :: O.ParserInfo Command
status = O.info (pure Status) (O.progDesc "Show the status of the cluster")

event :: O.ParserInfo Command
event = O.info (Event <$> eventName <*> eventPayload) (O.progDesc "Broadcast an event to the cluster")

cmdParser :: O.Parser Command
cmdParser = O.hsubparser (O.command "join" join <> O.command "status" status <> O.command "event" event)

cliParser :: O.ParserInfo CliOpts
cliParser = O.info ((CliOpts <$> serverHost <*> serverPort <*> cmdParser) O.<**> O.helper) (O.progDesc "Demo Client")

runCli :: CliOpts -> IO ()
runCli CliOpts{..} = GRPC.withGRPCClient (mkGRPCClientConf' targetHost (fromIntegral targetPort)) $ \client -> do
  HStreamGossip{..} <- hstreamGossipClient client
  case cliCmd of
    Join clusterHost clusterPort -> hstreamGossipCliJoin (mkClientNormalRequest (CliJoinReq clusterHost clusterPort)) >>= \case
      GRPC.ClientNormalResponse resp _ _ _ _ -> print resp
      GRPC.ClientErrorResponse  err          -> print err
    Status                       -> hstreamGossipCliCluster (mkClientNormalRequest Empty) >>= \case
      GRPC.ClientNormalResponse resp _ _ _ _ -> do
        let members = V.toList $ clusterMembers resp
        print (length members)
        putStrLn . showNodesTable $ members
      GRPC.ClientErrorResponse  err          -> print err
    Event name payload           -> hstreamGossipCliUserEvent (mkClientNormalRequest (UserEvent name payload)) >>= \case
      GRPC.ClientNormalResponse resp _ _ _ _ -> print resp
      GRPC.ClientErrorResponse  err          -> print err
