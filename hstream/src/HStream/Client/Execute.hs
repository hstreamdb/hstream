{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RecordWildCards #-}
module HStream.Client.Execute
  ( executeShowPlan
  , execute
  , executeWithAddr_
  , executeInsert
  ) where

import           Control.Concurrent            (modifyMVar_, readMVar, swapMVar)
import           Data.Functor                  (void)
import qualified Data.List                     as L
import qualified Data.Map                      as Map
import qualified Data.Text                     as T
import           Network.GRPC.HighLevel        (GRPCIOError (GRPCIOBadStatusCode))
import           Network.GRPC.HighLevel.Client
import           Z.IO.Network                  (SocketAddr)

import           HStream.Client.Action
import           HStream.Client.Gadget
import           HStream.Client.Type           (ClientContext (..))
import           HStream.Client.Utils
import           HStream.SQL
import           HStream.Server.HStreamApi
import           HStream.Utils                 (Format)

executeShowPlan :: ClientContext -> ShowObject -> IO ()
executeShowPlan ctx showObject =
  case showObject of
    SStreams    -> execute ctx listStreams
    SViews      -> execute ctx listViews
    SQueries    -> execute ctx listQueries
    SConnectors -> execute ctx listConnectors

executeInsert :: ClientContext -> T.Text -> Action AppendResponse -> IO ()
executeInsert ctx@ClientContext{..} sName action = do
  curProducers <- readMVar producers
  maybe retry runWith (Map.lookup sName curProducers)
  where
    runWith realNode = do
      resp <- runActionWithAddr (serverNodeToSocketAddr realNode) action
      case resp of
        ClientNormalResponse{} -> printResult resp
        ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInternal _))
          -> printResult resp
        ClientErrorResponse _  -> retry
    retry = do
      curAddr <- readMVar currentServer
      lookupStream ctx curAddr sName >>= \case
        Nothing       -> putStrLn "Failed to get any available server."
        Just realNode -> do
          modifyMVar_ producers (return . Map.insert sName realNode)
          runWith realNode

execute :: Format a => ClientContext
  -> Action a -> IO ()
execute ctx@ClientContext{..} action = do
  addr <- readMVar currentServer
  executeWithAddr_ ctx addr action printResult

executeWithAddr_
  :: ClientContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO ())
  -> IO ()
executeWithAddr_ ctx@ClientContext{..} addr action handleOKResp = do
  resp <- runActionWithAddr addr action
  case resp of
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInternal details)) ->
      print . unStatusDetails $ details
    ClientErrorResponse _ -> do
      modifyMVar_ availableServers (return . L.delete addr)
      curServers <- readMVar availableServers
      case curServers of
        []  -> putStrLn "No available servers"
        x:_ -> executeWithAddr_ ctx x action handleOKResp
    _ -> do
      void . swapMVar currentServer $ addr
      handleOKResp resp
