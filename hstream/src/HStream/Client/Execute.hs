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

import           Control.Concurrent            (readMVar)
import           Data.Functor                  (void)
import qualified Data.Text                     as T
import           Network.GRPC.HighLevel        (GRPCIOError (GRPCIOBadStatusCode))
import           Network.GRPC.HighLevel.Client
import           Z.IO.Network                  (SocketAddr)

import           HStream.Client.Action
import           HStream.Client.Gadget
import           HStream.Client.Type           (ClientContext (..))
import           HStream.Client.Utils
import           HStream.Server.HStreamApi
import           HStream.SQL
import           HStream.Utils                 (Format, serverNodeToSocketAddr)

executeShowPlan :: ClientContext -> ShowObject -> IO ()
executeShowPlan ctx showObject =
  case showObject of
    SStreams    -> execute ctx listStreams
    SViews      -> execute ctx listViews
    SQueries    -> execute ctx listQueries
    SConnectors -> execute ctx listConnectors

executeInsert :: ClientContext -> T.Text -> Action AppendResponse -> IO ()
executeInsert ctx@ClientContext{..} sName action = do
  curAddr <- readMVar currentServer
  lookupStream ctx curAddr sName >>= \case
    Nothing       -> return ()
    Just realNode -> runWith realNode
  where
    runWith realNode = do
      resp <- runActionWithAddr (serverNodeToSocketAddr realNode) action
      case resp of
        ClientNormalResponse{} -> printResult resp
        ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode _ _))
          -> printResult resp
        ClientErrorResponse _ ->
          executeInsert ctx sName action

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
executeWithAddr_ ctx addr action handleOKResp = do
  void $ getInfoWithAddr ctx addr action (\x -> handleOKResp x >> return Nothing)
