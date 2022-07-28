{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RecordWildCards #-}
module HStream.Client.Execute
  ( executeShowPlan
  , execute_
  , execute
  , executeWithAddr
  -- , executeInsert
  , executeWithAddr_
  ) where

import           Control.Concurrent            (readMVar)
import           Data.Functor                  (void)
import           Network.GRPC.HighLevel.Client
import           Z.IO.Network                  (SocketAddr)

import           HStream.Client.Action
import           HStream.Client.Gadget
import           HStream.Client.Types          (HStreamSqlContext (..))
import           HStream.Client.Utils
import           HStream.SQL
import           HStream.Utils                 (Format, getServerResp)

executeShowPlan :: HStreamSqlContext -> ShowObject -> IO ()
executeShowPlan ctx showObject =
  case showObject of
    SStreams    -> execute_ ctx listStreams
    SViews      -> execute_ ctx listViews
    SQueries    -> execute_ ctx listQueries
    SConnectors -> execute_ ctx listConnectors

execute_ :: Format a => HStreamSqlContext
  -> Action a -> IO ()
execute_ ctx@HStreamSqlContext{..} action = do
  addr <- readMVar currentServer
  void $ executeWithAddr_ ctx addr action printResult

execute :: HStreamSqlContext -> Action a -> IO (Maybe a)
execute ctx@HStreamSqlContext{..} action = do
  addr <- readMVar currentServer
  executeWithAddr ctx addr action ((Just <$>) . getServerResp)

executeWithAddr
  :: HStreamSqlContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO (Maybe a))
  -> IO (Maybe a)
executeWithAddr ctx addr action handleOKResp = do
  getInfoWithAddr ctx addr action handleOKResp

executeWithAddr_
  :: HStreamSqlContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO ())
  -> IO ()
executeWithAddr_ ctx addr action handleOKResp = do
  void $ getInfoWithAddr ctx addr action (\x -> handleOKResp x >> return Nothing)
