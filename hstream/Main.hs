{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import qualified Colog
import           Control.Exception      (SomeException, bracket)
import           Control.Monad          (unless)
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Reader   (ask, runReaderT)
import qualified Data.Text              as Text
import qualified Data.Vector            as V
import           Data.Yaml.Config       (loadYamlSettingsArgs, useEnv)
import qualified Network.HESP           as HESP
import           Network.Socket         (Socket)
import           System.Directory       (createDirectoryIfMissing)

import qualified HStream.LogStore.Base  as HStream
import           HStream.Server         (App, DatabaseSetting (..), Env (..),
                                         ServerSettings (..))
import qualified HStream.Server         as HStream

main :: IO ()
main = do
  settings@ServerSettings{..} <- loadYamlSettingsArgs [] useEnv
  status <- HStream.newServerStatus
  let env = Env { serverSettings = settings
                , serverStatus   = status
                }
  let DatabaseSetting{..} = databaseSetting
  -- create db directory (and all parent directories) if not exists.
  createDirectoryIfMissing True dbPath
  -- run server
  bracket
    (HStream.initialize $
     HStream.Config dbPath writeBufferSize enableDBStats statsPeriodSec
                     partitionInterval partitionFileNumLimit maxOpenDBs)
    (runReaderT HStream.shutDown)
    (HStream.runApp env . runServer)

runServer :: HStream.Context -> App ()
runServer ctx = do
  Env{..} <- ask
  let h = serverHost serverSettings
      p = show (serverPort serverSettings)
  Colog.logInfo "------------------------- HStream -------------------------"
  Colog.logInfo $ "Listening on "
    <> Text.pack (serverHost serverSettings) <> ":" <> Text.pack p
  HESP.runTCPServerG' h p setSocketOptions clean $ \(sock, _) -> go sock
  where
    go :: Socket -> App ()
    go sock = do
      msgs <- HESP.recvMsgs sock 1024
      unless (V.null msgs) $ do
        Colog.logDebug $ "Received: " <> Text.pack (show msgs)
        mapM_ (HStream.onRecvMsg sock ctx) msgs
        go sock

setSocketOptions :: Socket -> IO ()
setSocketOptions = HESP.setDefaultServerSO

clean :: (Either SomeException a, Socket) -> App ()
clean (Left e, sock)  = Colog.logException e >> onCloseSocket sock
clean (Right _, sock) = onCloseSocket sock

onCloseSocket :: Socket -> App ()
onCloseSocket sock = do
  env <- ask
  Colog.logInfo $ "Clean server status..."
  let status = serverStatus env
  liftIO $ do HStream.deleteClientsBySocket sock status
              -- FIXME: use gracefulClose ?
              HESP.close sock
