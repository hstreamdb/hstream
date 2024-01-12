module HStream.Kafka.Server.Config
  ( ServerOpts (..)
  , CliOptions (..)
  , cliOptionsParser

  , runServerConfig
  , runServerFromCliOpts

  , FileLoggerSettings (..)
  , MetaStoreAddr (..)
  , AdvertisedListeners
  , ListenersSecurityProtocolMap
  , TlsConfig (..)
  , SecurityProtocolMap, defaultProtocolMap
  , advertisedListenersToPB
  , StorageOptions (..)
  , ExperimentalFeature (..)
  ) where

import           Control.Exception                    (throwIO)
import qualified Data.Text                            as Text
import           Data.Yaml                            (ParseException (..),
                                                       decodeFileThrow,
                                                       parseEither)
import           System.Directory                     (makeAbsolute)

import           HStream.Common.Types                 (getHStreamVersion)
import           HStream.Kafka.Server.Config.FromCli
import           HStream.Kafka.Server.Config.FromJson
import           HStream.Kafka.Server.Config.Types
import qualified HStream.Server.HStreamApi            as A


runServerConfig :: [String] -> (ServerOpts -> IO ()) -> IO ()
runServerConfig args f = do
  serverCli <- runServerCli args
  case serverCli of
    ShowVersion -> showVersion
    Cli cliOpts -> getConfig cliOpts >>= f

runServerFromCliOpts :: CliOptions -> (ServerOpts -> IO ()) -> IO ()
runServerFromCliOpts cliOpts f = getConfig cliOpts >>= f

getConfig :: CliOptions -> IO ServerOpts
getConfig opts@CliOptions{..} = do
  path <- makeAbsolute cliConfigPath
  jsonCfg <- decodeFileThrow path
  case parseEither (parseJSONToOptions opts) jsonCfg of
    Left err  -> throwIO (AesonException err)
    Right cfg -> return cfg

showVersion :: IO ()
showVersion = do
  A.HStreamVersion{..} <- getHStreamVersion
  putStrLn $ "version: " <> Text.unpack hstreamVersionVersion
          <> " (" <> Text.unpack hstreamVersionCommit <> ")"
