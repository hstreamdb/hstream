{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Network.Security
  ( SaslState (..)
  , authenticate
  ) where

import           Control.Monad.IO.Class             (liftIO)
import           Control.Monad.State                (StateT)
import qualified Control.Monad.State                as State
import           Data.ByteString                    (ByteString)
import qualified Data.ByteString.Lazy               as BSL
import           Data.Maybe
import           Data.Text                          (Text)

import           HStream.Kafka.Server.Security.SASL
import           HStream.Kafka.Server.Types         (ServerContext (..))
import qualified HStream.Logger                     as Log
import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Message

data SaslState
  = SaslStateHandshakeOrVersions
  | SaslStateHandshake
  | SaslStateAuthenticate
  | SaslStateComplete
  | SaslStateFailed
  deriving (Eq, Show)

authenticate :: ServerContext
             -> (RequestHeader -> ByteString -> IO BSL.ByteString)
             -> StateT ByteString IO (Maybe (RequestHeader, ByteString))
             -> (BSL.ByteString -> IO ())
             -> SaslState
             -> Maybe SomeAuthenticator
             -> StateT ByteString IO SaslState
authenticate sc rh recv send saslState authenticator_m = do
  liftIO . Log.debug $ "[SASL] current state is " <> Log.build (show saslState)
  recv >>= \case
    Nothing -> if saslState == SaslStateComplete
                then return saslState
                else return SaslStateFailed
    Just (reqHeader@RequestHeader{..}, reqBs) -> do
      let headerBs = runPut reqHeader
          prevBs   = runPut (headerBs <> reqBs)
      case saslState of
        SaslStateHandshakeOrVersions -> case requestApiKey of
          ApiKey 17 -> do -- Handsake
            respBs <- liftIO $ rh reqHeader reqBs -- handle handshake
            liftIO $ send respBs
            (SaslHandshakeRequest{..}, _) <- liftIO $ runGet' reqBs
            authenticate sc rh recv send SaslStateAuthenticate (chooseAuthenticator mechanism)
          ApiKey 18 -> do -- ApiVersions
            respBs <- liftIO $ rh reqHeader reqBs -- handle apiversions
            liftIO $ send respBs
            authenticate sc rh recv send SaslStateHandshake authenticator_m
          ApiKey _  -> return SaslStateFailed

        SaslStateHandshake -> case requestApiKey of
          ApiKey 17 -> do -- Handsake
            respBs <- liftIO $ rh reqHeader reqBs -- handle handshake
            liftIO $ send respBs
            (SaslHandshakeRequest{..}, _) <- liftIO $ runGet' reqBs
            authenticate sc rh recv send SaslStateAuthenticate (chooseAuthenticator mechanism)
          ApiKey _  -> return SaslStateFailed

        SaslStateAuthenticate -> do
          case requestApiKey of
            ApiKey 36 -> do -- SaslAuthenticate
              case authenticator_m of
                Nothing -> return SaslStateFailed
                Just authenticator -> do
                  let recv' = fromJust <$> recv -- FIXME: fromJust
                      send' = send
                  withSomeAuthenticator authenticator $ \a -> do
                    State.put prevBs -- WARNING: Important! Authenticator will perform a `recv` first
                    runSaslAuthenticator a sc recv' send'
                  return SaslStateComplete
            ApiKey _  -> do
              return SaslStateFailed
        x -> do
          liftIO . Log.warning $ "[SASL] Unknown state: " <> Log.build (show x)
          return x

-- FIXME: case insensitive?
-- | Choose a proper authenticator based on the mechanism name.
chooseAuthenticator :: Text -> Maybe SomeAuthenticator
chooseAuthenticator mechName
  | mechName == "PLAIN"         = Just $ SomeAuthenticator saslPlainAuthenticator
  | mechName == "SCRAM-SHA-256" = Just $ SomeAuthenticator saslScramSha256Authenticator
  | otherwise                   = Nothing
