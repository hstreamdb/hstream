module HStream.Kafka.Server.Handler.Security
  ( handleSaslHandshake
  , handleSaslAuthenticate

  , handleAfterAuthSaslHandshakeV0
  , handleAfterAuthSaslHandshakeV1
  , handleAfterAuthSaslAuthenticateV0
  ) where

import           Control.Concurrent.MVar
import           Control.Monad                     (join)
import           Control.Monad.IO.Class            (liftIO)
import           Data.ByteString                   (ByteString)
import qualified Data.ByteString.Char8             as BSC
import qualified Data.List                         as L
import qualified Data.Map.Strict                   as Map
import qualified Data.Text                         as T
import qualified Data.Text.Encoding                as T
import qualified Data.Vector                       as V
import           Network.Protocol.SASL.GNU

import           HStream.Kafka.Server.Config.Types (SaslMechanismOption (..),
                                                    SaslOptions (..),
                                                    ServerOpts (..))
import           HStream.Kafka.Server.Types        (ServerContext (..))
import qualified HStream.Logger                    as Log
import qualified Kafka.Protocol.Encoding           as K
import qualified Kafka.Protocol.Error              as K
import qualified Kafka.Protocol.Message            as K
import qualified Kafka.Protocol.Service            as K

-------------------------------------------------------------------------------
saslPlainCallback :: ServerContext -> Property -> Session Progress
saslPlainCallback sc PropertyAuthID = do
  authID <- getProperty PropertyAuthID
  liftIO . Log.debug $ "SASL PLAIN invoke callback with PropertyAuthID. I got " <> Log.buildString' authID
  return Complete
saslPlainCallback sc PropertyPassword = do
  password <- getProperty PropertyPassword
  liftIO . Log.debug $ "SASL PLAIN invoke callback with PropertyPassword. I got" <> Log.buildString' password
  return Complete
saslPlainCallback sc PropertyAuthzID = do
  authzID <- getProperty PropertyAuthzID
  liftIO . Log.debug $ "SASL PLAIN invoke callback with PropertyAuthzID. I got" <> Log.buildString' authzID
  return Complete
saslPlainCallback sc ValidateSimple = do
  liftIO . Log.debug $ "SASL PLAIN invoke callback with ValidateSimple..."
  authID   <- getProperty PropertyAuthID
  password <- getProperty PropertyPassword
  let ServerOpts{..} = serverOpts sc
  let m = do
        -- FIXME: capital insensitive
       SaslOptions{..} <- join (snd <$> Map.lookup "sasl_plaintext" _securityProtocolMap)
       -- FIXME: more SASL mechanisms
       let findPlainMechanism = L.find (\mech -> case mech of
                                           SaslPlainOption _ -> True
                                           _                 -> False
                                       )
       (SaslPlainOption tups) <- findPlainMechanism saslMechanisms
       authID'   <- authID
       password' <- password
       (_, realPassword) <- L.find (\(a, _) -> BSC.pack a == authID') tups
       if password' == BSC.pack realPassword
         then Just ()
         else Nothing
  case m of
    Just _  -> return Complete
    Nothing -> throw AuthenticationError
saslPlainCallback sc prop = do
  liftIO . Log.warning $ "SASL PLAIN invoke callback with " <> Log.buildString' prop <> ". But I do not know how to handle it..."
  return Complete

saslPlainSession :: ByteString -> Session ByteString
saslPlainSession input = do
  mechanism <- mechanismName
  liftIO . Log.debug $ "SASL: I am using " <> Log.buildString' mechanism
  liftIO . Log.debug $ "SASL PLAIN: I got C: " <> Log.build input
  (serverMsg, prog) <- step input
  case prog of
    Complete  -> do
      liftIO . Log.debug $ "SASL PLAIN: Complete. S: " <> Log.build serverMsg
      return serverMsg
    NeedsMore -> do
      liftIO . Log.warning $ "SASL PLAIN: I need more... But why? S: " <> Log.build serverMsg
      throw AuthenticationError

saslPlain :: ServerContext -> ByteString -> SASL (Either Error ByteString)
saslPlain sc input = do
  setCallback (saslPlainCallback sc)
  runServer (Mechanism "PLAIN") (saslPlainSession input)

-------------------------------------------------------------------------------
handleSaslHandshake :: ServerContext -> K.RequestContext -> K.SaslHandshakeRequest -> IO K.SaslHandshakeResponse
handleSaslHandshake _ _ K.SaslHandshakeRequest{..} = do
  let reqMechanism = Mechanism (T.encodeUtf8 mechanism)
  isMechSupported <- runSASL (serverSupports reqMechanism)
  if isMechSupported then do
    Log.debug $ "SASL: client requests " <> Log.buildString' mechanism
    return $ K.SaslHandshakeResponse K.NONE (K.KaArray $ Just (V.singleton "PLAIN"))
    else do
    Log.warning $ "SASL: client requests " <> Log.buildString' mechanism <> ", but I do not support it..."
    return $ K.SaslHandshakeResponse K.UNSUPPORTED_SASL_MECHANISM (K.KaArray $ Just (V.singleton "PLAIN"))

handleSaslAuthenticate :: ServerContext -> K.RequestContext -> K.SaslAuthenticateRequest -> IO K.SaslAuthenticateResponse
handleSaslAuthenticate sc reqCtx K.SaslAuthenticateRequest{..} = do
  respBytes_e <- runSASL (saslPlain sc authBytes)
  case respBytes_e of
    Left err -> do
      Log.warning $ "SASL: auth failed, " <> Log.buildString' err
      putMVar (K.clientAuthDone reqCtx) False
      return $ K.SaslAuthenticateResponse K.SASL_AUTHENTICATION_FAILED (Just . T.pack $ show err) mempty
    Right respBytes -> do
      putMVar (K.clientAuthDone reqCtx) True
      return $ K.SaslAuthenticateResponse K.NONE mempty respBytes

-------------------------------------------------------------------------------
handleAfterAuthSaslHandshakeV0 :: ServerContext -> K.RequestContext -> K.SaslHandshakeRequestV0 -> IO K.SaslHandshakeResponseV0
handleAfterAuthSaslHandshakeV0 _ _ _ = do
  Log.warning $ "SASL: client requests handshake after successful authentication."
  return $ K.SaslHandshakeResponseV0 K.ILLEGAL_SASL_STATE (K.KaArray $ Just (V.singleton "PLAIN"))

handleAfterAuthSaslHandshakeV1 :: ServerContext -> K.RequestContext -> K.SaslHandshakeRequestV1 -> IO K.SaslHandshakeResponseV1
handleAfterAuthSaslHandshakeV1 = handleAfterAuthSaslHandshakeV0

handleAfterAuthSaslAuthenticateV0 :: ServerContext -> K.RequestContext -> K.SaslAuthenticateRequestV0 -> IO K.SaslAuthenticateResponseV0
handleAfterAuthSaslAuthenticateV0 _ _ _ = do
  Log.warning $ "SASL: client requests authenticate after successful authentication."
  return $ K.SaslAuthenticateResponseV0 K.ILLEGAL_SASL_STATE
                                        (Just "SaslAuthenticate request received after successful authentication")
                                        mempty
