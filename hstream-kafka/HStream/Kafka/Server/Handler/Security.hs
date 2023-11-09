module HStream.Kafka.Server.Handler.Security
  ( handleSaslHandshake
  , handleSaslAuthenticate

  , handleAfterAuthSaslHandshakeV0
  , handleAfterAuthSaslHandshakeV1
  , handleAfterAuthSaslAuthenticateV0
  ) where

import           Control.Concurrent.MVar
import           Control.Monad.IO.Class     (liftIO)
import           Data.ByteString            (ByteString)
import qualified Data.Text                  as T
import qualified Data.Text.Encoding         as T
import qualified Data.Vector                as V
import           Network.Protocol.SASL.GNU

import           HStream.Kafka.Server.Types (ServerContext (..))
import qualified HStream.Logger             as Log
import qualified Kafka.Protocol.Encoding    as K
import qualified Kafka.Protocol.Error       as K
import qualified Kafka.Protocol.Message     as K
import qualified Kafka.Protocol.Service     as K

-------------------------------------------------------------------------------
saslPlainCallback :: Property -> Session Progress
saslPlainCallback PropertyAuthID = do
  authID <- getProperty PropertyAuthID
  liftIO . Log.debug $ "SASL PLAIN invoke callback with PropertyAuthID. I got " <> Log.buildString' authID
  return Complete
saslPlainCallback PropertyPassword = do
  password <- getProperty PropertyPassword
  liftIO . Log.debug $ "SASL PLAIN invoke callback with PropertyPassword. I got" <> Log.buildString' password
  return Complete
saslPlainCallback PropertyAuthzID = do
  authzID <- getProperty PropertyAuthzID
  liftIO . Log.debug $ "SASL PLAIN invoke callback with PropertyAuthzID. I got" <> Log.buildString' authzID
  return Complete
saslPlainCallback ValidateSimple = do
  liftIO . Log.debug $ "SASL PLAIN invoke callback with ValidateSimple..."
  authID   <- getProperty PropertyAuthID
  password <- getProperty PropertyPassword
  if authID == Just "admin" && password == Just "passwd" -- FIXME: do actual check
    then return Complete
    else throw AuthenticationError
saslPlainCallback prop = do
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

saslPlain :: ByteString -> SASL (Either Error ByteString)
saslPlain input = do
  setCallback saslPlainCallback
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
handleSaslAuthenticate _ reqCtx K.SaslAuthenticateRequest{..} = do
  respBytes_e <- runSASL (saslPlain authBytes)
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
handleAfterAuthSaslHandshakeV0 _ _ _ = return $ K.SaslHandshakeResponseV0 K.ILLEGAL_SASL_STATE (K.KaArray Nothing)

handleAfterAuthSaslHandshakeV1 :: ServerContext -> K.RequestContext -> K.SaslHandshakeRequestV1 -> IO K.SaslHandshakeResponseV1
handleAfterAuthSaslHandshakeV1 = handleAfterAuthSaslHandshakeV0

handleAfterAuthSaslAuthenticateV0 :: ServerContext -> K.RequestContext -> K.SaslAuthenticateRequestV0 -> IO K.SaslAuthenticateResponseV0
handleAfterAuthSaslAuthenticateV0 _ _ _ =
  return $ K.SaslAuthenticateResponseV0 K.ILLEGAL_SASL_STATE
                                        (Just "SaslAuthenticate request received after successful authentication")
                                        mempty
