module HStream.Kafka.Server.Handler.Security
  ( handleSaslHandshake
  , handleSaslHandshakeAfterAuth
  , handleSaslAuthenticate
  ) where

import qualified Data.Vector                        as V

import           HStream.Kafka.Server.Security.SASL (serverSupportedMechanismNames)
import           HStream.Kafka.Server.Types         (ServerContext (..))
import qualified HStream.Logger                     as Log
import qualified Kafka.Protocol.Encoding            as K
import qualified Kafka.Protocol.Error               as K
import qualified Kafka.Protocol.Message             as K
import qualified Kafka.Protocol.Service             as K

-------------------------------------------------------------------------------

handleSaslHandshake :: ServerContext -> K.RequestContext -> K.SaslHandshakeRequest -> IO K.SaslHandshakeResponse
handleSaslHandshake _ _ K.SaslHandshakeRequest{..} = do
  -- isLibSupported <- runSASL (serverSupports reqMechanism)
  -- FIXME: check if lib supports the mechanism
  if mechanism `elem` serverSupportedMechanismNames then do -- FIXME: case insensitive?
    Log.debug $ "SASL: client requests " <> Log.buildString' mechanism
    return $ K.SaslHandshakeResponse K.NONE (K.KaArray $ Just (V.singleton mechanism))
    else do
    Log.warning $ "SASL: client requests " <> Log.buildString' mechanism <> ", but I do not support it..."
    return $ K.SaslHandshakeResponse K.UNSUPPORTED_SASL_MECHANISM (K.KaArray $ Just (V.fromList serverSupportedMechanismNames))

handleSaslHandshakeAfterAuth
  :: ServerContext
  -> K.RequestContext
  -> K.SaslHandshakeRequest
  -> IO K.SaslHandshakeResponse
handleSaslHandshakeAfterAuth _ _ _ = do
  Log.warning $ "SASL: client requests handshake after successful authentication."
  return $ K.SaslHandshakeResponse K.ILLEGAL_SASL_STATE (K.KaArray $ Just (V.fromList serverSupportedMechanismNames))

handleSaslAuthenticate :: ServerContext -> K.RequestContext -> K.SaslAuthenticateRequest -> IO K.SaslAuthenticateResponse
handleSaslAuthenticate _ _ _ = do
  Log.warning $ "SASL: client requests authenticate after successful authentication."
  return $ K.SaslAuthenticateResponse K.ILLEGAL_SASL_STATE
                                      (Just "SaslAuthenticate request received after successful authentication")
                                      mempty
