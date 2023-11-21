{-# LANGUAGE GADTs #-}

module HStream.Kafka.Server.Security.SASL where

import           Control.Exception                 (throwIO, try)
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State               (StateT)
import qualified Control.Monad.State               as State
import           Data.ByteString                   (ByteString)
import qualified Data.ByteString.Char8             as BSC
import qualified Data.ByteString.Lazy              as BSL
import qualified Data.ByteString.UTF8              as UTF8
import qualified Data.List                         as L
import qualified Data.Map.Strict                   as Map
import qualified Data.Text                         as T
import qualified Network.SASL.SASL                 as SASL

import           HStream.Kafka.Network.IO
import           HStream.Kafka.Server.Config.Types (SaslMechanismOption (..),
                                                    SaslOptions (..),
                                                    ServerOpts (..))
import           HStream.Kafka.Server.Types        (ServerContext (..))
import qualified HStream.Logger                    as Log
import           Kafka.Protocol.Encoding
import qualified Kafka.Protocol.Error              as K
import           Kafka.Protocol.Message

-------------------------------------------------------------------------------
serverSupportedMechanismNames :: [T.Text]
serverSupportedMechanismNames = ["PLAIN", "SCRAM-SHA-256"]

-------------------------------------------------------------------------------
class SaslAuthenticator a where
  runSaslAuthenticator :: a
                       -> ServerContext
                       -- ^ The server context.
                       -> StateT ByteString IO (RequestHeader, ByteString)
                       -- ^ The "receive" method, which returns the header and body of the next kafka request.
                       --   The "state" is the left bytes in TCP channel.
                       -> (BSL.ByteString -> IO ())
                       -- ^ The "send" method. Note that the 'ByteString' includes 'ResponseHeader'!
                       -> StateT ByteString IO ()
                       -- ^ Represents how to perform "authenticate" process. The "state" is the left bytes in TCP channel.


data SomeAuthenticator where
  SomeAuthenticator :: forall a. SaslAuthenticator a => a -> SomeAuthenticator

withSomeAuthenticator :: SomeAuthenticator -> (forall a. SaslAuthenticator a => a -> r) -> r
withSomeAuthenticator (SomeAuthenticator a) f = f a

-------------------------------------------------------------------------------
-- PLAIN mechanism
-------------------------------------------------------------------------------
newtype SaslPlain = SaslPlain
  { saslPlainDoAuth :: ServerContext
                    -> StateT ByteString IO (RequestHeader, ByteString)
                    -> (BSL.ByteString -> IO ())
                    -> StateT ByteString IO ()
  }

saslPlainAuthenticator :: SaslPlain
saslPlainAuthenticator = SaslPlain $ \sc recv send -> do
  i <- State.get -- the left bytes in TCP channel
  l_e <- liftIO . try $ SASL.withGSaslContext $ \saslCtx -> do
    SASL.setCallback saslCtx (saslPlainCallback sc)
    SASL.withServerSession saslCtx "PLAIN" (saslPlainSession i recv send)
  case l_e of
    Left (e :: SASL.GSaslException) -> error (show e) -- FIXME: throw proper exception
    Right l                         -> State.put l
  where
    -- TODO: Use 'MonadState m' instead of rigid 'IO' for monadic operations.
    --       This requires the support of lib 'gsasl-hs'.
    saslPlainSession i recv_ send_ saslSession = do
      ((reqHeader,reqBs), i') <- State.runStateT recv_ i
      Log.debug $ "[SASL PLAIN] recv: " <> Log.build reqBs
      (SaslAuthenticateRequest{..}, _) <- runGet' reqBs
      SASL.serverStep saslSession authBytes >>= \case
        (serverMsg, SASL.GSaslDone) -> do
          Log.debug $ "[SASL PLAIN] complete!"
          let resp = SaslAuthenticateResponse K.NONE mempty serverMsg
          send_ (packKafkaMsgBs reqHeader resp)
          return i'
        (serverMsg, SASL.GSaslMore) -> do
          Log.warning $ "[SASL PLAIN] I need more... But why? S: " <> Log.build serverMsg
          let resp = SaslAuthenticateResponse K.NONE mempty serverMsg
          send_ (packKafkaMsgBs reqHeader resp)
          throwIO (SASL.GSaslException SASL.AUTHENTICATION_ERROR)

instance SaslAuthenticator SaslPlain where
  runSaslAuthenticator SaslPlain{..} = saslPlainDoAuth

saslPlainCallback :: ServerContext -> SASL.Property -> SASL.GSaslSession -> IO SASL.GSaslErrCode
saslPlainCallback _ SASL.PropertyAuthid gsaslSession = do
  authID <- SASL.getProperty gsaslSession SASL.PropertyAuthid
  Log.debug $ "SASL PLAIN invoke callback with PropertyAuthid. I got " <> Log.buildString' authID
  return SASL.GSASL_OK
saslPlainCallback _ SASL.PropertyPassword gsaslSession = do
  password <- SASL.getProperty gsaslSession SASL.PropertyPassword
  Log.debug $ "SASL PLAIN invoke callback with PropertyPassword. I got" <> Log.buildString' password
  return SASL.GSASL_OK
saslPlainCallback _ SASL.PropertyAuthzid gsaslSession = do
  authzID <- SASL.getProperty gsaslSession SASL.PropertyAuthzid
  Log.debug $ "SASL PLAIN invoke callback with PropertyAuthzid. I got" <> Log.buildString' authzID
  return SASL.GSASL_OK
saslPlainCallback sc SASL.PropertyValidateSimple gsaslSession = do
  Log.debug $ "SASL PLAIN invoke callback with PropertyValidateSimple..."
  authID   <- SASL.getProperty gsaslSession SASL.PropertyAuthid
  password <- SASL.getProperty gsaslSession SASL.PropertyPassword
  let ServerOpts{..} = serverOpts sc
  let m = do
        -- FIXME: capital insensitive
       SaslOptions{..} <- join (snd <$> Map.lookup "sasl_plaintext" _securityProtocolMap)
       let findPlainMechanism = L.find (\case
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
    Just _  -> return SASL.GSASL_OK
    Nothing -> return SASL.AUTHENTICATION_ERROR
saslPlainCallback _ prop _ = do
  Log.warning $ "SASL PLAIN invoke callback with " <> Log.buildString' prop <> ". But I do not know how to handle it..."
  return SASL.NO_CALLBACK

-------------------------------------------------------------------------------
-- SCRAM-SHA-256 mechanism
-------------------------------------------------------------------------------
newtype SaslScramSha256 = SaslScramSha256
  { saslScramSha256DoAuth :: ServerContext
                          -> StateT ByteString IO (RequestHeader, ByteString)
                          -> (BSL.ByteString -> IO ())
                          -> StateT ByteString IO ()
  }

saslScramSha256Authenticator :: SaslScramSha256
saslScramSha256Authenticator = SaslScramSha256 $ \sc recv send -> do
  i <- State.get -- the left bytes in TCP channel
  l_e <- liftIO . try $ SASL.withGSaslContext $ \saslCtx -> do
    SASL.setCallback saslCtx (saslScramSha256Callback sc)
    SASL.withServerSession saslCtx "SCRAM-SHA-256" (saslScramSha256Session i recv send)
  case l_e of
    Left (e :: SASL.GSaslException) -> error (show e) -- FIXME: throw proper exception
    Right l                         -> State.put l
  where
    -- TODO: Use 'MonadState m' instead of rigid 'IO' for monadic operations.
    --       This requires the support of lib 'gsasl-hs'.
    saslScramSha256Session i recv_ send_ saslSession = do
      ((reqHeader,reqBs), i') <- State.runStateT recv_ i
      Log.debug $ "[SASL SCRAM-SHA-256] recv: " <> Log.build reqBs
      (SaslAuthenticateRequest{..}, _) <- runGet' reqBs
      Log.debug $ "[SASL SCRAM-SHA-256] C: " <> Log.buildString (UTF8.toString authBytes)
      SASL.serverStep saslSession authBytes >>= \case
        (serverMsg, SASL.GSaslDone) -> do
          Log.debug $ "[SASL SCRAM-SHA256] complete!"
          let resp = SaslAuthenticateResponse K.NONE mempty serverMsg
          send_ (packKafkaMsgBs reqHeader resp)
          return i'
        (serverMsg, SASL.GSaslMore) -> do
          Log.debug $ "[SASL SCRAM-SHA-256] S: " <> Log.buildString (UTF8.toString serverMsg)
          let resp = SaslAuthenticateResponse K.NONE mempty serverMsg
          send_ (packKafkaMsgBs reqHeader resp)
          saslScramSha256Session i' recv_ send_ saslSession

instance SaslAuthenticator SaslScramSha256 where
  runSaslAuthenticator SaslScramSha256{..} = saslScramSha256DoAuth

saslScramSha256Callback :: ServerContext -> SASL.Property -> SASL.GSaslSession -> IO SASL.GSaslErrCode
saslScramSha256Callback sc prop saslSession = case prop of
  SASL.PropertyAuthid -> do
    authID <- SASL.getProperty saslSession SASL.PropertyAuthid
    Log.debug $ "SASL SCRAM-SHA-256 invoke callback with PropertyAuthID. I got " <> Log.buildString' authID
    return SASL.GSASL_OK
  SASL.PropertyPassword -> do
    authID <- SASL.getProperty saslSession SASL.PropertyAuthid
    let ServerOpts{..} = serverOpts sc
    let m = do
          -- FIXME: capital insensitive
          SaslOptions{..} <- join (snd <$> Map.lookup "sasl_plaintext" _securityProtocolMap)
          let findScramSha256Mechanism =
                L.find (\case
                          SaslScramSha256Option _ -> True
                          _                       -> False
                       )
          (SaslScramSha256Option tups) <- findScramSha256Mechanism saslMechanisms
          authID'       <- authID
          (_, password) <- L.find (\(a, _) -> BSC.pack a == authID') tups
          return password
    case m of
      Just pw -> do
        SASL.setProperty saslSession SASL.PropertyPassword (BSC.pack pw)
        return SASL.GSASL_OK
      Nothing -> return SASL.AUTHENTICATION_ERROR
  SASL.PropertyScramSalt -> do
    let salt = SASL.toBase64 "salt" -- FIXME: generate salt
    SASL.setProperty saslSession SASL.PropertyScramSalt salt
    Log.debug $ "SASL SCRAM-SHA-256 invoke callback with PropertyScramSalt. I got " <> Log.buildString' salt
    return SASL.GSASL_OK
  SASL.PropertyScramIter -> do
    let iter = "4096" -- FIXME: customize iter
    SASL.setProperty saslSession SASL.PropertyScramIter iter
    Log.debug $ "SASL SCRAM-SHA-256 invoke callback with PropertyScramIter. I got " <> Log.buildString' iter
    return SASL.GSASL_OK
  SASL.PropertyCBTlsUnique -> do
  -- FIXME: support tls unique
    let tlsUnique = SASL.toBase64 ""
    Log.debug $ "SASL SCRAM-SHA-256 invoke callback with PropertyCBTlsUnique. I got " <> Log.buildString' tlsUnique
    return SASL.GSASL_OK
  SASL.PropertyScramServerKey -> do
    (Just pw  ) <- SASL.getProperty saslSession SASL.PropertyPassword
    (Just iter) <- SASL.getProperty saslSession SASL.PropertyScramIter
    (Just salt) <- SASL.getProperty saslSession SASL.PropertyScramSalt
    let n_iter = read (UTF8.toString iter)
    let (_,_,serverKey,_) = SASL.scramSecretsFromPasswordSha256 pw n_iter (SASL.fromBase64 salt)
    SASL.setProperty saslSession SASL.PropertyScramServerKey (SASL.toBase64 serverKey)
    Log.debug $ "SASL SCRAM-SHA-256 invoke callback with PropertyScramServerKey. I got " <> Log.buildString (UTF8.toString $ SASL.toBase64 serverKey)
    return SASL.GSASL_OK
  SASL.PropertyScramStoredKey -> do
    (Just pw  ) <- SASL.getProperty saslSession SASL.PropertyPassword
    (Just iter) <- SASL.getProperty saslSession SASL.PropertyScramIter
    (Just salt) <- SASL.getProperty saslSession SASL.PropertyScramSalt
    let n_iter = read (UTF8.toString iter)
    let (_,_,_,storedKey) = SASL.scramSecretsFromPasswordSha256 pw n_iter (SASL.fromBase64 salt)
    SASL.setProperty saslSession SASL.PropertyScramStoredKey (SASL.toBase64 storedKey)
    Log.debug $ "SASL SCRAM-SHA-256 invoke callback with PropertyScramStoredKey. I got " <> Log.buildString (UTF8.toString $ SASL.toBase64 storedKey)
    return SASL.GSASL_OK
  _ -> do
    Log.warning $ "SASL SCRAM-SHA-256 invoke callback with " <> Log.buildString' prop <> ". But I do not know how to handle it..."
    return SASL.NO_CALLBACK
