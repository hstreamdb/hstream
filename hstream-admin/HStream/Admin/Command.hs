module HStream.Admin.Command
  ( withAdminClient
  , withAdminClient'
  , sendAdminCommand
  , sendAdminCommand'
  , formatCommandResponse
  , module HStream.Admin.Command.ServerSql
  ) where

import           Control.Monad                    ((<=<))
import qualified Data.Aeson                       as Aeson
import           Data.ByteString                  (ByteString)
import qualified Data.HashMap.Strict              as HMap
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Text                        as Text
import qualified Data.Text.Encoding               as Text
import qualified Data.Vector                      as V
import qualified Network.GRPC.HighLevel.Client    as GRPC
import           Network.GRPC.HighLevel.Generated (withGRPCClient)
import           Network.Socket                   (PortNumber)
import qualified Z.Data.CBytes                    as CBytes
import qualified Z.Foreign                        as Z

import           HStream.Admin.Command.ServerSql
import           HStream.Admin.Types
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Utils                    as U

formatCommandResponse :: Text -> IO String
formatCommandResponse resp =
  case Aeson.eitherDecodeStrict (Text.encodeUtf8 resp) of
    Left errmsg              -> pure $ "Decode json error: " <> errmsg
    Right (v :: Aeson.Value) -> parseVal v
  where
    parseVal (Aeson.Object obj) = do
      let m_type = HMap.lookup "type" obj
          m_content = HMap.lookup "content" obj
      case m_type of
        Just (Aeson.String "table") -> extractJsonTable m_content
        Just (Aeson.String "plain") -> pure $ U.fillWithJsonString' "content" obj
        _                           -> pure "No such \"type\""
    parseVal x  = pure $ "Expecting obj value, but got " <> show x

    extractJsonTable (Just (Aeson.Object x)) = do
      let e_val = do hs <- formatTableHeader x
                     rs <- formatTableRows x
                     pure (hs, rs)
      case e_val of
        Left msg             -> pure msg
        Right (header, rows) -> U.defaultShowTableIO' header rows
    extractJsonTable (Just x) = pure $ "Expecting obj value, but got " <> show x
    extractJsonTable Nothing = pure "No such \"content\" key"

formatTableHeader :: Aeson.Object -> Either String [String]
formatTableHeader obj = format $ HMap.lookup "headers" obj
  where
    format (Just (Aeson.Array xs)) = Right . V.toList . V.map showTableValue $ xs
    format (Just x) = Left $ "Expecting array value, but got " <> show x
    format Nothing  = Left "No such \"headers\" key"

formatTableRows :: Aeson.Object -> Either String [[String]]
formatTableRows obj = format $ HMap.lookup "rows" obj
  where
    format (Just (Aeson.Array xs)) = Right . V.toList . V.map extractArray $ xs
    format (Just x) = Left $ "Expecting array value, but got " <> show x
    format Nothing  = Left "No such \"rows\" key"
    extractArray :: Aeson.Value -> [String]
    extractArray (Aeson.Array xs) = V.toList $ V.map showTableValue xs
    extractArray x = ["Expecting array value, but got " <> show x]

showTableValue :: Aeson.Value -> String
showTableValue (Aeson.String x) = Text.unpack x
showTableValue (Aeson.Number x) = show x
showTableValue (Aeson.Array x)  = show x
showTableValue (Aeson.Bool x)   = show x
showTableValue Aeson.Null       = "NULL"
showTableValue (Aeson.Object x) = show x

-------------------------------------------------------------------------------

sendAdminCommand :: Text -> U.HStreamClientApi -> IO Text
sendAdminCommand = sendAdminCommand' 10
{-# INLINABLE sendAdminCommand #-}

sendAdminCommand' :: GRPC.TimeoutSeconds -> Text -> U.HStreamClientApi -> IO Text
sendAdminCommand' timeout command api = do
  let comReq = API.AdminCommandRequest command
      req = GRPC.ClientNormalRequest comReq timeout $ GRPC.MetadataMap Map.empty
  fmap API.adminCommandResponseResult . U.getServerResp =<< API.hstreamApiSendAdminCommand api req
{-# INLINABLE sendAdminCommand' #-}

withAdminClient
  :: CliOpts
  -> (U.HStreamClientApi -> IO a)
  -> IO a
withAdminClient CliOpts{..} = withAdminClient' host port
  where
    host = Z.toByteString . CBytes.toBytes $ optServerHost
    port = fromIntegral optServerPort

withAdminClient'
  :: ByteString
  -> PortNumber
  -> (U.HStreamClientApi -> IO a)
  -> IO a
withAdminClient' host port f = withGRPCClient config (f <=< API.hstreamApiClient)
  where
    config =
      GRPC.ClientConfig { clientServerHost = GRPC.Host host
                        , clientServerPort = GRPC.Port (fromIntegral port)
                        , clientArgs = []
                        , clientSSLConfig = Nothing
                        , clientAuthority = Nothing
                        }
