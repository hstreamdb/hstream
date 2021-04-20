{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-unused-imports #-}
{-# OPTIONS_GHC -fno-warn-name-shadowing #-}
{-# OPTIONS_GHC -fno-warn-unused-matches #-}

-- | Generated by Haskell protocol buffer compiler. DO NOT EDIT!
module HStream.Server.HStreamApi where
import qualified Prelude as Hs
import qualified Proto3.Suite.Class as HsProtobuf
import qualified Proto3.Suite.DotProto as HsProtobuf
import qualified Proto3.Suite.JSONPB as HsJSONPB
import Proto3.Suite.JSONPB ((.=), (.:))
import qualified Proto3.Suite.Types as HsProtobuf
import qualified Proto3.Wire as HsProtobuf
import qualified Control.Applicative as Hs
import Control.Applicative ((<*>), (<|>), (<$>))
import qualified Control.DeepSeq as Hs
import qualified Control.Monad as Hs
import qualified Data.ByteString as Hs
import qualified Data.Coerce as Hs
import qualified Data.Int as Hs (Int16, Int32, Int64)
import qualified Data.List.NonEmpty as Hs (NonEmpty(..))
import qualified Data.Map as Hs (Map, mapKeysMonotonic)
import qualified Data.Proxy as Proxy
import qualified Data.String as Hs (fromString)
import qualified Data.Text.Lazy as Hs (Text)
import qualified Data.Vector as Hs (Vector)
import qualified Data.Word as Hs (Word16, Word32, Word64)
import qualified GHC.Enum as Hs
import qualified GHC.Generics as Hs
import qualified Unsafe.Coerce as Hs
import Network.GRPC.HighLevel.Generated as HsGRPC
import Network.GRPC.HighLevel.Client as HsGRPC
import Network.GRPC.HighLevel.Server as HsGRPC hiding (serverLoop)
import Network.GRPC.HighLevel.Server.Unregistered as HsGRPC
       (serverLoop)
import qualified ThirdParty.Google.Protobuf.Struct
 
data HStreamApi request
     response = HStreamApi{hstreamApiExecutePushQuery ::
                           request 'HsGRPC.ServerStreaming
                             HStream.Server.HStreamApi.CommandPushQuery
                             ThirdParty.Google.Protobuf.Struct.Struct
                             ->
                             Hs.IO
                               (response 'HsGRPC.ServerStreaming
                                  ThirdParty.Google.Protobuf.Struct.Struct),
                           hstreamApiExecuteQuery ::
                           request 'HsGRPC.Normal HStream.Server.HStreamApi.CommandQuery
                             HStream.Server.HStreamApi.CommandQueryResponse
                             ->
                             Hs.IO
                               (response 'HsGRPC.Normal
                                  HStream.Server.HStreamApi.CommandQueryResponse)}
              deriving Hs.Generic
 
hstreamApiServer ::
                   HStreamApi HsGRPC.ServerRequest HsGRPC.ServerResponse ->
                     HsGRPC.ServiceOptions -> Hs.IO ()
hstreamApiServer
  HStreamApi{hstreamApiExecutePushQuery = hstreamApiExecutePushQuery,
             hstreamApiExecuteQuery = hstreamApiExecuteQuery}
  (ServiceOptions serverHost serverPort useCompression
     userAgentPrefix userAgentSuffix initialMetadata sslConfig logger
     serverMaxReceiveMessageLength)
  = (HsGRPC.serverLoop
       HsGRPC.defaultOptions{HsGRPC.optNormalHandlers =
                               [(HsGRPC.UnaryHandler
                                   (HsGRPC.MethodName "/hstream.server.HStreamApi/ExecuteQuery")
                                   (HsGRPC.convertGeneratedServerHandler hstreamApiExecuteQuery))],
                             HsGRPC.optClientStreamHandlers = [],
                             HsGRPC.optServerStreamHandlers =
                               [(HsGRPC.ServerStreamHandler
                                   (HsGRPC.MethodName "/hstream.server.HStreamApi/ExecutePushQuery")
                                   (HsGRPC.convertGeneratedServerWriterHandler
                                      hstreamApiExecutePushQuery))],
                             HsGRPC.optBiDiStreamHandlers = [], optServerHost = serverHost,
                             optServerPort = serverPort, optUseCompression = useCompression,
                             optUserAgentPrefix = userAgentPrefix,
                             optUserAgentSuffix = userAgentSuffix,
                             optInitialMetadata = initialMetadata, optSSLConfig = sslConfig,
                             optLogger = logger,
                             optMaxReceiveMessageLength = serverMaxReceiveMessageLength})
 
hstreamApiClient ::
                   HsGRPC.Client ->
                     Hs.IO (HStreamApi HsGRPC.ClientRequest HsGRPC.ClientResult)
hstreamApiClient client
  = (Hs.pure HStreamApi) <*>
      ((Hs.pure (HsGRPC.clientRequest client)) <*>
         (HsGRPC.clientRegisterMethod client
            (HsGRPC.MethodName "/hstream.server.HStreamApi/ExecutePushQuery")))
      <*>
      ((Hs.pure (HsGRPC.clientRequest client)) <*>
         (HsGRPC.clientRegisterMethod client
            (HsGRPC.MethodName "/hstream.server.HStreamApi/ExecuteQuery")))
 
newtype CommandStreamTask = CommandStreamTask{commandStreamTaskCommandSql
                                              :: Hs.Text}
                            deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandStreamTask where
        nameOf _ = (Hs.fromString "CommandStreamTask")
 
instance HsProtobuf.HasDefault CommandStreamTask
 
instance HsProtobuf.Message CommandStreamTask where
        encodeMessage _
          CommandStreamTask{commandStreamTaskCommandSql =
                              commandStreamTaskCommandSql}
          = (Hs.mconcat
               [(HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 1)
                   commandStreamTaskCommandSql)])
        decodeMessage _
          = (Hs.pure CommandStreamTask) <*>
              (HsProtobuf.at HsProtobuf.decodeMessageField
                 (HsProtobuf.FieldNumber 1))
        dotProto _
          = [(HsProtobuf.DotProtoField (HsProtobuf.FieldNumber 1)
                (HsProtobuf.Prim HsProtobuf.String)
                (HsProtobuf.Single "command_sql")
                []
                "")]
 
instance HsJSONPB.ToJSONPB CommandStreamTask where
        toJSONPB (CommandStreamTask f1)
          = (HsJSONPB.object ["command_sql" .= f1])
        toEncodingPB (CommandStreamTask f1)
          = (HsJSONPB.pairs ["command_sql" .= f1])
 
instance HsJSONPB.FromJSONPB CommandStreamTask where
        parseJSONPB
          = (HsJSONPB.withObject "CommandStreamTask"
               (\ obj -> (Hs.pure CommandStreamTask) <*> obj .: "command_sql"))
 
instance HsJSONPB.ToJSON CommandStreamTask where
        toJSON = HsJSONPB.toAesonValue
        toEncoding = HsJSONPB.toAesonEncoding
 
instance HsJSONPB.FromJSON CommandStreamTask where
        parseJSON = HsJSONPB.parseJSONPB
 
instance HsJSONPB.ToSchema CommandStreamTask where
        declareNamedSchema _
          = do let declare_command_sql = HsJSONPB.declareSchemaRef
               commandStreamTaskCommandSql <- declare_command_sql Proxy.Proxy
               let _ = Hs.pure CommandStreamTask <*>
                         HsJSONPB.asProxy declare_command_sql
               Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandStreamTask",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList
                                                       [("command_sql",
                                                         commandStreamTaskCommandSql)]}})
 
newtype CommandStreamTaskResponse = CommandStreamTaskResponse{commandStreamTaskResponseCommandResp
                                                              :: Hs.Text}
                                    deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandStreamTaskResponse where
        nameOf _ = (Hs.fromString "CommandStreamTaskResponse")
 
instance HsProtobuf.HasDefault CommandStreamTaskResponse
 
instance HsProtobuf.Message CommandStreamTaskResponse where
        encodeMessage _
          CommandStreamTaskResponse{commandStreamTaskResponseCommandResp =
                                      commandStreamTaskResponseCommandResp}
          = (Hs.mconcat
               [(HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 1)
                   commandStreamTaskResponseCommandResp)])
        decodeMessage _
          = (Hs.pure CommandStreamTaskResponse) <*>
              (HsProtobuf.at HsProtobuf.decodeMessageField
                 (HsProtobuf.FieldNumber 1))
        dotProto _
          = [(HsProtobuf.DotProtoField (HsProtobuf.FieldNumber 1)
                (HsProtobuf.Prim HsProtobuf.String)
                (HsProtobuf.Single "command_resp")
                []
                "")]
 
instance HsJSONPB.ToJSONPB CommandStreamTaskResponse where
        toJSONPB (CommandStreamTaskResponse f1)
          = (HsJSONPB.object ["command_resp" .= f1])
        toEncodingPB (CommandStreamTaskResponse f1)
          = (HsJSONPB.pairs ["command_resp" .= f1])
 
instance HsJSONPB.FromJSONPB CommandStreamTaskResponse where
        parseJSONPB
          = (HsJSONPB.withObject "CommandStreamTaskResponse"
               (\ obj ->
                  (Hs.pure CommandStreamTaskResponse) <*> obj .: "command_resp"))
 
instance HsJSONPB.ToJSON CommandStreamTaskResponse where
        toJSON = HsJSONPB.toAesonValue
        toEncoding = HsJSONPB.toAesonEncoding
 
instance HsJSONPB.FromJSON CommandStreamTaskResponse where
        parseJSON = HsJSONPB.parseJSONPB
 
instance HsJSONPB.ToSchema CommandStreamTaskResponse where
        declareNamedSchema _
          = do let declare_command_resp = HsJSONPB.declareSchemaRef
               commandStreamTaskResponseCommandResp <- declare_command_resp
                                                         Proxy.Proxy
               let _ = Hs.pure CommandStreamTaskResponse <*>
                         HsJSONPB.asProxy declare_command_resp
               Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandStreamTaskResponse",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList
                                                       [("command_resp",
                                                         commandStreamTaskResponseCommandResp)]}})
 
data CommandConnect = CommandConnect{commandConnectClientVersion ::
                                     Hs.Text,
                                     commandConnectProtocolVersion :: Hs.Int32}
                    deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandConnect where
        nameOf _ = (Hs.fromString "CommandConnect")
 
instance HsProtobuf.HasDefault CommandConnect
 
instance HsProtobuf.Message CommandConnect where
        encodeMessage _
          CommandConnect{commandConnectClientVersion =
                           commandConnectClientVersion,
                         commandConnectProtocolVersion = commandConnectProtocolVersion}
          = (Hs.mconcat
               [(HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 1)
                   commandConnectClientVersion),
                (HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 2)
                   commandConnectProtocolVersion)])
        decodeMessage _
          = (Hs.pure CommandConnect) <*>
              (HsProtobuf.at HsProtobuf.decodeMessageField
                 (HsProtobuf.FieldNumber 1))
              <*>
              (HsProtobuf.at HsProtobuf.decodeMessageField
                 (HsProtobuf.FieldNumber 2))
        dotProto _
          = [(HsProtobuf.DotProtoField (HsProtobuf.FieldNumber 1)
                (HsProtobuf.Prim HsProtobuf.String)
                (HsProtobuf.Single "client_version")
                []
                ""),
             (HsProtobuf.DotProtoField (HsProtobuf.FieldNumber 2)
                (HsProtobuf.Prim HsProtobuf.Int32)
                (HsProtobuf.Single "protocol_version")
                []
                "")]
 
instance HsJSONPB.ToJSONPB CommandConnect where
        toJSONPB (CommandConnect f1 f2)
          = (HsJSONPB.object
               ["client_version" .= f1, "protocol_version" .= f2])
        toEncodingPB (CommandConnect f1 f2)
          = (HsJSONPB.pairs
               ["client_version" .= f1, "protocol_version" .= f2])
 
instance HsJSONPB.FromJSONPB CommandConnect where
        parseJSONPB
          = (HsJSONPB.withObject "CommandConnect"
               (\ obj ->
                  (Hs.pure CommandConnect) <*> obj .: "client_version" <*>
                    obj .: "protocol_version"))
 
instance HsJSONPB.ToJSON CommandConnect where
        toJSON = HsJSONPB.toAesonValue
        toEncoding = HsJSONPB.toAesonEncoding
 
instance HsJSONPB.FromJSON CommandConnect where
        parseJSON = HsJSONPB.parseJSONPB
 
instance HsJSONPB.ToSchema CommandConnect where
        declareNamedSchema _
          = do let declare_client_version = HsJSONPB.declareSchemaRef
               commandConnectClientVersion <- declare_client_version Proxy.Proxy
               let declare_protocol_version = HsJSONPB.declareSchemaRef
               commandConnectProtocolVersion <- declare_protocol_version
                                                  Proxy.Proxy
               let _ = Hs.pure CommandConnect <*>
                         HsJSONPB.asProxy declare_client_version
                         <*> HsJSONPB.asProxy declare_protocol_version
               Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandConnect",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList
                                                       [("client_version",
                                                         commandConnectClientVersion),
                                                        ("protocol_version",
                                                         commandConnectProtocolVersion)]}})
 
data CommandConnected = CommandConnected{commandConnectedServerVersion
                                         :: Hs.Text,
                                         commandConnectedProtocolVersion :: Hs.Int32}
                      deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandConnected where
        nameOf _ = (Hs.fromString "CommandConnected")
 
instance HsProtobuf.HasDefault CommandConnected
 
instance HsProtobuf.Message CommandConnected where
        encodeMessage _
          CommandConnected{commandConnectedServerVersion =
                             commandConnectedServerVersion,
                           commandConnectedProtocolVersion = commandConnectedProtocolVersion}
          = (Hs.mconcat
               [(HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 1)
                   commandConnectedServerVersion),
                (HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 2)
                   commandConnectedProtocolVersion)])
        decodeMessage _
          = (Hs.pure CommandConnected) <*>
              (HsProtobuf.at HsProtobuf.decodeMessageField
                 (HsProtobuf.FieldNumber 1))
              <*>
              (HsProtobuf.at HsProtobuf.decodeMessageField
                 (HsProtobuf.FieldNumber 2))
        dotProto _
          = [(HsProtobuf.DotProtoField (HsProtobuf.FieldNumber 1)
                (HsProtobuf.Prim HsProtobuf.String)
                (HsProtobuf.Single "server_version")
                []
                ""),
             (HsProtobuf.DotProtoField (HsProtobuf.FieldNumber 2)
                (HsProtobuf.Prim HsProtobuf.Int32)
                (HsProtobuf.Single "protocol_version")
                []
                "")]
 
instance HsJSONPB.ToJSONPB CommandConnected where
        toJSONPB (CommandConnected f1 f2)
          = (HsJSONPB.object
               ["server_version" .= f1, "protocol_version" .= f2])
        toEncodingPB (CommandConnected f1 f2)
          = (HsJSONPB.pairs
               ["server_version" .= f1, "protocol_version" .= f2])
 
instance HsJSONPB.FromJSONPB CommandConnected where
        parseJSONPB
          = (HsJSONPB.withObject "CommandConnected"
               (\ obj ->
                  (Hs.pure CommandConnected) <*> obj .: "server_version" <*>
                    obj .: "protocol_version"))
 
instance HsJSONPB.ToJSON CommandConnected where
        toJSON = HsJSONPB.toAesonValue
        toEncoding = HsJSONPB.toAesonEncoding
 
instance HsJSONPB.FromJSON CommandConnected where
        parseJSON = HsJSONPB.parseJSONPB
 
instance HsJSONPB.ToSchema CommandConnected where
        declareNamedSchema _
          = do let declare_server_version = HsJSONPB.declareSchemaRef
               commandConnectedServerVersion <- declare_server_version Proxy.Proxy
               let declare_protocol_version = HsJSONPB.declareSchemaRef
               commandConnectedProtocolVersion <- declare_protocol_version
                                                    Proxy.Proxy
               let _ = Hs.pure CommandConnected <*>
                         HsJSONPB.asProxy declare_server_version
                         <*> HsJSONPB.asProxy declare_protocol_version
               Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandConnected",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList
                                                       [("server_version",
                                                         commandConnectedServerVersion),
                                                        ("protocol_version",
                                                         commandConnectedProtocolVersion)]}})
 
newtype CommandPushQuery = CommandPushQuery{commandPushQueryQueryText
                                            :: Hs.Text}
                           deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandPushQuery where
        nameOf _ = (Hs.fromString "CommandPushQuery")
 
instance HsProtobuf.HasDefault CommandPushQuery
 
instance HsProtobuf.Message CommandPushQuery where
        encodeMessage _
          CommandPushQuery{commandPushQueryQueryText =
                             commandPushQueryQueryText}
          = (Hs.mconcat
               [(HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 1)
                   commandPushQueryQueryText)])
        decodeMessage _
          = (Hs.pure CommandPushQuery) <*>
              (HsProtobuf.at HsProtobuf.decodeMessageField
                 (HsProtobuf.FieldNumber 1))
        dotProto _
          = [(HsProtobuf.DotProtoField (HsProtobuf.FieldNumber 1)
                (HsProtobuf.Prim HsProtobuf.String)
                (HsProtobuf.Single "query_text")
                []
                "")]
 
instance HsJSONPB.ToJSONPB CommandPushQuery where
        toJSONPB (CommandPushQuery f1)
          = (HsJSONPB.object ["query_text" .= f1])
        toEncodingPB (CommandPushQuery f1)
          = (HsJSONPB.pairs ["query_text" .= f1])
 
instance HsJSONPB.FromJSONPB CommandPushQuery where
        parseJSONPB
          = (HsJSONPB.withObject "CommandPushQuery"
               (\ obj -> (Hs.pure CommandPushQuery) <*> obj .: "query_text"))
 
instance HsJSONPB.ToJSON CommandPushQuery where
        toJSON = HsJSONPB.toAesonValue
        toEncoding = HsJSONPB.toAesonEncoding
 
instance HsJSONPB.FromJSON CommandPushQuery where
        parseJSON = HsJSONPB.parseJSONPB
 
instance HsJSONPB.ToSchema CommandPushQuery where
        declareNamedSchema _
          = do let declare_query_text = HsJSONPB.declareSchemaRef
               commandPushQueryQueryText <- declare_query_text Proxy.Proxy
               let _ = Hs.pure CommandPushQuery <*>
                         HsJSONPB.asProxy declare_query_text
               Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandPushQuery",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList
                                                       [("query_text",
                                                         commandPushQueryQueryText)]}})
 
newtype CommandQuery = CommandQuery{commandQueryStmtText ::
                                    Hs.Text}
                       deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandQuery where
        nameOf _ = (Hs.fromString "CommandQuery")
 
instance HsProtobuf.HasDefault CommandQuery
 
instance HsProtobuf.Message CommandQuery where
        encodeMessage _
          CommandQuery{commandQueryStmtText = commandQueryStmtText}
          = (Hs.mconcat
               [(HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 1)
                   commandQueryStmtText)])
        decodeMessage _
          = (Hs.pure CommandQuery) <*>
              (HsProtobuf.at HsProtobuf.decodeMessageField
                 (HsProtobuf.FieldNumber 1))
        dotProto _
          = [(HsProtobuf.DotProtoField (HsProtobuf.FieldNumber 1)
                (HsProtobuf.Prim HsProtobuf.String)
                (HsProtobuf.Single "stmt_text")
                []
                "")]
 
instance HsJSONPB.ToJSONPB CommandQuery where
        toJSONPB (CommandQuery f1) = (HsJSONPB.object ["stmt_text" .= f1])
        toEncodingPB (CommandQuery f1)
          = (HsJSONPB.pairs ["stmt_text" .= f1])
 
instance HsJSONPB.FromJSONPB CommandQuery where
        parseJSONPB
          = (HsJSONPB.withObject "CommandQuery"
               (\ obj -> (Hs.pure CommandQuery) <*> obj .: "stmt_text"))
 
instance HsJSONPB.ToJSON CommandQuery where
        toJSON = HsJSONPB.toAesonValue
        toEncoding = HsJSONPB.toAesonEncoding
 
instance HsJSONPB.FromJSON CommandQuery where
        parseJSON = HsJSONPB.parseJSONPB
 
instance HsJSONPB.ToSchema CommandQuery where
        declareNamedSchema _
          = do let declare_stmt_text = HsJSONPB.declareSchemaRef
               commandQueryStmtText <- declare_stmt_text Proxy.Proxy
               let _ = Hs.pure CommandQuery <*> HsJSONPB.asProxy declare_stmt_text
               Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandQuery",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList
                                                       [("stmt_text", commandQueryStmtText)]}})
 
newtype CommandQueryResponse = CommandQueryResponse{commandQueryResponseKind
                                                    :: Hs.Maybe CommandQueryResponseKind}
                               deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandQueryResponse where
        nameOf _ = (Hs.fromString "CommandQueryResponse")
 
instance HsProtobuf.HasDefault CommandQueryResponse
 
instance HsProtobuf.Message CommandQueryResponse where
        encodeMessage _
          CommandQueryResponse{commandQueryResponseKind =
                                 commandQueryResponseKind}
          = (Hs.mconcat
               [case commandQueryResponseKind of
                    Hs.Nothing -> Hs.mempty
                    Hs.Just x
                      -> case x of
                             CommandQueryResponseKindSuccess y
                               -> (HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 1)
                                     (Hs.coerce @(Hs.Maybe HStream.Server.HStreamApi.CommandSuccess)
                                        @(HsProtobuf.Nested HStream.Server.HStreamApi.CommandSuccess)
                                        (Hs.Just y)))
                             CommandQueryResponseKindResultSet y
                               -> (HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 2)
                                     (Hs.coerce
                                        @(Hs.Maybe HStream.Server.HStreamApi.CommandQueryResultSet)
                                        @(HsProtobuf.Nested HStream.Server.HStreamApi.CommandQueryResultSet)
                                        (Hs.Just y)))])
        decodeMessage _
          = (Hs.pure CommandQueryResponse) <*>
              (HsProtobuf.oneof Hs.Nothing
                 [((HsProtobuf.FieldNumber 1),
                   (Hs.pure (Hs.fmap CommandQueryResponseKindSuccess)) <*>
                     (Hs.coerce
                        @(_ (HsProtobuf.Nested HStream.Server.HStreamApi.CommandSuccess))
                        @(_ (Hs.Maybe HStream.Server.HStreamApi.CommandSuccess))
                        HsProtobuf.decodeMessageField)),
                  ((HsProtobuf.FieldNumber 2),
                   (Hs.pure (Hs.fmap CommandQueryResponseKindResultSet)) <*>
                     (Hs.coerce
                        @(_ (HsProtobuf.Nested HStream.Server.HStreamApi.CommandQueryResultSet))
                        @(_ (Hs.Maybe HStream.Server.HStreamApi.CommandQueryResultSet))
                        HsProtobuf.decodeMessageField))])
        dotProto _ = []
 
instance HsJSONPB.ToJSONPB CommandQueryResponse where
        toJSONPB (CommandQueryResponse f1_or_f2)
          = (HsJSONPB.object
               [(let encodeKind
                       = (case f1_or_f2 of
                              Hs.Just (CommandQueryResponseKindSuccess f1)
                                -> (HsJSONPB.pair "success" f1)
                              Hs.Just (CommandQueryResponseKindResultSet f2)
                                -> (HsJSONPB.pair "result_set" f2)
                              Hs.Nothing -> Hs.mempty)
                   in
                   \ options ->
                     if HsJSONPB.optEmitNamedOneof options then
                       ("kind" .= (HsJSONPB.objectOrNull [encodeKind] options)) options
                       else encodeKind options)])
        toEncodingPB (CommandQueryResponse f1_or_f2)
          = (HsJSONPB.pairs
               [(let encodeKind
                       = (case f1_or_f2 of
                              Hs.Just (CommandQueryResponseKindSuccess f1)
                                -> (HsJSONPB.pair "success" f1)
                              Hs.Just (CommandQueryResponseKindResultSet f2)
                                -> (HsJSONPB.pair "result_set" f2)
                              Hs.Nothing -> Hs.mempty)
                   in
                   \ options ->
                     if HsJSONPB.optEmitNamedOneof options then
                       ("kind" .= (HsJSONPB.pairsOrNull [encodeKind] options)) options
                       else encodeKind options)])
 
instance HsJSONPB.FromJSONPB CommandQueryResponse where
        parseJSONPB
          = (HsJSONPB.withObject "CommandQueryResponse"
               (\ obj ->
                  (Hs.pure CommandQueryResponse) <*>
                    (let parseKind parseObj
                           = Hs.msum
                               [Hs.Just Hs.. CommandQueryResponseKindSuccess <$>
                                  (HsJSONPB.parseField parseObj "success"),
                                Hs.Just Hs.. CommandQueryResponseKindResultSet <$>
                                  (HsJSONPB.parseField parseObj "result_set"),
                                Hs.pure Hs.Nothing]
                       in
                       ((obj .: "kind") Hs.>>= (HsJSONPB.withObject "kind" parseKind)) <|>
                         (parseKind obj))))
 
instance HsJSONPB.ToJSON CommandQueryResponse where
        toJSON = HsJSONPB.toAesonValue
        toEncoding = HsJSONPB.toAesonEncoding
 
instance HsJSONPB.FromJSON CommandQueryResponse where
        parseJSON = HsJSONPB.parseJSONPB
 
instance HsJSONPB.ToSchema CommandQueryResponse where
        declareNamedSchema _
          = do let declare_kind = HsJSONPB.declareSchemaRef
               commandQueryResponseKind <- declare_kind Proxy.Proxy
               let _ = Hs.pure CommandQueryResponse <*>
                         HsJSONPB.asProxy declare_kind
               Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandQueryResponse",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList
                                                       [("kind", commandQueryResponseKind)]}})
 
data CommandQueryResponseKind = CommandQueryResponseKindSuccess HStream.Server.HStreamApi.CommandSuccess
                              | CommandQueryResponseKindResultSet HStream.Server.HStreamApi.CommandQueryResultSet
                              deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandQueryResponseKind where
        nameOf _ = (Hs.fromString "CommandQueryResponseKind")
 
instance HsJSONPB.ToSchema CommandQueryResponseKind where
        declareNamedSchema _
          = do let declare_success = HsJSONPB.declareSchemaRef
               commandQueryResponseKindSuccess <- declare_success Proxy.Proxy
               let _ = Hs.pure CommandQueryResponseKindSuccess <*>
                         HsJSONPB.asProxy declare_success
               let declare_result_set = HsJSONPB.declareSchemaRef
               commandQueryResponseKindResultSet <- declare_result_set Proxy.Proxy
               let _ = Hs.pure CommandQueryResponseKindResultSet <*>
                         HsJSONPB.asProxy declare_result_set
               Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandQueryResponseKind",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList
                                                       [("success",
                                                         commandQueryResponseKindSuccess),
                                                        ("result_set",
                                                         commandQueryResponseKindResultSet)],
                                                   HsJSONPB._schemaMinProperties = Hs.Just 1,
                                                   HsJSONPB._schemaMaxProperties = Hs.Just 1}})
 
newtype CommandQueryResultSet = CommandQueryResultSet{commandQueryResultSetResultSet
                                                      ::
                                                      Hs.Vector
                                                        ThirdParty.Google.Protobuf.Struct.Struct}
                                deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandQueryResultSet where
        nameOf _ = (Hs.fromString "CommandQueryResultSet")
 
instance HsProtobuf.HasDefault CommandQueryResultSet
 
instance HsProtobuf.Message CommandQueryResultSet where
        encodeMessage _
          CommandQueryResultSet{commandQueryResultSetResultSet =
                                  commandQueryResultSetResultSet}
          = (Hs.mconcat
               [(HsProtobuf.encodeMessageField (HsProtobuf.FieldNumber 1)
                   (Hs.coerce @(Hs.Vector ThirdParty.Google.Protobuf.Struct.Struct)
                      @(HsProtobuf.NestedVec ThirdParty.Google.Protobuf.Struct.Struct)
                      commandQueryResultSetResultSet))])
        decodeMessage _
          = (Hs.pure CommandQueryResultSet) <*>
              (Hs.coerce
                 @(_ (HsProtobuf.NestedVec ThirdParty.Google.Protobuf.Struct.Struct))
                 @(_ (Hs.Vector ThirdParty.Google.Protobuf.Struct.Struct))
                 (HsProtobuf.at HsProtobuf.decodeMessageField
                    (HsProtobuf.FieldNumber 1)))
        dotProto _
          = [(HsProtobuf.DotProtoField (HsProtobuf.FieldNumber 1)
                (HsProtobuf.Repeated
                   (HsProtobuf.Named (HsProtobuf.Single "Struct")))
                (HsProtobuf.Single "result_set")
                []
                "")]
 
instance HsJSONPB.ToJSONPB CommandQueryResultSet where
        toJSONPB (CommandQueryResultSet f1)
          = (HsJSONPB.object ["result_set" .= f1])
        toEncodingPB (CommandQueryResultSet f1)
          = (HsJSONPB.pairs ["result_set" .= f1])
 
instance HsJSONPB.FromJSONPB CommandQueryResultSet where
        parseJSONPB
          = (HsJSONPB.withObject "CommandQueryResultSet"
               (\ obj -> (Hs.pure CommandQueryResultSet) <*> obj .: "result_set"))
 
instance HsJSONPB.ToJSON CommandQueryResultSet where
        toJSON = HsJSONPB.toAesonValue
        toEncoding = HsJSONPB.toAesonEncoding
 
instance HsJSONPB.FromJSON CommandQueryResultSet where
        parseJSON = HsJSONPB.parseJSONPB
 
instance HsJSONPB.ToSchema CommandQueryResultSet where
        declareNamedSchema _
          = do let declare_result_set = HsJSONPB.declareSchemaRef
               commandQueryResultSetResultSet <- declare_result_set Proxy.Proxy
               let _ = Hs.pure CommandQueryResultSet <*>
                         HsJSONPB.asProxy declare_result_set
               Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandQueryResultSet",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList
                                                       [("result_set",
                                                         commandQueryResultSetResultSet)]}})
 
data CommandSuccess = CommandSuccess{}
                    deriving (Hs.Show, Hs.Eq, Hs.Ord, Hs.Generic, Hs.NFData)
 
instance HsProtobuf.Named CommandSuccess where
        nameOf _ = (Hs.fromString "CommandSuccess")
 
instance HsProtobuf.HasDefault CommandSuccess
 
instance HsProtobuf.Message CommandSuccess where
        encodeMessage _ CommandSuccess{} = (Hs.mconcat [])
        decodeMessage _ = (Hs.pure CommandSuccess)
        dotProto _ = []
 
instance HsJSONPB.ToJSONPB CommandSuccess where
        toJSONPB (CommandSuccess) = (HsJSONPB.object [])
        toEncodingPB (CommandSuccess) = (HsJSONPB.pairs [])
 
instance HsJSONPB.FromJSONPB CommandSuccess where
        parseJSONPB
          = (HsJSONPB.withObject "CommandSuccess"
               (\ obj -> (Hs.pure CommandSuccess)))
 
instance HsJSONPB.ToJSON CommandSuccess where
        toJSON = HsJSONPB.toAesonValue
        toEncoding = HsJSONPB.toAesonEncoding
 
instance HsJSONPB.FromJSON CommandSuccess where
        parseJSON = HsJSONPB.parseJSONPB
 
instance HsJSONPB.ToSchema CommandSuccess where
        declareNamedSchema _
          = do Hs.return
                 (HsJSONPB.NamedSchema{HsJSONPB._namedSchemaName =
                                         Hs.Just "CommandSuccess",
                                       HsJSONPB._namedSchemaSchema =
                                         Hs.mempty{HsJSONPB._schemaParamSchema =
                                                     Hs.mempty{HsJSONPB._paramSchemaType =
                                                                 Hs.Just HsJSONPB.SwaggerObject},
                                                   HsJSONPB._schemaProperties =
                                                     HsJSONPB.insOrdFromList []}})