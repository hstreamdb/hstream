-----------------------------------------------------------------
-- Autogenerated by Thrift
--
-- DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
--  @generated
-----------------------------------------------------------------
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# OPTIONS_GHC -fno-warn-unused-imports#-}
{-# OPTIONS_GHC -fno-warn-overlapping-patterns#-}
{-# OPTIONS_GHC -fno-warn-incomplete-patterns#-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
module Fb303.FacebookService.Client
       (FacebookService, getVersion, getVersionIO, send_getVersion,
        _build_getVersion, recv_getVersion, _parse_getVersion, getStatus,
        getStatusIO, send_getStatus, _build_getStatus, recv_getStatus,
        _parse_getStatus, aliveSince, aliveSinceIO, send_aliveSince,
        _build_aliveSince, recv_aliveSince, _parse_aliveSince, getPid,
        getPidIO, send_getPid, _build_getPid, recv_getPid, _parse_getPid)
       where
import qualified Control.Arrow as Arrow
import qualified Control.Concurrent as Concurrent
import qualified Control.Exception as Exception
import qualified Control.Monad as Monad
import qualified Control.Monad.Trans.Class as Trans
import qualified Control.Monad.Trans.Reader as Reader
import qualified Data.ByteString.Builder as ByteString
import qualified Data.ByteString.Lazy as LBS
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Int as Int
import qualified Data.List as List
import qualified Data.Proxy as Proxy
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Prelude as Prelude
import qualified Thrift.Binary.Parser as Parser
import qualified Thrift.Codegen as Thrift
import qualified Thrift.Protocol.ApplicationException.Types
       as Thrift
import Data.Monoid ((<>))
import Prelude ((==), (=<<), (>>=), (<$>), (.))
import Fb303.Types

data FacebookService

getVersion ::
             (Thrift.Protocol p, Thrift.ClientChannel c,
              (Thrift.<:) s FacebookService) =>
             Thrift.ThriftM p c s Text.Text
getVersion
  = do Thrift.ThriftEnv _proxy _channel _opts _counter <- Reader.ask
       Trans.lift (getVersionIO _proxy _channel _counter _opts)

getVersionIO ::
               (Thrift.Protocol p, Thrift.ClientChannel c,
                (Thrift.<:) s FacebookService) =>
               Proxy.Proxy p ->
                 c s -> Thrift.Counter -> Thrift.RpcOptions -> Prelude.IO Text.Text
getVersionIO _proxy _channel _counter _opts
  = do (_handle, _sendCob, _recvCob) <- Thrift.mkCallbacks
                                          (recv_getVersion _proxy)
       send_getVersion _proxy _channel _counter _sendCob _recvCob _opts
       Thrift.wait _handle

send_getVersion ::
                  (Thrift.Protocol p, Thrift.ClientChannel c,
                   (Thrift.<:) s FacebookService) =>
                  Proxy.Proxy p ->
                    c s ->
                      Thrift.Counter ->
                        Thrift.SendCallback ->
                          Thrift.RecvCallback -> Thrift.RpcOptions -> Prelude.IO ()
send_getVersion _proxy _channel _counter _sendCob _recvCob _rpcOpts
  = do _seqNum <- _counter
       let
         _callMsg
           = LBS.toStrict
               (ByteString.toLazyByteString (_build_getVersion _proxy _seqNum))
       Thrift.sendRequest _channel
         (Thrift.Request _callMsg
            (Thrift.setRpcPriority _rpcOpts Thrift.NormalPriority))
         _sendCob
         _recvCob

recv_getVersion ::
                  (Thrift.Protocol p) =>
                  Proxy.Proxy p ->
                    Thrift.Response -> Prelude.Either Exception.SomeException Text.Text
recv_getVersion _proxy (Thrift.Response _response _)
  = Monad.join
      (Arrow.left (Exception.SomeException . Thrift.ProtocolException)
         (Parser.parse (_parse_getVersion _proxy) _response))

_build_getVersion ::
                    Thrift.Protocol p =>
                    Proxy.Proxy p -> Int.Int32 -> ByteString.Builder
_build_getVersion _proxy _seqNum
  = Thrift.genMsgBegin _proxy "getVersion" 1 _seqNum <>
      Thrift.genStruct _proxy []
      <> Thrift.genMsgEnd _proxy

_parse_getVersion ::
                    Thrift.Protocol p =>
                    Proxy.Proxy p ->
                      Parser.Parser (Prelude.Either Exception.SomeException Text.Text)
_parse_getVersion _proxy
  = do Thrift.MsgBegin _name _msgTy _ <- Thrift.parseMsgBegin _proxy
       _result <- case _msgTy of
                    1 -> Prelude.fail
                           "getVersion: expected reply but got function call"
                    2 | _name == "getVersion" ->
                        do let
                             _idMap = HashMap.fromList [("getVersion_success", 0)]
                           _fieldBegin <- Thrift.parseFieldBegin _proxy 0 _idMap
                           case _fieldBegin of
                             Thrift.FieldBegin _type _id _bool -> do case _id of
                                                                       0 | _type ==
                                                                             Thrift.getStringType
                                                                               _proxy
                                                                           ->
                                                                           Prelude.fmap
                                                                             Prelude.Right
                                                                             (Thrift.parseText
                                                                                _proxy)
                                                                       _ -> Prelude.fail
                                                                              (Prelude.unwords
                                                                                 ["unrecognized exception, type:",
                                                                                  Prelude.show
                                                                                    _type,
                                                                                  "field id:",
                                                                                  Prelude.show _id])
                             Thrift.FieldEnd -> Prelude.fail "no response"
                      | Prelude.otherwise -> Prelude.fail "reply function does not match"
                    3 -> Prelude.fmap (Prelude.Left . Exception.SomeException)
                           (Thrift.parseStruct _proxy ::
                              Parser.Parser Thrift.ApplicationException)
                    4 -> Prelude.fail
                           "getVersion: expected reply but got oneway function call"
                    _ -> Prelude.fail "getVersion: invalid message type"
       Thrift.parseMsgEnd _proxy
       Prelude.return _result

getStatus ::
            (Thrift.Protocol p, Thrift.ClientChannel c,
             (Thrift.<:) s FacebookService) =>
            Thrift.ThriftM p c s Fb_status
getStatus
  = do Thrift.ThriftEnv _proxy _channel _opts _counter <- Reader.ask
       Trans.lift (getStatusIO _proxy _channel _counter _opts)

getStatusIO ::
              (Thrift.Protocol p, Thrift.ClientChannel c,
               (Thrift.<:) s FacebookService) =>
              Proxy.Proxy p ->
                c s -> Thrift.Counter -> Thrift.RpcOptions -> Prelude.IO Fb_status
getStatusIO _proxy _channel _counter _opts
  = do (_handle, _sendCob, _recvCob) <- Thrift.mkCallbacks
                                          (recv_getStatus _proxy)
       send_getStatus _proxy _channel _counter _sendCob _recvCob _opts
       Thrift.wait _handle

send_getStatus ::
                 (Thrift.Protocol p, Thrift.ClientChannel c,
                  (Thrift.<:) s FacebookService) =>
                 Proxy.Proxy p ->
                   c s ->
                     Thrift.Counter ->
                       Thrift.SendCallback ->
                         Thrift.RecvCallback -> Thrift.RpcOptions -> Prelude.IO ()
send_getStatus _proxy _channel _counter _sendCob _recvCob _rpcOpts
  = do _seqNum <- _counter
       let
         _callMsg
           = LBS.toStrict
               (ByteString.toLazyByteString (_build_getStatus _proxy _seqNum))
       Thrift.sendRequest _channel
         (Thrift.Request _callMsg
            (Thrift.setRpcPriority _rpcOpts Thrift.Important))
         _sendCob
         _recvCob

recv_getStatus ::
                 (Thrift.Protocol p) =>
                 Proxy.Proxy p ->
                   Thrift.Response -> Prelude.Either Exception.SomeException Fb_status
recv_getStatus _proxy (Thrift.Response _response _)
  = Monad.join
      (Arrow.left (Exception.SomeException . Thrift.ProtocolException)
         (Parser.parse (_parse_getStatus _proxy) _response))

_build_getStatus ::
                   Thrift.Protocol p =>
                   Proxy.Proxy p -> Int.Int32 -> ByteString.Builder
_build_getStatus _proxy _seqNum
  = Thrift.genMsgBegin _proxy "getStatus" 1 _seqNum <>
      Thrift.genStruct _proxy []
      <> Thrift.genMsgEnd _proxy

_parse_getStatus ::
                   Thrift.Protocol p =>
                   Proxy.Proxy p ->
                     Parser.Parser (Prelude.Either Exception.SomeException Fb_status)
_parse_getStatus _proxy
  = do Thrift.MsgBegin _name _msgTy _ <- Thrift.parseMsgBegin _proxy
       _result <- case _msgTy of
                    1 -> Prelude.fail "getStatus: expected reply but got function call"
                    2 | _name == "getStatus" ->
                        do let
                             _idMap = HashMap.fromList [("getStatus_success", 0)]
                           _fieldBegin <- Thrift.parseFieldBegin _proxy 0 _idMap
                           case _fieldBegin of
                             Thrift.FieldBegin _type _id _bool -> do case _id of
                                                                       0 | _type ==
                                                                             Thrift.getI32Type
                                                                               _proxy
                                                                           ->
                                                                           Prelude.fmap
                                                                             Prelude.Right
                                                                             (Thrift.parseEnum
                                                                                _proxy
                                                                                "Fb_status")
                                                                       _ -> Prelude.fail
                                                                              (Prelude.unwords
                                                                                 ["unrecognized exception, type:",
                                                                                  Prelude.show
                                                                                    _type,
                                                                                  "field id:",
                                                                                  Prelude.show _id])
                             Thrift.FieldEnd -> Prelude.fail "no response"
                      | Prelude.otherwise -> Prelude.fail "reply function does not match"
                    3 -> Prelude.fmap (Prelude.Left . Exception.SomeException)
                           (Thrift.parseStruct _proxy ::
                              Parser.Parser Thrift.ApplicationException)
                    4 -> Prelude.fail
                           "getStatus: expected reply but got oneway function call"
                    _ -> Prelude.fail "getStatus: invalid message type"
       Thrift.parseMsgEnd _proxy
       Prelude.return _result

aliveSince ::
             (Thrift.Protocol p, Thrift.ClientChannel c,
              (Thrift.<:) s FacebookService) =>
             Thrift.ThriftM p c s Int.Int64
aliveSince
  = do Thrift.ThriftEnv _proxy _channel _opts _counter <- Reader.ask
       Trans.lift (aliveSinceIO _proxy _channel _counter _opts)

aliveSinceIO ::
               (Thrift.Protocol p, Thrift.ClientChannel c,
                (Thrift.<:) s FacebookService) =>
               Proxy.Proxy p ->
                 c s -> Thrift.Counter -> Thrift.RpcOptions -> Prelude.IO Int.Int64
aliveSinceIO _proxy _channel _counter _opts
  = do (_handle, _sendCob, _recvCob) <- Thrift.mkCallbacks
                                          (recv_aliveSince _proxy)
       send_aliveSince _proxy _channel _counter _sendCob _recvCob _opts
       Thrift.wait _handle

send_aliveSince ::
                  (Thrift.Protocol p, Thrift.ClientChannel c,
                   (Thrift.<:) s FacebookService) =>
                  Proxy.Proxy p ->
                    c s ->
                      Thrift.Counter ->
                        Thrift.SendCallback ->
                          Thrift.RecvCallback -> Thrift.RpcOptions -> Prelude.IO ()
send_aliveSince _proxy _channel _counter _sendCob _recvCob _rpcOpts
  = do _seqNum <- _counter
       let
         _callMsg
           = LBS.toStrict
               (ByteString.toLazyByteString (_build_aliveSince _proxy _seqNum))
       Thrift.sendRequest _channel
         (Thrift.Request _callMsg
            (Thrift.setRpcPriority _rpcOpts Thrift.Important))
         _sendCob
         _recvCob

recv_aliveSince ::
                  (Thrift.Protocol p) =>
                  Proxy.Proxy p ->
                    Thrift.Response -> Prelude.Either Exception.SomeException Int.Int64
recv_aliveSince _proxy (Thrift.Response _response _)
  = Monad.join
      (Arrow.left (Exception.SomeException . Thrift.ProtocolException)
         (Parser.parse (_parse_aliveSince _proxy) _response))

_build_aliveSince ::
                    Thrift.Protocol p =>
                    Proxy.Proxy p -> Int.Int32 -> ByteString.Builder
_build_aliveSince _proxy _seqNum
  = Thrift.genMsgBegin _proxy "aliveSince" 1 _seqNum <>
      Thrift.genStruct _proxy []
      <> Thrift.genMsgEnd _proxy

_parse_aliveSince ::
                    Thrift.Protocol p =>
                    Proxy.Proxy p ->
                      Parser.Parser (Prelude.Either Exception.SomeException Int.Int64)
_parse_aliveSince _proxy
  = do Thrift.MsgBegin _name _msgTy _ <- Thrift.parseMsgBegin _proxy
       _result <- case _msgTy of
                    1 -> Prelude.fail
                           "aliveSince: expected reply but got function call"
                    2 | _name == "aliveSince" ->
                        do let
                             _idMap = HashMap.fromList [("aliveSince_success", 0)]
                           _fieldBegin <- Thrift.parseFieldBegin _proxy 0 _idMap
                           case _fieldBegin of
                             Thrift.FieldBegin _type _id _bool -> do case _id of
                                                                       0 | _type ==
                                                                             Thrift.getI64Type
                                                                               _proxy
                                                                           ->
                                                                           Prelude.fmap
                                                                             Prelude.Right
                                                                             (Thrift.parseI64
                                                                                _proxy)
                                                                       _ -> Prelude.fail
                                                                              (Prelude.unwords
                                                                                 ["unrecognized exception, type:",
                                                                                  Prelude.show
                                                                                    _type,
                                                                                  "field id:",
                                                                                  Prelude.show _id])
                             Thrift.FieldEnd -> Prelude.fail "no response"
                      | Prelude.otherwise -> Prelude.fail "reply function does not match"
                    3 -> Prelude.fmap (Prelude.Left . Exception.SomeException)
                           (Thrift.parseStruct _proxy ::
                              Parser.Parser Thrift.ApplicationException)
                    4 -> Prelude.fail
                           "aliveSince: expected reply but got oneway function call"
                    _ -> Prelude.fail "aliveSince: invalid message type"
       Thrift.parseMsgEnd _proxy
       Prelude.return _result

getPid ::
         (Thrift.Protocol p, Thrift.ClientChannel c,
          (Thrift.<:) s FacebookService) =>
         Thrift.ThriftM p c s Int.Int64
getPid
  = do Thrift.ThriftEnv _proxy _channel _opts _counter <- Reader.ask
       Trans.lift (getPidIO _proxy _channel _counter _opts)

getPidIO ::
           (Thrift.Protocol p, Thrift.ClientChannel c,
            (Thrift.<:) s FacebookService) =>
           Proxy.Proxy p ->
             c s -> Thrift.Counter -> Thrift.RpcOptions -> Prelude.IO Int.Int64
getPidIO _proxy _channel _counter _opts
  = do (_handle, _sendCob, _recvCob) <- Thrift.mkCallbacks
                                          (recv_getPid _proxy)
       send_getPid _proxy _channel _counter _sendCob _recvCob _opts
       Thrift.wait _handle

send_getPid ::
              (Thrift.Protocol p, Thrift.ClientChannel c,
               (Thrift.<:) s FacebookService) =>
              Proxy.Proxy p ->
                c s ->
                  Thrift.Counter ->
                    Thrift.SendCallback ->
                      Thrift.RecvCallback -> Thrift.RpcOptions -> Prelude.IO ()
send_getPid _proxy _channel _counter _sendCob _recvCob _rpcOpts
  = do _seqNum <- _counter
       let
         _callMsg
           = LBS.toStrict
               (ByteString.toLazyByteString (_build_getPid _proxy _seqNum))
       Thrift.sendRequest _channel
         (Thrift.Request _callMsg
            (Thrift.setRpcPriority _rpcOpts Thrift.NormalPriority))
         _sendCob
         _recvCob

recv_getPid ::
              (Thrift.Protocol p) =>
              Proxy.Proxy p ->
                Thrift.Response -> Prelude.Either Exception.SomeException Int.Int64
recv_getPid _proxy (Thrift.Response _response _)
  = Monad.join
      (Arrow.left (Exception.SomeException . Thrift.ProtocolException)
         (Parser.parse (_parse_getPid _proxy) _response))

_build_getPid ::
                Thrift.Protocol p =>
                Proxy.Proxy p -> Int.Int32 -> ByteString.Builder
_build_getPid _proxy _seqNum
  = Thrift.genMsgBegin _proxy "getPid" 1 _seqNum <>
      Thrift.genStruct _proxy []
      <> Thrift.genMsgEnd _proxy

_parse_getPid ::
                Thrift.Protocol p =>
                Proxy.Proxy p ->
                  Parser.Parser (Prelude.Either Exception.SomeException Int.Int64)
_parse_getPid _proxy
  = do Thrift.MsgBegin _name _msgTy _ <- Thrift.parseMsgBegin _proxy
       _result <- case _msgTy of
                    1 -> Prelude.fail "getPid: expected reply but got function call"
                    2 | _name == "getPid" ->
                        do let
                             _idMap = HashMap.fromList [("getPid_success", 0)]
                           _fieldBegin <- Thrift.parseFieldBegin _proxy 0 _idMap
                           case _fieldBegin of
                             Thrift.FieldBegin _type _id _bool -> do case _id of
                                                                       0 | _type ==
                                                                             Thrift.getI64Type
                                                                               _proxy
                                                                           ->
                                                                           Prelude.fmap
                                                                             Prelude.Right
                                                                             (Thrift.parseI64
                                                                                _proxy)
                                                                       _ -> Prelude.fail
                                                                              (Prelude.unwords
                                                                                 ["unrecognized exception, type:",
                                                                                  Prelude.show
                                                                                    _type,
                                                                                  "field id:",
                                                                                  Prelude.show _id])
                             Thrift.FieldEnd -> Prelude.fail "no response"
                      | Prelude.otherwise -> Prelude.fail "reply function does not match"
                    3 -> Prelude.fmap (Prelude.Left . Exception.SomeException)
                           (Thrift.parseStruct _proxy ::
                              Parser.Parser Thrift.ApplicationException)
                    4 -> Prelude.fail
                           "getPid: expected reply but got oneway function call"
                    _ -> Prelude.fail "getPid: invalid message type"
       Thrift.parseMsgEnd _proxy
       Prelude.return _result
