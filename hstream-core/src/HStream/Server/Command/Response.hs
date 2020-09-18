{-# LANGUAGE OverloadedStrings #-}

-- | Messages send to client.
module HStream.Server.Command.Response
  ( mkHandshakeRespSucc

  , mkSPutRespSucc
  , mkSPutRespFail

  , mkElementResp
  , mkSimpleElementResp

  , mkCmdPush
  , mkCmdPushError

  , mkGeneralError
  , mkGeneralPushError
  ) where

import           Data.ByteString       (ByteString)

import           HStream.LogStore.Base (EntryID)
import qualified HStream.Server.Types  as T
import qualified HStream.Utils         as U
import qualified Network.HESP          as HESP

-------------------------------------------------------------------------------

{-# INLINE mkHandshakeRespSucc #-}
mkHandshakeRespSucc :: T.ClientId
                    -> HESP.Message
mkHandshakeRespSucc cid =
  HESP.mkArrayFromList [ HESP.mkBulkString "hi"
                       , HESP.mkBulkString "OK"
                       , HESP.mkBulkString $ T.packClientIdBS cid
                       ]

{-# INLINE mkSPutRespSucc #-}
mkSPutRespSucc :: T.ClientId
               -> ByteString
               -> EntryID
               -> HESP.Message
mkSPutRespSucc cid topic entryID =
  HESP.mkPushFromList "sput" [ HESP.mkBulkString $ T.packClientIdBS cid
                             , HESP.mkBulkString topic
                             , HESP.mkBulkString "OK"
                             , HESP.mkBulkString $ U.encodeTextShow entryID
                             ]

{-# INLINE mkSPutRespFail #-}
mkSPutRespFail :: T.ClientId
               -> ByteString
               -> ByteString
               -> HESP.Message
mkSPutRespFail cid topic errmsg =
  HESP.mkPushFromList "sput" [ HESP.mkBulkString $ T.packClientIdBS cid
                             , HESP.mkBulkString topic
                             , HESP.mkBulkString "ERR"
                             , HESP.mkBulkString errmsg
                             ]

{-# INLINE mkElementResp #-}
mkElementResp :: T.ClientId
              -> ByteString
              -> ByteString
              -> (EntryID, ByteString)
              -> HESP.Message
mkElementResp cid lcmd topic (i, p) =
  HESP.mkPushFromList lcmd [ HESP.mkBulkString $ T.packClientIdBS cid
                           , HESP.mkBulkString topic
                           , HESP.mkBulkString "OK"
                           , HESP.mkBulkString (U.encodeTextShow i)
                           , HESP.mkBulkString p
                           ]

{-# INLINE mkSimpleElementResp #-}
mkSimpleElementResp :: (EntryID, ByteString)
                    -> HESP.Message
mkSimpleElementResp (i, p) =
  let e_kvs = HESP.deserialize p
   in case e_kvs of
        Left s -> mkGeneralError $ "Entry deserialization failed: "
                                 <> U.str2bs s <> "."
        Right kvs ->
          HESP.mkArrayFromList [ HESP.mkBulkString (U.encodeTextShow i)
                               , kvs
                               ]

-------------------------------------------------------------------------------

{-# INLINE mkCmdPush #-}
mkCmdPush :: T.ClientId
          -> ByteString
          -> ByteString
          -> ByteString
          -> HESP.Message
mkCmdPush cid lcmd topic resp_type =
  HESP.mkPushFromList lcmd [ HESP.mkBulkString $ T.packClientIdBS cid
                           , HESP.mkBulkString topic
                           , HESP.mkBulkString resp_type
                           ]

{-# INLINE mkCmdPushError #-}
mkCmdPushError :: T.ClientId
               -> ByteString
               -> ByteString
               -> ByteString
               -> HESP.Message
mkCmdPushError cid lcmd topic errmsg =
  HESP.mkPushFromList lcmd [ HESP.mkBulkString $ T.packClientIdBS cid
                           , HESP.mkBulkString topic
                           , HESP.mkBulkString "ERR"
                           , HESP.mkBulkString errmsg
                           ]

{-# INLINE mkGeneralError #-}
mkGeneralError :: ByteString -> HESP.Message
mkGeneralError = HESP.mkSimpleError "ERR"

{-# INLINE mkGeneralPushError #-}
mkGeneralPushError :: ByteString -> ByteString -> HESP.Message
mkGeneralPushError pushtype errmsg =
  HESP.mkPushFromList pushtype [ HESP.mkBulkString "ERR"
                               , HESP.mkBulkString errmsg
                               ]
