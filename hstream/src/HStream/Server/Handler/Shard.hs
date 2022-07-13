{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Shard
  ( listShardsHandler
  , readShardHandler
  , splitShardsHandler
  )
where

import           Control.Exception
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Shard        as C
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Store                    as Store
import           HStream.Utils

-----------------------------------------------------------------------------------

listShardsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListShardsRequest ListShardsResponse
  -> IO (ServerResponse 'Normal ListShardsResponse)
listShardsHandler sc (ServerNormalRequest _metadata request) = do
  Log.debug "Receive List Shards Request"
  C.listShards sc request >>= returnResp . ListShardsResponse

readShardHandler
  :: ServerContext
  -> ServerRequest 'Normal ReadShardRequest ReadShardResponse
  -> IO (ServerResponse 'Normal ReadShardResponse)
readShardHandler sc (ServerNormalRequest _metadata request) = readShardExceptionHandle $ do
  Log.debug $ "Receive read shard Request: " <> Log.buildString (show request)
  C.readShard sc request >>= returnResp . ReadShardResponse

splitShardsHandler
  :: ServerContext
  -> ServerRequest 'Normal SplitShardsRequest SplitShardsResponse
  -> IO (ServerResponse 'Normal SplitShardsResponse)
splitShardsHandler sc (ServerNormalRequest _metadata request) = do
  Log.debug $ "Receive Split Shards Request: " <> Log.buildString' (show request)
  C.splitShards sc request >>= returnResp . SplitShardsResponse

-----------------------------------------------------------------------------------

readShardExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
readShardExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  [ Handler (\(err :: Store.NOTFOUND) ->
      return (StatusUnavailable, mkStatusDetails err))
  ] ++ defaultHandlers
