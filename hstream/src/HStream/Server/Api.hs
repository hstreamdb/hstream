{-# LANGUAGE DataKinds     #-}
{-# LANGUAGE TypeOperators #-}

module HStream.Server.Api where

import           HStream.Server.Type
import           Servant

type ServerApi = StreamApi

type StreamApi =
    "show" :> "querys" :> Get '[JSON] [TaskInfo]
    :<|> "create" :> "query" :> ReqBody '[JSON] ReqSQL :> Post '[JSON] (Either String TaskInfo)
    :<|> "delete" :> "query" :> Capture "query id" TaskID :> Get '[JSON] Resp
    :<|> "create" :> "stream" :> "query" :> ReqBody '[JSON] ReqSQL :> StreamPost NoFraming OctetStream (SourceIO RecordStream)
    :<|> "delete" :> "query" :> "all" :> Get '[JSON] Resp
