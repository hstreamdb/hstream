{-# LANGUAGE DataKinds     #-}
{-# LANGUAGE TypeOperators #-}

module HStream.Server.Api where

import           HStream.Server.Type
import           Servant

type ServerApi = StreamApi

type StreamApi =
    "show" :> "tasks" :> Get '[JSON] [TaskInfo]
    :<|> "create" :> "task" :> ReqBody '[JSON] ReqSQL :> Post '[JSON] (Either String TaskInfo)
    :<|> "query" :> "task" :> Capture "task id" TaskID :> Get '[JSON] (Maybe TaskInfo)
    :<|> "delete" :> "task" :> Capture "task id" TaskID :> Get '[JSON] Resp
    :<|> "create" :> "stream" :> "task" :> ReqBody '[JSON] ReqSQL :> StreamPost NoFraming OctetStream (SourceIO RecordStream)
    :<|> "delete" :> "task" :> "all" :> Get '[JSON] Resp
