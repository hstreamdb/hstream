{-# LANGUAGE DataKinds     #-}
{-# LANGUAGE TypeOperators #-}

module HStream.Server.Api where

import           Servant
import           HStream.Server.Type

-- | server api
-- query/select a.c from
-- info
type ServerAPI1 = StreamApi

type StreamApi =
    "show" :> "tasks" :> Get '[JSON] [TaskInfo]
    :<|> "create" :> "task" :> ReqBody '[JSON] ReqSQL :> Post '[JSON] (Either String TaskInfo)
    :<|> "query" :> "task" :> Capture "task id" TaskID :> Get '[JSON] (Maybe TaskInfo)
    :<|> "delete" :> "task" :> Capture "task id" TaskID :> Get '[JSON] Resp
    :<|> "create" :> "stream" :> "task" :> ReqBody '[JSON] ReqSQL :> StreamPost NoFraming JSON (SourceIO [RecordVal])
    :<|> "delete" :> "task" :> "all" :> Get '[JSON] Resp
