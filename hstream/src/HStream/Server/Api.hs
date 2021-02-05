{-# LANGUAGE DataKinds     #-}
{-# LANGUAGE TypeOperators #-}

module HStream.Server.Api where

import           Data.Text           (Text)
import           HStream.Server.Type
import           Servant

type ServerApi = StreamApi

type StreamApi =
    "show" :> "queries" :> Get '[JSON] [TaskInfo]
    :<|> "create" :> "query" :> ReqBody '[JSON] ReqSQL :> Post '[JSON] (Either String TaskInfo)
    :<|> "terminate" :> "query" :> Capture "query id" TaskID :> Get '[JSON] Resp
    :<|> "create" :> "stream" :> "query" :> Capture "query name" Text :> ReqBody '[JSON] ReqSQL :> StreamPost NoFraming OctetStream (SourceIO RecordStream)
    :<|> "terminate" :> "query" :> "all" :> Get '[JSON] Resp
