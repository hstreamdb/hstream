{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

import           HStream.Server.HStreamApi
import           HStream.Server.Handler
import           Network.GRPC.HighLevel.Generated

options :: ServiceOptions
options = defaultServiceOptions

main :: IO ()
main = hstreamApiServer handlers options
