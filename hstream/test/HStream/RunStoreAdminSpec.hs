{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunStoreAdminSpec (spec) where

import           Data.Int                         (Int32)
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (isJust)
import           Network.GRPC.HighLevel.Generated
import           Test.Hspec

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger
import           HStream.Utils                    (setupSigsegvHandler)


listNodes :: IO (Maybe ListNodesResponse)
listNodes = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let listNodesRequest = ListNodesRequest {}
  resp <- hstreamApiListNodes (ClientNormalRequest listNodesRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse x@ListNodesResponse{} _meta1 _meta2 _status _details -> return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "List Nodes Client Error: " <> show clientError
      return Nothing

getNode :: Int32 -> IO (Maybe Node)
getNode nodeId = withGRPCClient clientConfig $ \client -> do
  HStreamApi{..} <- hstreamApiClient client
  let getNodeRequest = GetNodeRequest { getNodeRequestId = nodeId }
  resp <- hstreamApiGetNode (ClientNormalRequest getNodeRequest 100 (MetadataMap Map.empty))
  case resp of
    ClientNormalResponse x@Node{} _meta1 _meta2 _status _details -> do
      return $ Just x
    ClientErrorResponse clientError -> do
      putStrLn $ "Get Node Client Error: " <> show clientError
      return Nothing

spec :: Spec
spec = xdescribe "HStream.RunConnectorSpec" $ do
  runIO setupSigsegvHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  it "list nodes" $ do
    Just ListNodesResponse {listNodesResponseNodes = nodes} <- listNodes
    (length nodes >= 1) `shouldBe` True

  it "get node" $ do
    node <- getNode 0
    node `shouldSatisfy` isJust
