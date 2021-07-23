{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunPersistentSpec where

import           Control.Exception
import           Control.Monad                        (void)
import           Data.Int                             (Int64)
import           Data.Maybe                           (fromMaybe)
import           GHC.Stack                            (HasCallStack)
import           System.Environment                   (lookupEnv)
import           Test.Hspec                           (Spec, describe, it,
                                                       runIO, shouldReturn)
import           Z.Data.CBytes                        (CBytes)
import qualified Z.Data.CBytes                        as CB
import           Z.Data.JSON                          (JSON, decode, encode)
import qualified ZooKeeper                            as Z
import           ZooKeeper.Exception                  (ZNODEEXISTS)
import           ZooKeeper.Types                      (DataCompletion (dataCompletionValue),
                                                       ZHandle)

import qualified HStream.Server.Persistence           as P
import           HStream.Server.Persistence.Exception (FailedToDecode (FailedToDecode))

initZooKeeper :: ZHandle -> IO ()
initZooKeeper zk = catch (P.initializeAncestors zk) (\(_ :: ZNODEEXISTS) -> pure ())

decodeQ :: JSON a => DataCompletion -> a
decodeQ = (\case { Right x -> x ; _ -> throw FailedToDecode}) . snd . decode
        . (\case { Nothing -> ""; Just x -> x}) . dataCompletionValue

setQueryStatus' :: HasCallStack => CBytes -> Int64 -> P.PStatus -> ZHandle -> IO ()
setQueryStatus' qid time statusQ zkHandle = void $
  Z.zooSet zkHandle ("/hstreamdb/hstream/queries/" <> qid <> "/status") (Just (encode $ P.Status statusQ time)) Nothing

setConnectorStatus' :: HasCallStack => CBytes -> Int64 -> P.PStatus -> ZHandle -> IO ()
setConnectorStatus' qid time statusQ zkHandle = void $
  Z.zooSet zkHandle ("/hstreamdb/hstream/connectors/" <> qid <> "/status") (Just (encode $ P.Status statusQ time)) Nothing

spec :: Spec
spec = describe "HStream.RunPersistenceSpec" $ do
  zkHandle <- runIO $ do
    port <- CB.pack . fromMaybe "2181" <$> lookupEnv "SERVER_LOCAL_PORT"
    Z.zookeeperInit ("127.0.0.1" <> ":" <> port) 5000 Nothing 0
  runIO $ initZooKeeper zkHandle
  runIO $ P.getQueryIds zkHandle >>= mapM_ (flip (`P.removeQuery'` False) zkHandle)

  it "insert query" $
    ( do
        P.insertQuery "111" (P.Info "select * from demo emit changes" 111) (P.PlainQuery ["demo"]) zkHandle
        setQueryStatus' "111" 111111111111111 P.Created zkHandle
        info   <- decodeQ <$> Z.zooGet zkHandle "/hstreamdb/hstream/queries/111/details"
        extra  <- decodeQ <$> Z.zooGet zkHandle "/hstreamdb/hstream/queries/111/details/extra"
        status <- decodeQ <$> Z.zooGet zkHandle "/hstreamdb/hstream/queries/111/status"
        return $ P.Query "111" info extra status
    ) `shouldReturn` P.Query "111" (P.Info "select * from demo emit changes" 111) (P.PlainQuery ["demo"]) (P.Status P.Created 111111111111111)

  it "set and get query status" $
    ( do
        P.setQueryStatus "111" P.Terminated zkHandle
        P.getQueryStatus "111" zkHandle
    ) `shouldReturn` P.Terminated

  it "get query ids" $
    ( do
        P.insertQuery "222" (P.Info "select * from demo emit changes" 111) (P.PlainQuery ["demo"]) zkHandle
        P.insertQuery "333" (P.Info "select * from demo emit changes" 111) (P.PlainQuery ["demo"]) zkHandle
        P.getQueryIds zkHandle
    ) `shouldReturn` ["111","222","333"]

  it "remove query" $
    ( do
        P.removeQuery' "111" False zkHandle
        P.removeQuery' "222" False zkHandle
        P.removeQuery' "333" False zkHandle
        P.getQueryIds zkHandle
    ) `shouldReturn` []

  it "insert Connector" $
    ( do
        P.insertConnector "111" (P.Info "create sink connector ..." 111) zkHandle
        setConnectorStatus' "111" 111111111111111 P.Created zkHandle
        info   <- decodeQ <$> Z.zooGet zkHandle "/hstreamdb/hstream/connectors/111/details"
        status <- decodeQ <$> Z.zooGet zkHandle "/hstreamdb/hstream/connectors/111/status"
        return $ P.Connector "111" info status
    ) `shouldReturn` P.Connector "111" (P.Info "create sink connector ..." 111) (P.Status P.Created 111111111111111)

  it "set and get connector status" $
    ( do
        P.setConnectorStatus "111" P.Terminated zkHandle
        P.getConnectorStatus "111" zkHandle
    ) `shouldReturn` P.Terminated

  it "get connector ids" $
    ( do
        P.insertConnector "222" (P.Info "create sink connector ..." 111) zkHandle
        P.insertConnector "333" (P.Info "create sink connector ..." 111) zkHandle
        P.getConnectorIds zkHandle
    ) `shouldReturn` ["111","222","333"]

  it "remove connector" $
    ( do
        P.removeConnector "111" zkHandle
        P.removeConnector "222" zkHandle
        P.removeConnector "333" zkHandle
        P.getConnectorIds zkHandle
    ) `shouldReturn` []
