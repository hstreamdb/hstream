{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.RunPersistentSpec where

import           Control.Exception
import           Control.Monad              (void)
import           Data.Int                   (Int64)
import           GHC.Stack                  (HasCallStack)
import           HStream.Server.Exception   (FailedToDecode (FailedToDecode))
import qualified HStream.Server.Persistence as P
import           Z.Data.CBytes              (CBytes)
import           Z.Data.JSON                (JSON, decode, encode)
import qualified ZooKeeper                  as Z
import           ZooKeeper.Exception        (ZNODEEXISTS)
import           ZooKeeper.Types            (DataCompletion (dataCompletionValue),
                                             ZHandle)

import           Test.Hspec                 (Spec, describe, it, runIO,
                                             shouldReturn)

zkUriDefault :: CBytes
zkUriDefault = "127.0.0.1:2181"

initZooKeeper :: ZHandle -> IO ()
initZooKeeper zk = catch (P.initializeAncestors zk) (\(_ :: ZNODEEXISTS) -> pure ())

decodeQ :: JSON a => DataCompletion -> a
decodeQ = (\case { Right x -> x ; _ -> throw FailedToDecode}) . snd . decode
        . (\case { Nothing -> ""; Just x -> x}) . dataCompletionValue

setQueryStatus' :: HasCallStack => CBytes -> Int64 -> P.PStatus -> ZHandle -> IO ()
setQueryStatus' qid time statusQ zkHandle = void $
  Z.zooSet zkHandle ("/hstreamdb/hstream/queries/" <> qid <> "/status") (Just (encode $ P.Status statusQ time)) Nothing

spec :: Spec
spec = describe "HStream.RunSQLSpec" $ do
  zkHandle <- runIO $ Z.zookeeperInit zkUriDefault 5000 Nothing 0
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
