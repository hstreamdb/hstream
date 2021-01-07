{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.PubSubSpec (spec) where

import           Control.Exception
import           Test.Hspec
import           Z.Data.Vector     (packASCII)

import qualified HStream.Store     as S
import qualified HStream.PubSub     as P

spec :: Spec
spec = describe "HStream.Store.PubSub" $ do
  it "create topic group" $
    (do _ <- S.setLoggerlevelError
        let topicid = S.mkTopicID 1
        client <- S.newStreamClient "/data/store/logdevice.conf"
        v <- P.createTopic client (P.Topic "a/a") 3
        case v of 
            Left _ -> return False 
            Right _ -> return True
    ) `shouldReturn` True

  it "pub and sub" $
    (do _ <- S.setLoggerlevelError
        let topicid = S.mkTopicID 1
        client <- S.newStreamClient "/data/store/logdevice.conf"
        Right reader <- P.sub client (P.Topic "a/a")
        P.pub client (P.Topic "a/a") "message"
        [v] <- P.poll reader 1
        return $ S.recordPayload v
    ) `shouldReturn` "message"


















