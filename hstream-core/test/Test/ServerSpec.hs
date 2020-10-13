{-# LANGUAGE CPP                 #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Test.ServerSpec (spec) where

import           Control.Exception     (SomeException, try)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString.Char8 as BSC
import           Data.Either           (isRight)
import           Data.List             (find)
import qualified Data.List             as L
import           Data.Maybe            (fromJust, fromMaybe, isJust)
import qualified Data.Vector           as V
import           HStream.LogStore.Base (EntryID)
import qualified HStream.Utils         as HStream
import qualified Network.HESP          as HESP
import           Network.Socket        (HostName, ServiceName)
import           System.Environment    (lookupEnv)
import           Test.Hspec
import           Text.Read             (readMaybe)

spec :: Spec
spec = do
#ifdef SERVER_TESTS
  connectionTest
  commandsTest
#else
  dummyTest
#endif

getHost :: IO HostName
getHost = fromMaybe "127.0.0.1" <$> lookupEnv "SERVER_HOST"

getPort :: IO ServiceName
getPort = fromMaybe "6560" <$> lookupEnv "SERVER_PORT"

------------------------------------------------------------------------------

dummyTest :: Spec
dummyTest = describe "Since SERVER_TESTS flag is undefined, we now run this dummy test" $ do
  it "always true" $ True `shouldBe` True

connectionTest :: Spec
connectionTest = describe "Connection" $ do
  it "Connect to default local server" $ do
    host <- getHost
    port <- getPort
    (r :: Either SomeException ()) <- try $ HESP.connect host port $ \_ -> return ()
    r `shouldSatisfy` isRight

commandsTest :: Spec
commandsTest = describe "Test supported commands" $ do
  it "xadd: single entry" $ do
    host <- getHost
    port <- getPort
    let req = mkXAddRequest "test_topic" ["key1", "key2"] ["value1", "value2"]
    HESP.connect host port $ \(sock, _) -> do
      HESP.sendMsg sock req
      msgs <- HESP.recvMsgs sock 1024
      (_, result) <- matchSingleEntryIDResp msgs
      return result
    `shouldReturn` "Passed"

  it "xadd: multiple entries" $ do
    host <- getHost
    port <- getPort
    let req = mkXAddRequest "test_topic" ["key1", "key2"] ["value1", "value2"]
    HESP.connect host port $ \(sock, _) -> do
      HESP.sendMsg sock req
      msgs1 <- HESP.recvMsgs sock 1024
      HESP.sendMsg sock req
      msgs2 <- HESP.recvMsgs sock 1024
      HESP.sendMsg sock req
      msgs3 <- HESP.recvMsgs sock 1024
      results@[(id1, _), (id2, _), (id3, _)] <- mapM matchSingleEntryIDResp [msgs1, msgs2, msgs3]
      case all (== "Passed") (snd <$> results) of
        False -> return . fromJust $ find (/= "Passed") (snd <$> results)
        True -> return $ if id1 < id2 && id2 < id3
                            then "Passed"
                            else "EntryIDs are not in order"
    `shouldReturn` "Passed"

  it "xrange: use '-' and '+'" $ do
    host <- getHost
    port <- getPort
    let req = mkXRangeRequest "test_topic" "-" "+" Nothing
    HESP.connect host port $ \(sock, _) -> do
      HESP.sendMsg sock req
      msgs <- HESP.recvMsgs sock 1024
      e_elements <- extractElements msgs
      case e_elements of
        Left s -> return s
        Right elements -> do
          let expected = HESP.mkArrayFromList (mkFields ["key1", "key2"] ["value1", "value2"])
          case L.all (== expected) (snd <$> elements) of
            True  -> return "Passed"
            False -> return "Data is incorrect or DB environment is not clean"
    `shouldReturn` "Passed"

  it "xrange: use certain entryID" $ do
    host <- getHost
    port <- getPort
    let req = mkXRangeRequest "test_topic" "0-0" "18446744073709551615-0" Nothing
    HESP.connect host port $ \(sock, _) -> do
      HESP.sendMsg sock req
      msgs <- HESP.recvMsgs sock 1024
      e_elements <- extractElements msgs
      case e_elements of
        Left s -> return s
        Right elements -> do
          let expected = HESP.mkArrayFromList (mkFields ["key1", "key2"] ["value1", "value2"])
          case L.all (== expected) (snd <$> elements) of
            True  -> return "Passed"
            False -> return "Data is incorrect or DB environment is not clean"
    `shouldReturn` "Passed"

  it "xrange: with count field" $ do
    host <- getHost
    port <- getPort
    let req = mkXRangeRequest "test_topic" "-" "+" (Just "3")
    HESP.connect host port $ \(sock, _) -> do
      HESP.sendMsg sock req
      msgs <- HESP.recvMsgs sock 1024
      e_elements <- extractElements msgs
      case e_elements of
        Left s -> return s
        Right elements ->
          case L.length elements of
            3 -> do
              let expected = HESP.mkArrayFromList (mkFields ["key1", "key2"] ["value1", "value2"])
              case L.all (== expected) (snd <$> elements) of
                True -> return "Passed"
                False -> return "Data is incorrect or DB environment is not clean"
            _ -> return "Incorrect number of returned elements"
    `shouldReturn` "Passed"

  it "mixed: xadd and xrange" $ do
    host <- getHost
    port <- getPort
    let xaddReq = mkXAddRequest "test_topic" ["key1", "key2"] ["value1", "value2"]
    HESP.connect host port $ \(sock, _) -> do
      HESP.sendMsg sock xaddReq
      msgs1 <- HESP.recvMsgs sock 1024
      (m_entryID, result) <- matchSingleEntryIDResp msgs1
      case m_entryID of
        Nothing -> return $ "XAdd: " <> result
        Just entryID -> do
          let xrangeReq = mkXRangeRequest "test_topic" (HStream.str2bs . show $ entryID) "+" Nothing
          HESP.sendMsg sock xrangeReq
          msgs2 <- HESP.recvMsgs sock 1024
          e_elements <- extractElements msgs2
          case e_elements of
            Left s -> return $ "XRange: " <> s
            Right elements ->
              case L.length elements of
                1 -> do
                  let expected = HESP.mkArrayFromList (mkFields ["key1", "key2"] ["value1", "value2"])
                  case (snd . L.head $ elements) == expected
                    && (fst . L.head $ elements) == (HStream.str2bs . show $ entryID) of
                    True  -> return "Passed"
                    False -> return "Data is incorrect"
                _ -> return $ "XRange: Incorrect number of returned elements"
   `shouldReturn` "Passed"

------------------------------------------------------------------------------

mkFields :: [ByteString] -> [ByteString] -> [HESP.Message]
mkFields [] _ = []
mkFields _ [] = []
mkFields (k:ks) (v:vs) = [ HESP.mkBulkString k
                         , HESP.mkBulkString v
                         ] ++ mkFields ks vs

mkXAddRequest :: ByteString -> [ByteString] -> [ByteString] -> HESP.Message
mkXAddRequest topic keys values =
  HESP.mkArrayFromList $ [ HESP.mkBulkString "xadd"
                         , HESP.mkBulkString topic
                         , HESP.mkBulkString "*"
                         ] ++ mkFields keys values

matchSingleEntryIDResp :: V.Vector (Either String HESP.Message) -> IO (Maybe EntryID, String)
matchSingleEntryIDResp msgs =
  case V.length msgs of
    0 -> return (Nothing, "Empty response")
    1 -> do
      case V.head msgs of
        Left _ -> return (Nothing, "Process failed on server side")
        Right msg -> case msg of
          HESP.MatchBulkString s -> do
            let m_entryID = (readMaybe (BSC.unpack s) :: Maybe EntryID)
            return $ if isJust m_entryID
                     then (m_entryID, "Passed")
                     else (Nothing, "Response is not a valid EntryID")
          _ -> return (Nothing, "Incorrect response format")
    _ -> return (Nothing, "Unexpected response length")

mkXRangeRequest :: ByteString -> ByteString -> ByteString -> Maybe ByteString -> HESP.Message
mkXRangeRequest topic sid eid m_count =
  case m_count of
    Nothing    -> HESP.mkArrayFromList baseList
    Just count ->
      HESP.mkArrayFromList $ baseList ++ [ HESP.mkBulkString "count"
                                         , HESP.mkBulkString count
                                         ]
  where
    baseList = [ HESP.mkBulkString "xrange"
               , HESP.mkBulkString topic
               , HESP.mkBulkString sid
               , HESP.mkBulkString eid
               ]

extractElements :: V.Vector (Either String HESP.Message) -> IO (Either String [(ByteString, HESP.Message)])
extractElements msgs = case V.length msgs of
  0 -> return $ Left "Empty response"
  1 -> do
    case V.head msgs of
      Left _ -> return $ Left "Process failed on server side"
      Right msg -> case msg of
        HESP.MatchArray arr ->
          return . Right . V.toList $ extract <$> arr
        _ -> return $ Left "Incorrect response format"
  _ -> return $ Left "Unexpected response length"
  where
    extract element = case element of
      HESP.MatchArray item -> (extractBulkString (item V.! 0), item V.! 1)
      _                    -> error "Unexpected element format"
    extractBulkString msg = case msg of
      HESP.MatchBulkString s -> s
      _                      -> error "Unexpected message format"
