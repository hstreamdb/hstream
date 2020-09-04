{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module Test.LogStoreSpec (spec) where

import           Control.Concurrent.Async.Lifted.Safe (async, wait)
import           Control.Monad                        (void)
import           Control.Monad.IO.Class               (MonadIO)
import           Control.Monad.Reader                 (ReaderT, runReaderT)
import           Control.Monad.Trans.Class            (lift)
import           Control.Monad.Trans.Resource         (MonadUnliftIO, allocate,
                                                       runResourceT)
import qualified Data.ByteString.Char8                as C
import qualified Data.ByteString.UTF8                 as U
import qualified Data.Foldable                        as F
import qualified Data.Vector                          as V
import           System.IO.Temp                       (createTempDirectory)
import           Test.Hspec

import           HStream.LogStore.Base
import           HStream.LogStore.Utils               (lastElemInSeq)

spec :: Spec
spec =
  describe "Basic Functionality" $
    do
      it "open logs for creating" $
        withLogStoreTest
          ( do
              void $ open "log1" defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ open "log2" defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ open "log3" defaultOpenOptions {writeMode = True, createIfMissing = True}
              return ("success" :: String)
          )
          `shouldReturn` "success"
      it "open an existent log" $
        withLogStoreTest
          ( do
              void $ open "log" defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ open
                "log"
                defaultOpenOptions {writeMode = True}
              return ("success" :: String)
          )
          `shouldReturn` "success"
      it "put an entry to a log" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntry logHandle "entry"
              return ("success" :: String)
          )
          `shouldReturn` "success"
      it "put some entries to a log" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntry logHandle "entry1"
              void $ appendEntry logHandle "entry2"
              void $ appendEntry logHandle "entry3"
              return ("success" :: String)
          )
          `shouldReturn` "success"
      it "put some entries to multiple logs" $
        withLogStoreTest
          ( do
              lh1 <-
                open
                  "log1"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $appendEntry lh1 "log1-entry1"
              void $ appendEntry lh1 "log1-entry2"
              void $ appendEntry lh1 "log1-entry3"

              lh2 <-
                open
                  "log2"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntry lh2 "log2-entry1"
              void $ appendEntry lh2 "log2-entry2"
              void $ appendEntry lh2 "log2-entry3"

              lh3 <-
                open
                  "log3"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntry lh3 "log3-entry1"
              void $ appendEntry lh3 "log3-entry2"
              void $ appendEntry lh3 "log3-entry3"

              return ("success" :: String)
          )
          `shouldReturn` "success"
      it "put an entry to a log and read it" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              _entryId <- appendEntry logHandle "entry"
              r <- readEntries logHandle Nothing Nothing
              return $ fmap snd (F.toList r)
          )
          `shouldReturn` ["entry"]
      it "put some entries to a log and read them" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntry logHandle "entry1"
              void $ appendEntry logHandle "entry2"
              void $ appendEntry logHandle "entry3"
              r <- readEntries logHandle Nothing Nothing
              return $ fmap snd (F.toList r)
          )
          `shouldReturn` ["entry1", "entry2", "entry3"]
      it "put some entries to multiple logs and read them (1)" $
        withLogStoreTest
          ( do
              lh1 <-
                open
                  "log1"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntry lh1 "log1-entry1"
              void $ appendEntry lh1 "log1-entry2"
              void $ appendEntry lh1 "log1-entry3"

              lh2 <-
                open
                  "log2"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntry lh2 "log2-entry1"
              void $ appendEntry lh2 "log2-entry2"
              void $ appendEntry lh2 "log2-entry3"

              lh3 <-
                open
                  "log3"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntry lh3 "log3-entry1"
              void $ appendEntry lh3 "log3-entry2"
              void $ appendEntry lh3 "log3-entry3"

              r1 <- F.toList <$> readEntries lh1 Nothing Nothing
              r2 <- F.toList <$> readEntries lh2 Nothing Nothing
              r3 <- F.toList <$> readEntries lh3 Nothing Nothing
              return [fmap snd r1, fmap snd r2, fmap snd r3]
          )
          `shouldReturn` [ ["log1-entry1", "log1-entry2", "log1-entry3"],
                           ["log2-entry1", "log2-entry2", "log2-entry3"],
                           ["log3-entry1", "log3-entry2", "log3-entry3"]
                         ]
      it "put some entries to multiple logs and read them (2)" $
        withLogStoreTest
          ( do
              lh1 <-
                open
                  "log1"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              lh2 <-
                open
                  "log2"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntry lh2 "log2-entry1"
              void $ appendEntry lh2 "log2-entry2"
              void $ appendEntry lh2 "log2-entry3"
              F.toList <$> readEntries lh1 Nothing Nothing
          )
          `shouldReturn` []
      it "append entries to a log and read them " $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntries logHandle (V.replicate 3 (U.fromString "entry"))
              r <- readEntries logHandle Nothing Nothing
              return $ fmap snd (F.toList r)
          )
          `shouldReturn` generateReadResult 3 ""
      it "append entry repeatly to a log and read them" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntryRepeat 300 logHandle ""
              res <- F.toList <$> readEntries logHandle Nothing Nothing
              return $ map snd res
          )
          `shouldReturn` generateReadResult 300 ""
      it "readEntriesByCount (1)" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntryRepeat 300 logHandle ""
              res <- F.toList <$> readEntriesByCount logHandle Nothing 10
              return $ map snd res
          )
          `shouldReturn` generateReadResult 10 ""
      it "readEntriesByCount (2)" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntryRepeat 300 logHandle ""
              res <- F.toList <$> readEntriesByCount logHandle Nothing 500
              return $ map snd res
          )
          `shouldReturn` generateReadResult 300 ""
      it "readEntriesByCount (3)" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntryRepeat 300 logHandle ""
              r1 <- readEntriesByCount logHandle Nothing 10
              r2 <- readEntriesByCount logHandle (Just $ fst $ lastElemInSeq r1) 50
              return $ map snd $ F.toList r2
          )
          `shouldReturn` generateReadResult 50 ""
      it "show and read entryId" $
        withLogStoreTest
          ( do
              logHandle <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              entryId <- appendEntry logHandle "entry"
              return $ entryId == read (show entryId)
          )
          `shouldReturn` True
      it "multiple open" $
        withLogStoreTest
          ( do
              lh1 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntryRepeat 300 lh1 ""
              lh2 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              void $ appendEntryRepeat 300 lh2 ""
              lh3 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              res <- F.toList <$> readEntries lh3 Nothing Nothing
              return $ map snd res
          )
          `shouldReturn` generateReadResult 600 ""
      it "sequencial open the same log should return the same logHandle" $
        withLogStoreTest
          ( do
              lh1 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              lh2 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              lh3 <-
                open
                  "log"
                  defaultOpenOptions {writeMode = True, createIfMissing = True}
              return $ lh1 == lh2 && lh1 == lh3
          )
          `shouldReturn` True
      it "concurrent open the same log should return the same logHandle" $
        withLogStoreTest
          ( do
              void $ open
                "log"
                defaultOpenOptions {writeMode = True, createIfMissing = True}
              c1 <-
                async
                  ( open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  )
              c2 <-
                async
                  ( open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  )
              c3 <-
                async
                  ( open
                      "log"
                      defaultOpenOptions {writeMode = True, createIfMissing = True}
                  )
              r1 <- wait c1
              r2 <- wait c2
              _r3 <- wait c3
              return $ r1 == r2 && r1 == r2
          )
          `shouldReturn` True
      it "concurrent open, append and read different logs" $
        withLogStoreTest
          ( do
              c1 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log1"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      void $ appendEntryRepeat 3 logHandle "l1"
                      F.toList <$> readEntries logHandle Nothing Nothing
                  )
              c2 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log2"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      void $ appendEntryRepeat 3 logHandle "l2"
                      F.toList <$> readEntries logHandle Nothing Nothing
                  )
              c3 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log3"
                          defaultOpenOptions {writeMode = True, createIfMissing = True}
                      void $ appendEntryRepeat 3 logHandle "l3"
                      F.toList <$> readEntries logHandle Nothing Nothing
                  )
              r1 <- wait c1
              r2 <- wait c2
              r3 <- wait c3
              return [fmap snd r1, fmap snd r2, fmap snd r3]
          )
          `shouldReturn` [generateReadResult 3 "l1", generateReadResult 3 "l2", generateReadResult 3 "l3"]
      it "concurrent append to the same log" $
        withLogStoreTest
          ( do
              void $ open
                "log"
                defaultOpenOptions {writeMode = True, createIfMissing = True}
              c1 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log"
                          defaultOpenOptions {writeMode = True}
                      void $ appendEntryRepeat 300 logHandle ""
                      return logHandle
                  )
              c2 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log"
                          defaultOpenOptions {writeMode = True}
                      void $ appendEntryRepeat 300 logHandle ""
                      return logHandle
                  )
              c3 <-
                async
                  ( do
                      logHandle <-
                        open
                          "log"
                          defaultOpenOptions {writeMode = True}
                      void $ appendEntryRepeat 300 logHandle ""
                      return logHandle
                  )
              void $ wait c1
              void $ wait c2
              lh <- wait c3
              res <- F.toList <$> readEntries lh Nothing Nothing
              return $ map snd res
          )
          `shouldReturn` generateReadResult 900 ""

-- | append n entries to a log
appendEntryRepeat :: MonadIO m => Int -> LogHandle -> String -> ReaderT Context m EntryID
appendEntryRepeat n lh entryPrefix = append' 1
  where
    append' x =
      if (x == n)
        then do
          id' <- appendEntry lh $ C.pack $ entryPrefix ++ testEntryContent
          -- liftIO $ print id
          return id'
        else do
          _id <- appendEntry lh $ C.pack $ entryPrefix ++ testEntryContent
          -- liftIO $ print id
          append' (x + 1)

-- | help run test case
-- | wrap create temp directory
withLogStoreTest :: MonadUnliftIO m => ReaderT Context m a -> m a
withLogStoreTest r =
  runResourceT
    ( do
        (_, path) <-
          createTempDirectory Nothing "log-store-test"
        (_, ctx) <-
          allocate
            ( initialize defaultConfig {rootDbPath = path}
            )
            (runReaderT shutDown)
        lift $ runReaderT r ctx
    )

generateReadResult :: Int -> String -> [Entry]
generateReadResult num entryPrefix = replicate num (C.pack $ entryPrefix ++ testEntryContent)

testEntryContent :: String
testEntryContent = "entry"
