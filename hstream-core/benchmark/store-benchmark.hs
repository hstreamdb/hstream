{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict            #-}

import           Control.Monad.Trans          (lift)
import           Control.Monad.Trans.Reader   (ReaderT, runReaderT)
import           Control.Monad.Trans.Resource (MonadUnliftIO, allocate,
                                               runResourceT)
import           Criterion.Main
import qualified Data.ByteString              as B
import qualified Data.Vector                  as V
import           HStream.LogStore.Base
import           System.IO.Temp               (createTempDirectory)

main :: IO ()
main =
  defaultMain
    [ bgroup
        "append-single"
        [ bench "2^4 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 4) (2 ^ 20),
          bench "2^5 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 5) (2 ^ 20),
          bench "2^6 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 6) (2 ^ 20),
          bench "2^7 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 7) (2 ^ 20),
          bench "2^12 2^13" $
            nfIO $
              writeNBytesEntries (2 ^ 12) (2 ^ 13),
          bench "2^12 2^14" $
            nfIO $
              writeNBytesEntries (2 ^ 12) (2 ^ 14),
          bench "2^12 2^20" $
            nfIO $
              writeNBytesEntries (2 ^ 12) (2 ^ 20)
        ],
      bgroup
        "append-batch"
        [ bench "2^5 2^7 2^13" $
            nfIO $
              writeNBytesEntriesBatch (2 ^ 5) (2 ^ 7) (2 ^ 13),
          bench "2^6 2^7 2^13" $
            nfIO $
              writeNBytesEntriesBatch (2 ^ 6) (2 ^ 7) (2 ^ 13),
          bench "2^7 2^7 2^13" $
            nfIO $
              writeNBytesEntriesBatch (2 ^ 7) (2 ^ 7) (2 ^ 13)
        ],
      bgroup
        "read"
        [ bench "2^5 2^7 2^13" $
            nfIO $
              writeAndRead (2 ^ 5) (2 ^ 7) (2 ^ 13),
          bench "2^6 2^7 2^13" $
            nfIO $
              writeAndRead (2 ^ 6) (2 ^ 7) (2 ^ 13),
          bench "2^7 2^7 2^13" $
            nfIO $
              writeAndRead (2 ^ 7) (2 ^ 7) (2 ^ 13)
        ]
    ]

nBytesEntry :: Int -> B.ByteString
nBytesEntry n = B.replicate n 0xff

writeNBytesEntries :: MonadUnliftIO m => Int -> Int -> m ()
writeNBytesEntries entrySize entryNum =
  withLogStoreBench $ do
    lh <-
      open
        "log"
        defaultOpenOptions {writeMode = True, createIfMissing = True}
    write' lh 1
  where
    write' lh x =
      if x == entryNum
        then do
          _ <- appendEntry lh entry
          return ()
        else do
          _ <- appendEntry lh entry
          write' lh (x + 1)
          return ()
    entry = nBytesEntry entrySize

writeNBytesEntriesBatch :: MonadUnliftIO m => Int -> Int -> Int -> m ()
writeNBytesEntriesBatch entrySize batchSize batchNum =
  withLogStoreBench $ do
    lh <-
      open
        "log"
        defaultOpenOptions {writeMode = True, createIfMissing = True}
    write' lh 1
  where
    write' lh x =
      if x == batchNum
        then do
          _ <- appendEntries lh $ V.replicate batchSize entry
          return ()
        else do
          _ <- appendEntries lh $ V.replicate batchSize entry
          write' lh (x + 1)
          return ()
    entry = nBytesEntry entrySize

writeAndRead :: MonadUnliftIO m => Int -> Int -> Int -> m ()
writeAndRead entrySize batchSize batchNum =
  withLogStoreBench $ do
    lh <-
      open
        "log"
        defaultOpenOptions {writeMode = True, createIfMissing = True}
    _ <- write' lh 1
    _ <- readEntries lh Nothing Nothing
    return ()
  where
    write' lh x =
      if x == batchNum
        then appendEntries lh $ V.replicate batchSize entry
        else do
          _ <- appendEntries lh $ V.replicate batchSize entry
          write' lh (x + 1)
    entry = nBytesEntry entrySize

withLogStoreBench :: MonadUnliftIO m => ReaderT Context m a -> m a
withLogStoreBench r =
  runResourceT
    ( do
        _ <-
          createTempDirectory Nothing "log-store-bench"
        (_, ctx) <-
          allocate
            (initialize HStream.LogStore.Base.defaultConfig)
            (runReaderT shutDown)
        lift $ runReaderT r ctx
    )
