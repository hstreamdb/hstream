{-# LANGUAGE PatternSynonyms #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module HStream.Kafka.Server.Handler.SparseOffset.Consume
  ( handleFetchSparseOffset
  ) where

#define HSTREAM_SPARSE_OFFSET
#include "HStream/Kafka/Server/Handler/Consume.hs"
#undef HSTREAM_SPARSE_OFFSET
