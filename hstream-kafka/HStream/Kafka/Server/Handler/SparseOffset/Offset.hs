{-# LANGUAGE PatternSynonyms #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module HStream.Kafka.Server.Handler.SparseOffset.Offset
  ( handleListOffsetsSparseOffset
  ) where

#define HSTREAM_SPARSE_OFFSET
#include "HStream/Kafka/Server/Handler/Offset.hs"
#undef HSTREAM_SPARSE_OFFSET
