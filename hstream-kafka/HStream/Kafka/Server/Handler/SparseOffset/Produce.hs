{-# LANGUAGE PatternSynonyms #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module HStream.Kafka.Server.Handler.SparseOffset.Produce
  ( handleProduceSparseOffset
  ) where

#define HSTREAM_SPARSE_OFFSET
#include "HStream/Kafka/Server/Handler/Produce.hs"
#undef HSTREAM_SPARSE_OFFSET
