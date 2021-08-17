{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Table
  ( Table (..),
    toStream,
  )
where

import           HStream.Processing.Encoding
import           HStream.Processing.Stream.Internal
import           RIO
import qualified RIO.Text                           as T

data Table k v s = Table
  { tableKeySerde :: Maybe (Serde k s),
    tableValueSerde :: Maybe (Serde v s),
    tableProcessorName :: T.Text,
    tableStoreName :: T.Text,
    tableInternalBuilder :: InternalStreamBuilder
  }

toStream ::
  (Typeable k, Typeable v) =>
  Table k v s ->
  IO (Stream k v s)
toStream Table {..} =
  return $
    mkStream tableKeySerde tableValueSerde tableProcessorName tableInternalBuilder
