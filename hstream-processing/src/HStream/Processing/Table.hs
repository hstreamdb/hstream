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

data Table k v
  = Table
      { tableKeySerde :: Maybe (Serde k),
        tableValueSerde :: Maybe (Serde v),
        tableProcessorName :: T.Text,
        tableStoreName :: T.Text,
        tableInternalBuilder :: InternalStreamBuilder
      }

toStream ::
  (Typeable k, Typeable v) =>
  Table k v ->
  IO (Stream k v)
toStream Table {..} =
  return $
    mkStream tableKeySerde tableValueSerde tableProcessorName tableInternalBuilder
