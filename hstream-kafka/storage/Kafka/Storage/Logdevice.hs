{-# LANGUAGE PatternSynonyms #-}

module Kafka.Storage.Logdevice
  ( -- * Client
    LDClient
  , newLDClient
  , setClientSetting
  , LDLogLevel
  , trimLastBefore

    -- * Topic
  , StreamId (streamName)
  , StreamType (StreamTypeTopic)
  , C_LogID
  , createStream
  , createStreamPartition
  , listStreamPartitions
  , listStreamPartitionsOrderedByName
  , findStreams
  , doesStreamExist
  , isLogEmpty
  , transToTopicStreamName
  , getStreamIdFromLogId
  , showStreamName
  , logIdHasGroup
    -- ** LSN
  , LSN
  , pattern LSN_MIN
  , pattern LSN_MAX
  , pattern LSN_INVALID
  , getTailLSN
  , findKey
  , findTime
  , FindKeyAccuracy (FindKeyStrict)
  , pattern KeyTypeFindKey
    -- ** Attributes
  , removeStream
  , getLogHeadAttrsTrimPoint
  , getLogHeadAttrs
  , Attribute (attrValue)
  , LogAttributes (..)
  , getStreamLogAttrs
  , def
  , defAttr1
  , getLogTailAttrsLSN
  , getLogTailAttrs
  , getStreamExtraAttrs

    -- * Records
    -- ** Data record
  , DataRecord (..)
  , DataRecordAttr (..)
    -- ** Gap record
  , GapRecord (..)
  , GapType
  , pattern GapTypeUnknown
  , pattern GapTypeBridge
  , pattern GapTypeHole
  , pattern GapTypeDataloss
  , pattern GapTypeTrim
  , pattern GapTypeAccess
  , pattern GapTypeNotInConfig
  , pattern GapTypeFilteredOut
  , pattern GapTypeMax

    -- * Append
  , appendCompressedBS
  , Compression (..)
  , AppendCompletion (..)

    -- * Reader
  , LDReader
  , newLDReader
  , readerSetTimeout
  , readerSetWaitOnlyWhenNoData
  , readerStartReading
  , readerRead
  , readerReadSome
  , readerReadAllowGap
  , readerIsReading
  , readerStopReading

    -- * Checkpoint store
  , LDCheckpointStore
  , initOffsetCheckpointDir
  , allocOffsetCheckpointId
  , newRSMBasedCheckpointStore
  , ckpStoreUpdateMultiLSN
  , ckpStoreGetAllCheckpoints'
  , freeOffsetCheckpointId

    -- * Exception
  , NOTFOUND (..)
  , EXISTS (..)
  ) where


import           HStream.Store
import           HStream.Store.Internal.LogDevice
import           HStream.Store.Logger
