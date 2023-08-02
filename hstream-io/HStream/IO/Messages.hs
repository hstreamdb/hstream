{-# LANGUAGE DeriveGeneric #-}

module HStream.IO.Messages where

import qualified Data.Aeson          as J
import           Data.Char           (toLower)
import qualified Data.Text           as T
import           GHC.Generics        (Generic)

import qualified Data.Vector         as Vector
import qualified HStream.Utils.Aeson as A

-- Custom Aeson instances options
customOptions :: Int -> J.Options
customOptions n = J.defaultOptions{J.fieldLabelModifier = map toLower . drop n}

-- ============ Connector -> Server
newtype KvGetMessage
  = KvGetMessage
    { kgKey    :: T.Text
    } deriving (Show, Generic)

instance J.FromJSON KvGetMessage where
  parseJSON = J.genericParseJSON (customOptions 2)

instance J.ToJSON KvGetMessage where
  toEncoding = J.genericToEncoding (customOptions 2)

data KvSetMessage
  = KvSetMessage
    { ksKey   :: T.Text
    , ksValue :: T.Text
    } deriving (Show, Generic)

instance J.FromJSON KvSetMessage where
  parseJSON = J.genericParseJSON (customOptions 2)

instance J.ToJSON KvSetMessage where
  toEncoding = J.genericToEncoding (customOptions 2)

data ReportMessage = ReportMessage
  { deliveredRecords :: Int
  , deliveredBytes   :: Int
  , offsets          :: Vector.Vector J.Object
  } deriving (Show, Generic)

instance J.FromJSON ReportMessage
instance J.ToJSON ReportMessage

data ConnectorMessage
  = KvGet KvGetMessage
  | KvSet KvSetMessage
  | Report ReportMessage
  deriving (Show)

data ConnectorRequest
  = ConnectorRequest
    { crId      :: T.Text
    , crMessage :: ConnectorMessage
    } deriving (Show)

instance J.FromJSON ConnectorRequest where
  parseJSON = J.withObject "ConnectorRequest" $ \v -> ConnectorRequest
    <$> v J..: "id"
    <*> (v J..: "name" >>= \case
          ("KvGet" :: T.Text) -> KvGet <$> (v J..: "body")
          "KvSet" -> KvSet <$> (v J..: "body")
          "Report" -> Report <$> (v J..: "body")
          name -> fail $ "Unknown Connector Request:" ++ T.unpack name
        )

data ConnectorResponse = ConnectorResponse
  { ccrId      :: T.Text
  , ccrMessage :: J.Value
  } deriving (Show)

instance J.ToJSON ConnectorResponse where
  toJSON ConnectorResponse{..} =
    J.object [ A.fromText "name" J..= ("ConnectorResponse" :: T.Text)
             , A.fromText "id" J..= ccrId
             , A.fromText "body" J..= ccrMessage
             ]

-- ========== Server -> Connector
data InputCommand
  = InputCommandStop
  deriving (Show, Eq)

instance J.ToJSON InputCommand where
  toJSON InputCommandStop = J.object [A.fromText "name" J..= ("stop" :: T.Text)]

-- =========

data CheckResult
  = CheckResult
    { crtResult  :: Bool
    , crtType    :: Maybe T.Text
    , crtMessage :: Maybe T.Text
    , crtDetail  :: Maybe J.Value
    } deriving (Show, Generic)

instance J.FromJSON CheckResult where
  parseJSON = J.genericParseJSON (customOptions 3)

instance J.ToJSON CheckResult where
  toEncoding = J.genericToEncoding (customOptions 3)
