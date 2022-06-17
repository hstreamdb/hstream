{-# LANGUAGE TemplateHaskell #-}

module HStream.IO.Types where

import qualified Data.Text as T
import qualified Data.Aeson as J

import qualified Data.Aeson.TH as JT
import Data.Char (toLower)

data ConfiguratedCatalogStream
  = ConfiguratedCatalogStream {
      ccsStream :: J.Object,
      ccsSync_Mode :: T.Text,
      ccsDestination_Sync_Mode :: T.Text
    }
JT.deriveJSON 
    JT.defaultOptions { JT.fieldLabelModifier = map toLower . drop 3 } 
    ''ConfiguratedCatalogStream


newtype ConfiguratedCatalog
  = ConfiguratedCatalog
    { ccStreams :: [ConfiguratedCatalogStream]
    }
JT.deriveJSON 
    JT.defaultOptions { JT.fieldLabelModifier = map toLower . drop 2 } 
    ''ConfiguratedCatalog

makeConfiguratedCatalog :: AirbyteCatalogMessage -> T.Text -> T.Text -> ConfiguratedCatalog
makeConfiguratedCatalog AirbyteCatalogMessage{..} read_mode write_mode 
  = ConfiguratedCatalog { ccStreams = ns }
  where
    ns = map (\v -> ConfiguratedCatalogStream v read_mode write_mode) streams

newtype AirbyteCatalogMessage
  = AirbyteCatalogMessage {
      streams :: [J.Object]
    }
JT.deriveJSON JT.defaultOptions ''AirbyteCatalogMessage

newtype AirbyteStateMessage
  = AirbyteStateMessage { asmData :: J.Value }
JT.deriveJSON 
    JT.defaultOptions { JT.fieldLabelModifier = map toLower . drop 3 } 
    ''AirbyteStateMessage


data AirbyteRecordMessage
  = AirbyteRecordMessage { 
      armStream :: T.Text,
      armData :: J.Object,
      armEmitted_at :: Int
    }
JT.deriveJSON 
    JT.defaultOptions 
      { JT.fieldLabelModifier = map toLower . drop 3
      , JT.omitNothingFields = True
      } 
    ''AirbyteRecordMessage

data AirbyteLogMessage
  = AirbyteLogMessage { 
      almLevel :: T.Text,
      almMessage :: T.Text
    }
JT.deriveJSON 
    JT.defaultOptions 
      { JT.fieldLabelModifier = map toLower . drop 3
      , JT.omitNothingFields = True
      } 
    ''AirbyteLogMessage

data AirbyteMessage
  = AMLog AirbyteLogMessage
  | AMState AirbyteStateMessage
  | AMCatalog AirbyteCatalogMessage
  | AMRecord AirbyteRecordMessage

instance J.FromJSON AirbyteMessage where
  parseJSON = J.withObject "airbyteMessage" $ \v ->
    v J..: "type"
      >>= \case ("LOG" :: String) -> AMLog <$> v J..: "log"
                "STATE" -> AMState <$> v J..: "state"
                "CATALOG" -> AMCatalog <$> v J..: "catalog"
                "RECORD" -> AMRecord <$> v J..: "record"
                _ -> fail "invalid type messages"

instance J.ToJSON AirbyteMessage where
  toJSON (AMLog m) = J.object ["type" J..= ("LOG" :: T.Text), "log" J..= m]
  toJSON (AMState m) = J.object ["type" J..= ("STATE"::T.Text), "state" J..= m]
  toJSON (AMCatalog m) = J.object ["type" J..= ("CATALOG"::T.Text), "catalog" J..= m]
  toJSON (AMRecord m) = J.object ["type" J..= ("RECORD"::T.Text), "record" J..= m]

-- hstream-io state messages
newtype IOStateMessage
  = IOStateMessage { ismHStreamIOStateType :: T.Text }
JT.deriveJSON 
    JT.defaultOptions { JT.fieldLabelModifier = drop 3 } 
    ''IOStateMessage
