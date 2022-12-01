{-# LANGUAGE TemplateHaskell #-}

module HStream.IO.Messages where

import qualified Data.Aeson          as J
import qualified Data.Aeson.TH       as JT
import           Data.Char           (toLower)
import qualified Data.Text           as T

import qualified HStream.Utils.Aeson as A

data InputCommand
  = InputCommandStop
  deriving (Show, Eq)

instance J.ToJSON InputCommand where
  toJSON InputCommandStop = J.object [(A.fromText "type") J..= ("stop" :: T.Text)]

data CheckResult
  = CheckResult
    { result  :: Bool
    , message :: T.Text
    }

$(JT.deriveJSON JT.defaultOptions ''CheckResult)

data ConnectorMessageType
  = CONNECTOR_CALL
  | CONNECTOR_SEND
  deriving (Show)

$(JT.deriveJSON
    JT.defaultOptions { JT.constructorTagModifier = map toLower }
    ''ConnectorMessageType)

data KvMessage
  = KvMessage
    { kmAction :: T.Text
    , kmKey    :: T.Text
    , kmValue  :: Maybe T.Text
    } deriving (Show)

$(JT.deriveJSON
    JT.defaultOptions
      { JT.fieldLabelModifier = map toLower . drop 2 }
    ''KvMessage)

data ConnectorMessage
  = ConnectorMessage
    { cmType    :: ConnectorMessageType
    , cmId      :: T.Text
    , cmMessage :: KvMessage
    } deriving (Show)

$(JT.deriveJSON
    JT.defaultOptions
      { JT.fieldLabelModifier = map toLower . drop 2 }
    ''ConnectorMessage)

data ConnectorCallResponse = ConnectorCallResponse
  { ccrId      :: T.Text
  , ccrMessage :: J.Value
  } deriving (Show)

instance J.ToJSON ConnectorCallResponse where
  toJSON ConnectorCallResponse{..} =
    J.object [ (A.fromText "type") J..= CONNECTOR_CALL
             , (A.fromText "id") J..= ccrId
             , (A.fromText "message") J..= ccrMessage
             ]
