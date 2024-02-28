{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE RecordWildCards        #-}
{-# LANGUAGE TypeFamilies           #-}

module HStream.Kafka.Common.Resource where

import           Data.Char
import           Data.Maybe
import qualified Data.Set   as Set
import           Data.Text  (Text)
import qualified Data.Text  as T

--
class Matchable a b | a -> b, b -> a where
  match                   :: a -> b -> Bool
  matchAtMostOne          :: b -> Bool
  indefiniteFieldInFilter :: b -> Maybe Text
  toFilter                :: a -> b
  anyFilter               :: b

-- | A type of resource that can be applied to by an ACL, which is an 'Int8'
--   start from 0.
--   See org.apache.kafka.common.resource.ResourceType.
data ResourceType
  = Res_UNKNOWN
  | Res_ANY
  | Res_TOPIC
  | Res_GROUP
  | Res_CLUSTER
  | Res_TRANSACTIONAL_ID
  | Res_DELEGATION_TOKEN
  | Res_USER
  deriving (Eq, Ord)
instance Show ResourceType where
  show Res_UNKNOWN          = "UNKNOWN"
  show Res_ANY              = "ANY"
  show Res_TOPIC            = "TOPIC"
  show Res_GROUP            = "GROUP"
  show Res_CLUSTER          = "CLUSTER"
  show Res_TRANSACTIONAL_ID = "TRANSACTIONAL_ID"
  show Res_DELEGATION_TOKEN = "DELEGATION_TOKEN"
  show Res_USER             = "USER"
instance Read ResourceType where
  readsPrec _ s = case toUpper <$> s of
    "UNKNOWN"          -> [(Res_UNKNOWN, "")]
    "ANY"              -> [(Res_ANY, "")]
    "TOPIC"            -> [(Res_TOPIC, "")]
    "GROUP"            -> [(Res_GROUP, "")]
    "CLUSTER"          -> [(Res_CLUSTER, "")]
    "TRANSACTIONAL_ID" -> [(Res_TRANSACTIONAL_ID, "")]
    "DELEGATION_TOKEN" -> [(Res_DELEGATION_TOKEN, "")]
    "USER"             -> [(Res_USER, "")]
    _                  -> []
-- Note: a little different from derived 'Enum' instance.
--       indexes out of range is mapped to 'Res_UNKNOWN'.
instance Enum ResourceType where
  toEnum n | n == 0 = Res_UNKNOWN
           | n == 1 = Res_ANY
           | n == 2 = Res_TOPIC
           | n == 3 = Res_GROUP
           | n == 4 = Res_CLUSTER
           | n == 5 = Res_TRANSACTIONAL_ID
           | n == 6 = Res_DELEGATION_TOKEN
           | n == 7 = Res_USER
           | otherwise = Res_UNKNOWN
  fromEnum Res_UNKNOWN          = 0
  fromEnum Res_ANY              = 1
  fromEnum Res_TOPIC            = 2
  fromEnum Res_GROUP            = 3
  fromEnum Res_CLUSTER          = 4
  fromEnum Res_TRANSACTIONAL_ID = 5
  fromEnum Res_DELEGATION_TOKEN = 6
  fromEnum Res_USER             = 7

-- | A cluster resource which is a 2-tuple (type, name).
--   See org.apache.kafka.common.resource.Resource.
data Resource = Resource
  { resResourceType :: ResourceType
  , resResourceName :: Text
  }
instance Show Resource where
  show Resource{..} =
    "(resourceType=" <> show resResourceType <>
    ", name="        <> s_name               <> ")"
    where s_name = if T.null resResourceName then "<any>" else T.unpack resResourceName

-- [0..4]
-- | A resource pattern type, which is an 'Int8' start from 0.
--   WARNING: Be sure to understand the meaning of 'Pat_MATCH'.
--            A '(TYPE, "name", MATCH)' filter matches the following patterns:
--            1. All '(TYPE, "name", LITERAL)'
--            2. All '(TYPE, "*", LITERAL)'
--            3. All '(TYPE, "name", PREFIXED)'
--   See org.apache.kafka.common.resource.PatternType.
data PatternType
  = Pat_UNKNOWN
  | Pat_ANY
  | Pat_MATCH
  | Pat_LITERAL
  | Pat_PREFIXED
  deriving (Eq, Ord)
instance Show PatternType where
  show Pat_UNKNOWN  = "UNKNOWN"
  show Pat_ANY      = "ANY"
  show Pat_MATCH    = "MATCH"
  show Pat_LITERAL  = "LITERAL"
  show Pat_PREFIXED = "PREFIXED"
instance Read PatternType where
  readsPrec _ s = case toUpper <$> s of
    "UNKNOWN"  -> [(Pat_UNKNOWN, "")]
    "ANY"      -> [(Pat_ANY, "")]
    "MATCH"    -> [(Pat_MATCH, "")]
    "LITERAL"  -> [(Pat_LITERAL, "")]
    "PREFIXED" -> [(Pat_PREFIXED, "")]
    _          -> []
-- Note: a little different from derived 'Enum' instance.
--       indexes out of range is mapped to 'Pat_UNKNOWN'.
instance Enum PatternType where
  toEnum n | n == 0 = Pat_UNKNOWN
           | n == 1 = Pat_ANY
           | n == 2 = Pat_MATCH
           | n == 3 = Pat_LITERAL
           | n == 4 = Pat_PREFIXED
           | otherwise = Pat_UNKNOWN
  fromEnum Pat_UNKNOWN  = 0
  fromEnum Pat_ANY      = 1
  fromEnum Pat_MATCH    = 2
  fromEnum Pat_LITERAL  = 3
  fromEnum Pat_PREFIXED = 4

isPatternTypeSpecific :: PatternType -> Bool
isPatternTypeSpecific Pat_LITERAL  = True
isPatternTypeSpecific Pat_PREFIXED = True
isPatternTypeSpecific _            = False

-- | A pattern used by ACLs to match resources.
--   See org.apache.kafka.common.resource.ResourcePattern.
data ResourcePattern = ResourcePattern
  { resPatResourceType :: ResourceType -- | Can not be 'Res_ANY'
  , resPatResourceName :: Text -- | Can not be null but can be 'WILDCARD'
  , resPatPatternType  :: PatternType -- | Can not be 'Pat_ANY' or 'Pat_MATCH'
  } deriving (Eq)
instance Show ResourcePattern where
  show ResourcePattern{..} =
    "ResourcePattern(resourceType=" <> show resPatResourceType     <>
    ", name="                       <> T.unpack resPatResourceName <>
    ", patternType="                <> show resPatPatternType      <> ")"

-- FIXME then: design a proper serialization format for 'ResourcePattern'.
-- WARNING: the resource name may contain '_'!
-- | Convert a 'ResourcePattern' to a metadata key typed 'Text'.
--   A wildcard resource name "*" will be converted to "AnyResource".
resourcePatternToMetadataKey :: ResourcePattern -> Text
resourcePatternToMetadataKey ResourcePattern{..} =
  T.pack (show resPatPatternType)  <> "_" <>
  T.pack (show resPatResourceType) <> "_" <>
  (if resPatResourceName == wildcardResourceName
    then "AnyResource"
    else resPatResourceName
  )
-- WARNING: the resource name may contain '_'!
-- | Convert a metadata key typed 'Text' to a 'ResourcePattern'.
resourcePatternFromMetadataKey :: Text -> Maybe ResourcePattern
resourcePatternFromMetadataKey name =
  let (pat,_left1) = T.breakOn "_" name
      (typ,_left2) = T.breakOn "_" (T.drop 1 _left1)
      res          = T.drop 1 _left2
   in if T.null pat || T.null typ || T.null res
        then Nothing
        else Just $ ResourcePattern (read (T.unpack typ))
                                    (if res == "AnyResource"
                                      then wildcardResourceName
                                      else res)
                                    (read (T.unpack pat))

wildcardResourceName :: Text
wildcardResourceName = "*"

-- | Orders by resource type, then pattern type, and finally REVERSE name
--   See kafka.security.authorizer.
instance Ord ResourcePattern where
  compare p1 p2 = compare (resPatResourceType p1) (resPatResourceType p2)
               <> compare (resPatPatternType  p1) (resPatPatternType  p2)
               <> compare (resPatResourceName p2) (resPatResourceName p1)

-- | A filter that can match 'ResourcePattern'.
--   See org.apache.kafka.common.resource.ResourcePatternFilter.
data ResourcePatternFilter = ResourcePatternFilter
  { resPatFilterResourceType :: ResourceType
    -- | The resource type to match. If 'Res_ANY', ignore the resource type.
    --   Otherwise, only match patterns with the same resource type.
  , resPatFilterResourceName :: Text
    -- | The resource name to match. If null, ignore the resource name.
    --   If 'WILDCARD', only match wildcard patterns.
  , resPatFilterPatternType  :: PatternType
    -- | The resource pattern type to match.
    --   If 'Pat_ANY', match ignore the pattern type.
    --   If 'Pat_MATCH', see 'Pat_MATCH'.
    --   Otherwise, match patterns with the same pattern type.
  }
instance Show ResourcePatternFilter where
  show ResourcePatternFilter{..} =
    "ResourcePattern(resourceType=" <> show resPatFilterResourceType <>
    ", name="                       <> s_name                        <>
    ", patternType="                <> show resPatFilterPatternType  <> ")"
    where s_name = if T.null resPatFilterResourceName then "<any>" else T.unpack resPatFilterResourceName

instance Matchable ResourcePattern ResourcePatternFilter where
  -- See org.apache.kafka.common.resource.ResourcePatternFilter#matches
  match ResourcePattern{..} ResourcePatternFilter{..}
    | resPatFilterResourceType /= Res_ANY && resPatResourceType /= resPatFilterResourceType = False
    | resPatFilterPatternType /= Pat_ANY &&
      resPatFilterPatternType /= Pat_MATCH &&
      resPatFilterPatternType /= resPatPatternType = False
    | T.null resPatFilterResourceName = True
    | resPatFilterPatternType == Pat_ANY ||
      resPatFilterPatternType == resPatPatternType = resPatFilterResourceName == resPatResourceName
    | otherwise = case resPatPatternType of
        Pat_LITERAL  -> resPatFilterResourceName == resPatResourceName || resPatResourceName == wildcardResourceName
        Pat_PREFIXED -> T.isPrefixOf resPatFilterResourceName resPatResourceName
        _            -> error $ "Unsupported PatternType: "  <> show resPatPatternType -- FIXME: exception
  matchAtMostOne = isNothing . indefiniteFieldInFilter
  indefiniteFieldInFilter ResourcePatternFilter{..}
    | resPatFilterResourceType == Res_ANY     = Just "Resource type is ANY."
    | resPatFilterResourceType == Res_UNKNOWN = Just "Resource type is UNKNOWN."
    | T.null resPatFilterResourceName         = Just "Resource name is NULL."
    | resPatFilterPatternType == Pat_MATCH    = Just "Resource pattern type is MATCH."
    | resPatFilterPatternType == Pat_UNKNOWN  = Just "Resource pattern type is UNKNOWN."
    | otherwise                               = Nothing
  toFilter ResourcePattern{..} = ResourcePatternFilter
    { resPatFilterResourceType = resPatResourceType
    , resPatFilterResourceName = resPatResourceName
    , resPatFilterPatternType  = resPatPatternType
    }
  anyFilter = ResourcePatternFilter Res_ANY "" Pat_ANY

resourcePatternFromFilter :: ResourcePatternFilter -> Set.Set ResourcePattern
resourcePatternFromFilter ResourcePatternFilter{..} =
  case resPatFilterPatternType of
    Pat_LITERAL -> Set.singleton $ ResourcePattern
                                 { resPatResourceType = resPatFilterResourceType
                                 , resPatResourceName = resPatFilterResourceName
                                 , resPatPatternType  = resPatFilterPatternType
                                 }
    Pat_PREFIXED -> Set.singleton $ ResourcePattern
                                  { resPatResourceType = resPatFilterResourceType
                                  , resPatResourceName = resPatFilterResourceName
                                  , resPatPatternType  = resPatFilterPatternType
                                  }
    Pat_ANY -> Set.fromList [ ResourcePattern
                              { resPatResourceType = resPatFilterResourceType
                              , resPatResourceName = resPatFilterResourceName
                              , resPatPatternType  = Pat_LITERAL
                              }
                            , ResourcePattern
                              { resPatResourceType = resPatFilterResourceType
                              , resPatResourceName = resPatFilterResourceName
                              , resPatPatternType  = Pat_PREFIXED
                              }
                            ]
    _ -> error $ "Cannot determine matching resources for patternType "  <> show resPatFilterPatternType -- FIXME: exception
