{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Codegen.SKey where

import           Control.Exception
import qualified Data.HashMap.Strict   as HM
import qualified Data.List             as L
import           Data.Text             (Text)
import           GHC.Stack
import           HStream.SQL.AST
import           HStream.SQL.Exception

constantKeygen :: FlowObject -> FlowObject
constantKeygen _ =
  HM.fromList [(SKey "key" Nothing (Just "__reduce_key__"), FlowText "__constant_key__")]

alwaysTrueJoinCond :: FlowObject -> FlowObject -> Bool
alwaysTrueJoinCond _ _ = True

makeExtra :: Text -> FlowObject -> FlowObject
makeExtra extra =
  HM.mapKeys (\(SKey f s_m _) -> SKey f s_m (Just extra))

getExtra :: Text -> FlowObject -> FlowObject
getExtra extra =
  HM.filterWithKey (\(SKey _ _ extra_m) v -> extra_m == Just extra)

discardExtra :: Text -> FlowObject -> FlowObject
discardExtra extra =
  HM.filterWithKey (\(SKey _ _ extra_m) v -> extra_m /= Just extra)

getExtraAndReset :: Text -> FlowObject -> FlowObject
getExtraAndReset extra o =
  HM.mapKeys (\(SKey f s_m _) -> SKey f s_m Nothing) $
  HM.filterWithKey (\(SKey f s_m extra_m) v -> extra_m == Just extra) o

getField :: Text -> Maybe Text -> Maybe Text -> FlowObject -> Maybe (SKey, FlowValue)
getField k stream_m extra_m o =
  let filterCond = case stream_m of
        Nothing -> \(SKey f _ _) _ -> f == k
        Just s  -> \(SKey f s_m _) _ -> f == k && s_m == stream_m
   in case HM.toList (HM.filterWithKey filterCond o) of
        []         -> Nothing
        [(skey,v)] -> Just (skey, v)
        xs         ->
          case L.filter (\(SKey _ _ extra_, _) -> extra_ == extra_m) xs of
            []         -> Nothing
            [(skey,v)] -> Just (skey, v)
            _          -> throw
                          SomeRuntimeException
                          { runtimeExceptionMessage = "!!! Ambiguous field name with different <stream> and/or <extra>: " <> show xs <> ": " <> show callStack
                          , runtimeExceptionCallStack = callStack
                          }

getField' :: Text -> Maybe Text -> Maybe Text -> FlowObject -> (SKey, FlowValue)
getField' k stream_m extra_m o =
  case getField k stream_m extra_m o of
    Nothing        -> (SKey k stream_m Nothing, FlowNull)
    Just (skey, v) -> (skey, v)

makeSKeyStream :: Text -> FlowObject -> FlowObject
makeSKeyStream stream =
  HM.mapKeys (\(SKey f _ extra_m) -> SKey f (Just stream) extra_m)
