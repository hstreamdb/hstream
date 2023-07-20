{-# LANGUAGE CPP #-}

module HStream.SQL
  ( module HStream.SQL.Abs
  , module HStream.SQL.Rts

-- frontend: AST or Binder-Planner
-- --------------------------------------
#ifdef HStreamEnableSchema
  , module HStream.SQL.ParseNew
  , module HStream.SQL.Binder
  , module HStream.SQL.PlannerNew
  , module HStream.SQL.Codegen.V1New
-- --------------------------------------
#else
  , module HStream.SQL.Parse
  , module HStream.SQL.AST
  , module HStream.SQL.Planner
-- codegen: V1 or V2
-- +++++++++++++++
#ifdef HStreamUseV2Engine
  , module HStream.SQL.Codegen.V2
-- +++++++++++++++
#else
  , module HStream.SQL.Codegen.V1
#endif
-- +++++++++++++++
#endif
-- --------------------------------------
  ) where

import           HStream.SQL.Abs
import           HStream.SQL.Rts

-- frontend: AST or Binder-Planner
-- --------------------------------------
#ifdef HStreamEnableSchema
import           HStream.SQL.Binder        hiding (lookupColumn)
import           HStream.SQL.Codegen.V1New
import           HStream.SQL.ParseNew
import           HStream.SQL.PlannerNew    hiding (lookupColumn)
-- --------------------------------------
#else
import           HStream.SQL.AST
import           HStream.SQL.Parse
import           HStream.SQL.Planner
-- codegen: V1 or V2
-- +++++++++++++++
#ifdef HStreamUseV2Engine
import           HStream.SQL.Codegen.V2
-- +++++++++++++++
#else
import           HStream.SQL.Codegen.V1
#endif
-- +++++++++++++++
#endif
-- --------------------------------------
