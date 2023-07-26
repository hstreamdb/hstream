module Slt.Plan where

import qualified Data.Text       as T
import           Slt.Format
import           Slt.Utils       (SqlDataType)
import           Text.Megaparsec (many)
import qualified Text.Megaparsec as P

----------------------------------------
-- Slt Suite & Plan
----------------------------------------

newtype SltSuite = SltSuite
  { plan :: [Plan]
  }
  deriving (Show)

pSlt, pSlt' :: Parser SltSuite
pSlt = scn *> pSlt' <* P.eof
pSlt' = do
  plan <- many pPlan
  pure $
    SltSuite
      { plan = plan
      }

----------------------------------------

data Plan
  = PlanOk T.Text
  | PlanError T.Text (Maybe T.Text)
  | PlanRandomNoTable RandomNoTablePlan
  deriving (Show)

pPlan :: Parser Plan
pPlan = PlanRandomNoTable <$> pPlanRandomNoTable

----------------------------------------
-- RandomNoTablePlan
----------------------------------------

data RandomNoTablePlan = RandomNoTablePlan
  { colInfo :: ColInfo,
    rowNum  :: Maybe Int,
    sql     :: T.Text
  }
  deriving (Show)

pPlanRandomNoTable :: Parser RandomNoTablePlan
pPlanRandomNoTable = do
  _ <- symbol "random" <* symbol "no-table"
  rowNum <- P.optional $ pIntOpt "row-num"
  requireNewline
  colInfo <- pColInfo
  sql <- takeToNewline1
  pure $
    RandomNoTablePlan
      { colInfo,
        rowNum,
        sql
      }

----------------------------------------
-- Common
----------------------------------------

newtype ColInfo = ColInfo
  { unColInfo :: [(T.Text, SqlDataType)]
  }
  deriving (Show)

pColInfo :: Parser ColInfo
pColInfo = ColInfo <$> pTypeInfo

----------------------------------------
-- Misc
----------------------------------------

parseTest :: FilePath -> IO ()
parseTest src = P.parseTest pSlt . T.pack =<< readFile src

parsePlan :: FilePath -> IO SltSuite
parsePlan file = do
  src <- T.pack <$> readFile file
  case P.parse pSlt [] src of
    Left err -> error $ show err
    Right ok -> pure ok
