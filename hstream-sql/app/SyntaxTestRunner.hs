{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

#ifdef HStreamEnableSchema
import           Control.Exception
import qualified Data.IntMap       as IntMap
import qualified Data.Text         as T
import           HStream.SQL       (BoundDataType (..), ColumnCatalog (..),
                                    Schema (..), parseAndBind)
import           Lib               (HasRunTest (..), mkMain)
#else
import           Control.Exception
import qualified Data.Text         as T
import           HStream.SQL       (parseAndRefine)
import           Lib               (HasRunTest (..), mkMain)
#endif

#ifdef HStreamEnableSchema
main :: IO ()
main = mkMain SyntaxTestRunner

data SyntaxTestRunner = SyntaxTestRunner

instance HasRunTest SyntaxTestRunner where
  runTest _ x = do
    res <- try @SomeException $ parseAndBind x mockGetSchema
    pure $ case res of
      Right ok -> Right . T.pack $ show ok
      Left err -> Left . T.pack . head . lines . show $ err
  getTestName _ = "syntax"

mockGetSchema :: T.Text -> IO (Maybe Schema)
mockGetSchema s
  | s == "s"  = pure . Just $ Schema
                { schemaOwner = "s"
                , schemaColumns = IntMap.fromList
                  [ (0, ColumnCatalog { columnId         = 0
                                      , columnName       = "x"
                                      , columnType       = BTypeInteger
                                      , columnStream     = "s"
                                      , columnStreamId   = 0
                                      , columnIsNullable = True
                                      , columnIsHidden   = False
                                      })
                  , (1, ColumnCatalog { columnId         = 1
                                      , columnName       = "a"
                                      , columnType       = BTypeInteger
                                      , columnStream     = "s"
                                      , columnStreamId   = 0
                                      , columnIsNullable = True
                                      , columnIsHidden   = False
                                      })
                  , (2, ColumnCatalog { columnId         = 2
                                      , columnName       = "b"
                                      , columnType       = BTypeInteger
                                      , columnStream     = "s"
                                      , columnStreamId   = 0
                                      , columnIsNullable = True
                                      , columnIsHidden   = False
                                      })
                  ]
                }
  | s == "production_changes" = pure . Just $ Schema
                { schemaOwner = "s"
                , schemaColumns = IntMap.fromList
                  [ (0, ColumnCatalog { columnId         = 0
                                      , columnName       = "c"
                                      , columnType       = BTypeJsonb
                                      , columnStream     = "production_changes"
                                      , columnStreamId   = 0
                                      , columnIsNullable = True
                                      , columnIsHidden   = False
                                      })
                  ]
                }
  | otherwise = pure Nothing

#else

main :: IO ()
main = mkMain SyntaxTestRunner

data SyntaxTestRunner = SyntaxTestRunner

instance HasRunTest SyntaxTestRunner where
  runTest _ x = do
    res <- try @SomeException $ parseAndRefine x
    pure $ case res of
      Right ok -> Right . T.pack $ show ok
      Left err -> Left . T.pack . head . lines . show $ err
  getTestName _ = "syntax"
#endif
