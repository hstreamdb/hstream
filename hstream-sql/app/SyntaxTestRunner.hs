{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import           Control.Exception
import           Control.Monad
import qualified Data.Aeson            as A
import qualified Data.ByteString.Char8 as BS
import           Data.Either
import           Data.Foldable
import           Data.Functor
import           Data.List             (intercalate)
import           Data.Maybe
import qualified Data.Text             as T
import qualified Data.Vector           as V
import qualified Data.Yaml             as Y
import           GHC.Generics
import           HStream.SQL.Parse     (parseAndRefine)
import           Options.Applicative
import           System.IO

main :: IO ()
main = do
  Opts {inplace, optCmd, files} <- execParser parseArgs
  case optCmd of
    Run -> getThenRunTestSuite files
    Gen -> getThenGenTestSuite files inplace

data Opts = Opts
  { inplace :: !Bool,
    optCmd  :: !Cmd,
    files   :: ![FilePath]
  }

parseArgs :: ParserInfo Opts
parseArgs =
  info
    (helper <*> parseOpts)
    fullDesc

parseOpts :: Parser Opts
parseOpts =
  Opts
    <$> switch (short 'i' <> help "Inplace edit <file>s, if specified.")
    <*> hsubparser
      ( cmdRun <> cmdGen
      )
    <*> some (strArgument @FilePath mempty)

data Cmd = Run | Gen

cmdRun ::
  Mod
    CommandFields
    Cmd
cmdRun =
  command
    "run"
    $ info parseCmdRun
    $ progDesc "Run all syntax tests"

parseCmdRun :: Parser Cmd
parseCmdRun = pure Run

cmdGen ::
  Mod
    CommandFields
    Cmd
cmdGen =
  command
    "gen"
    $ info parseCmdGen
    $ progDesc "Generate or update all syntax tests results"

parseCmdGen :: Parser Cmd
parseCmdGen = pure Gen

getThenRunTestSuite :: [FilePath] -> IO ()
getThenRunTestSuite files = do
  results <- traverse (runTestSuite <=< getTestSuite) files
  case lefts results of
    []     -> pure ()
    errors -> error $ intercalate "\n" errors

runTestSuite :: TestSuite -> IO (Either String ())
runTestSuite TestSuite {testSuiteCases} = do
  failedCases <- (\f -> foldM f [] testSuiteCases) $ \failedCaseAcc testSuiteCase -> do
    runTestCaseResult <- runTestCase testSuiteCase
    pure $ case runTestCaseResult of
      Right () -> failedCaseAcc
      Left err -> err : failedCaseAcc
  case failedCases of
    [] -> do
      putStrLn "All syntax tests passed!"
      pure $ Right ()
    xs -> do
      let errMsg = T.unpack . T.intercalate "\n" $ prettyFailedTestCase <$> xs
      hPutStrLn stderr errMsg
      pure $ Left errMsg

runTestCase :: TestCase -> IO (Either FailedTestCase ())
runTestCase TestCase {testCaseLabel, testCaseStmts, testCaseResult, testCaseFail} = do
  failedTestCaseStmtsList <- (\f -> foldM f [] testCaseStmts) $ \failedStmtsAcc testCaseStmt -> do
    parseAndRefineResultText <- getParseAndRefineResultText testCaseStmt
    pure $
      if testLogic parseAndRefineResultText testCaseResult (fromMaybe False testCaseFail)
        then failedStmtsAcc
        else (testCaseStmt, fromEither parseAndRefineResultText) : failedStmtsAcc
  pure $ case failedTestCaseStmtsList of
    [] -> Right ()
    xs ->
      Left $
        FailedTestCase
          { failedTestCaseLabel = testCaseLabel,
            failedTestCaseStmts = xs,
            failedTestCaseResult = testCaseResult
          }

testLogic :: Eq a => Either a a -> Maybe a -> Bool -> Bool
testLogic (Left _) _ True = True
testLogic (Right _) _ True = False
testLogic _ Nothing False = True
testLogic testResult (Just expected) _ =
  fromEither testResult == expected

getThenGenTestSuite :: [FilePath] -> Bool -> IO ()
getThenGenTestSuite files inplace = do
  results <- traverse (genTestSuite <=< getTestSuite) files
  case inplace of
    True -> traverse_ (uncurry Y.encodeFile) (zip files results)
    False -> for_ (zip files results) $ \(file, result) -> do
      when (length files /= 1) $ do
        putStrLn $ file <> ":"
      BS.putStrLn $ Y.encode result

genTestSuite :: TestSuite -> IO TestSuite
genTestSuite TestSuite {testSuiteCases} = do
  TestSuite <$> traverse genTestCase testSuiteCases

genTestCase :: TestCase -> IO TestCase
genTestCase TestCase {testCaseLabel, testCaseStmts, testCaseResult, testCaseFail} = do
  testCaseResult' <- case testCaseResult of
    Nothing -> pure Nothing
    Just _ -> do
      testCaseResultVec <- traverse getParseAndRefineResultText testCaseStmts
      let maybeTestCaseResult = (V.!) testCaseResultVec 0
      pure . Just $ fromEither (assert ((== maybeTestCaseResult) `all` testCaseResultVec) maybeTestCaseResult)
  pure $
    TestCase
      testCaseLabel
      testCaseStmts
      testCaseResult'
      testCaseFail

getTestSuite :: FilePath -> IO TestSuite
getTestSuite = Y.decodeFileThrow

getParseAndRefineResultText :: T.Text -> IO (Either T.Text T.Text)
getParseAndRefineResultText stmt = do
  parseAndRefineResult <- try @SomeException (parseAndRefine stmt)
  pure $ case parseAndRefineResult of
    Right ok -> Right . T.pack $ show ok
    Left err -> Left . T.pack . head . lines $ show err

newtype TestSuite = TestSuite
  { testSuiteCases :: V.Vector TestCase
  }
  deriving (Generic, Show)

instance A.FromJSON TestSuite where
  parseJSON =
    A.genericParseJSON
      A.defaultOptions
        { A.omitNothingFields = True
        }

instance A.ToJSON TestSuite where
  toJSON =
    A.genericToJSON
      A.defaultOptions
        { A.omitNothingFields = True
        }

data TestCase = TestCase
  { testCaseLabel  :: Maybe T.Text,
    testCaseStmts  :: V.Vector T.Text,
    testCaseResult :: Maybe T.Text,
    testCaseFail   :: Maybe Bool
  }
  deriving (Generic, Show)

instance A.FromJSON TestCase

instance A.ToJSON TestCase

data FailedTestCase = FailedTestCase
  { failedTestCaseLabel  :: Maybe T.Text,
    failedTestCaseStmts  :: [(T.Text, T.Text)],
    failedTestCaseResult :: Maybe T.Text
  }

prettyFailedTestCase :: FailedTestCase -> T.Text
prettyFailedTestCase FailedTestCase {failedTestCaseLabel, failedTestCaseStmts, failedTestCaseResult} =
  let heading =
        "Test failed" <> case failedTestCaseLabel of
          Nothing    -> ""
          Just label -> " for case `" <> label <> "`"
      expecting =
        "Expecting "
          <> ( case failedTestCaseResult of
                 Nothing     -> "test to fail"
                 Just result -> "`" <> result <> "`"
             )
      failedCases = failedTestCaseStmts <&> \(stmt, err) -> "But got `" <> err <> "` for `" <> stmt <> "`"
   in T.intercalate "\n" [heading, expecting, T.intercalate "\n" failedCases]

fromEither :: Either a a -> a
fromEither = \case
  Right x -> x
  Left x  -> x
