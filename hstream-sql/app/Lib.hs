{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib where

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
import           Options.Applicative
import           System.IO

class HasRunTest a where
  runTest :: a -> T.Text -> IO (Either T.Text T.Text)
  getTestName :: a -> String

mkMain :: HasRunTest a => a -> IO ()
mkMain x = do
  Opts {inplace, optCmd, files} <- execParser $ parseArgs x
  case optCmd of
    Run -> getThenRunTestSuite x files
    Gen -> getThenGenTestSuite x files inplace

data Opts = Opts
  { inplace :: !Bool,
    optCmd  :: !Cmd,
    files   :: ![FilePath]
  }

parseArgs :: HasRunTest a => a -> ParserInfo Opts
parseArgs x =
  info
    (helper <*> parseOpts x)
    fullDesc

parseOpts :: HasRunTest a => a -> Parser Opts
parseOpts x =
  Opts
    <$> switch (short 'i' <> help "Inplace edit <file>s, if specified")
    <*> hsubparser
      ( cmdRun x <> cmdGen x
      )
    <*> some (strArgument @FilePath $ metavar "FILES" <> help "File paths to verify or generate tests")

data Cmd = Run | Gen

cmdRun ::
  HasRunTest a =>
  a ->
  Mod
    CommandFields
    Cmd
cmdRun x =
  command
    "run"
    $ info parseCmdRun
    $ progDesc ("Run all " <> getTestName x <> " tests")

parseCmdRun :: Parser Cmd
parseCmdRun = pure Run

cmdGen ::
  HasRunTest a =>
  a ->
  Mod
    CommandFields
    Cmd
cmdGen x =
  command
    "gen"
    $ info parseCmdGen
    $ progDesc ("Generate or update all " <> getTestName x <> " tests results")

parseCmdGen :: Parser Cmd
parseCmdGen = pure Gen

getThenRunTestSuite :: HasRunTest a => a -> [FilePath] -> IO ()
getThenRunTestSuite x files = do
  results <- traverse (runTestSuite x <=< getTestSuite) files
  case lefts results of
    []     -> pure ()
    errors -> error $ intercalate "\n" errors

runTestSuite :: HasRunTest a => a -> TestSuite -> IO (Either String ())
runTestSuite x TestSuite {testSuiteCases} = do
  failedCases <- (\f -> foldM f [] testSuiteCases) $ \failedCaseAcc testSuiteCase -> do
    runTestCaseResult <- runTestCase x testSuiteCase
    pure $ case runTestCaseResult of
      Right () -> failedCaseAcc
      Left err -> err : failedCaseAcc
  case failedCases of
    [] -> do
      putStrLn $ "All " <> getTestName x <> " tests passed!"
      pure $ Right ()
    xs -> do
      let errMsg = T.unpack . T.intercalate "\n" $ prettyFailedTestCase <$> xs
      hPutStrLn stderr errMsg
      pure $ Left errMsg

runTestCase :: HasRunTest a => a -> TestCase -> IO (Either FailedTestCase ())
runTestCase x TestCase {testCaseLabel, testCaseStmts, testCaseResult, testCaseFail} = do
  failedTestCaseStmtsList <- (\f -> foldM f [] testCaseStmts) $ \failedStmtsAcc testCaseStmt -> do
    runTestResultText <- getRunTestResultText x testCaseStmt
    pure $
      if testLogic runTestResultText testCaseResult (fromMaybe False testCaseFail)
        then failedStmtsAcc
        else (testCaseStmt, fromEither runTestResultText) : failedStmtsAcc
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

getThenGenTestSuite :: HasRunTest a => a -> [FilePath] -> Bool -> IO ()
getThenGenTestSuite x files inplace = do
  results <- traverse (genTestSuite x <=< getTestSuite) files
  case inplace of
    True -> traverse_ (uncurry Y.encodeFile) (zip files results)
    False -> for_ (zip files results) $ \(file, result) -> do
      when (length files /= 1) $ do
        putStrLn $ file <> ":"
      BS.putStrLn $ Y.encode result

genTestSuite :: HasRunTest a => a -> TestSuite -> IO TestSuite
genTestSuite x TestSuite {testSuiteCases} = do
  TestSuite <$> traverse (genTestCase x) testSuiteCases

genTestCase :: HasRunTest a => a -> TestCase -> IO TestCase
genTestCase x TestCase {testCaseLabel, testCaseStmts, testCaseResult, testCaseFail} = do
  testCaseResult' <- case testCaseResult of
    Nothing -> pure Nothing
    Just _ -> do
      testCaseResultVec <- traverse (getRunTestResultText x) testCaseStmts
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

getRunTestResultText :: HasRunTest a => a -> T.Text -> IO (Either T.Text T.Text)
getRunTestResultText = runTest

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
