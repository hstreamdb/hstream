module HStream.Store.Admin.Command.SQL
  ( startSQLRepl
  ) where

import           Control.Monad                    (forM_)
import           Control.Monad.IO.Class           (liftIO)
import           Data.List                        (isPrefixOf)
import           HStream.Store.Admin.Format       (simpleShowTable)
import qualified System.Console.Haskeline         as H
import           Text.Layout.Table                (asciiS, center, colsAllG,
                                                   column, def, expand,
                                                   fixedLeftCol, justifyText,
                                                   left, tableString, titlesH)
import           Z.Data.CBytes                    (pack, unpack)

import           HStream.Store.Admin.API
import           HStream.Store.Admin.Types
import qualified HStream.Store.Internal.LogDevice as S

runShowTables :: S.LDQuery -> IO ()
runShowTables ldq = do
  tables <- S.showTables ldq
  let titles  = ["Table", "Description"]
  let width   = 40
  let format  = formatRow width <$> tables
  let colSpec = [column expand left def def, fixedLeftCol width]
  putStrLn $
    tableString colSpec asciiS (titlesH titles) (colsAllG center <$> format)
  where
    formatRow width (name, desc) =
      [[unpack name], justifyText width $ unpack desc]

runDescribe :: S.LDQuery -> String -> IO ()
runDescribe ldq name = do
  columns <- S.showTableColumns ldq $ pack name
  let titles  = ["Column", "Type", "Description"]
  let width   = 40
  let format  = formatRow width columns
  let colSpec = [column expand left def def,
                 column expand left def def,
                 fixedLeftCol width]
  putStrLn $
    tableString colSpec asciiS (titlesH titles) (colsAllG center <$> format)
  where
    formatRow width (cols, typs, descs) =
      zipWith3 (\col typ desc ->
                  [[unpack col], [unpack typ], justifyText width $ unpack desc])
      cols typs descs

runSelect :: S.LDQuery -> String -> IO ()
runSelect ldq cmd = do
  results <- S.runQuery ldq $ pack cmd
  forM_ results $ \result -> do
    let titles = unpack <$> S.resultHeaders result
    let rows   = fmap unpack <$> S.resultRows result
    putStrLn $
      simpleShowTable ((, 20, left) <$> titles) rows

startSQLRepl :: HeaderConfig AdminAPI -> StartSQLReplOpts -> IO ()
startSQLRepl conf StartSQLReplOpts{..} = do
  let ldq' = buildLDQueryRes conf startSQLReplTimeout startSQLReplUseSsl
  withResource ldq' $ \ldq -> do
    completionWords <- fmap (unpack . fst) <$> S.showTables ldq
    let complete = H.completeWord Nothing " \t" $ return . completeFunc completionWords
    let setting  = (H.defaultSettings :: H.Settings IO) {H.complete = complete}
    H.runInputT setting $
      fix (\loop -> do
              H.getInputLine "> " >>= \case
                Nothing  -> return ()
                Just str -> runSQLCmd ldq str >> loop)

runSQLCmd :: S.LDQuery -> String -> H.InputT IO ()
runSQLCmd ldq str = liftIO $
  case words str of
    "show" : "tables" : _ -> runShowTables ldq
    "describe" : name : _ -> runDescribe ldq name
    "select" : _          -> runSelect ldq str
    _                     -> putStrLn $ "unknown command: " <> str

completeFunc :: [String] -> String -> [H.Completion]
completeFunc wordList str =
  map H.simpleCompletion $ filter (str `isPrefixOf`) wordList

-------------------------------------------------------------------------------
-- Utils
fix :: (a -> a) -> a
fix f = f (fix f)
