module HStream.Store.Admin.Command.SQL
  ( startSQLRepl
  ) where

import           Control.Monad.IO.Class           (liftIO)
import           Data.Char                        (toLower, toUpper)
import           Data.Function                    (fix)
import           Data.List                        (isPrefixOf)
import qualified System.Console.Haskeline         as H
import           Text.Layout.Table                (asciiS, center, colsAllG,
                                                   column, def, expand,
                                                   fixedLeftCol, justifyText,
                                                   left, tableString, titlesH)
import           Z.Data.CBytes                    (pack, unpack)

import           HStream.Store.Admin.API
import           HStream.Store.Admin.Types
import qualified HStream.Store.Internal.LogDevice as S
import           HStream.Utils                    (simpleShowTable)


runShowTables :: S.LDQuery -> IO String
runShowTables ldq = do
  tables <- S.showTables ldq
  let titles  = ["Table", "Description"]
  let width   = 40
  let format  = formatRow width <$> tables
  let colSpec = [column expand left def def, fixedLeftCol width]
  return $ tableString colSpec asciiS (titlesH titles) (colsAllG center <$> format)
  where
    formatRow width (name, desc) =
      [[unpack name], justifyText width $ unpack desc]

runDescribe :: S.LDQuery -> String -> IO String
runDescribe ldq name = do
  let titles  = ["Column", "Type", "Description"]
  columns <- S.showTableColumns ldq $ pack name
  case columns of
    ([], [], []) -> return $ "Unknown table `" <> name <> "`"
    _ -> do let width   = 40
            let format  = formatRow width columns
            let colSpec = [ column expand left def def
                          , column expand left def def
                          , fixedLeftCol width
                          ]
            return $ tableString colSpec asciiS (titlesH titles) (colsAllG center <$> format)
  where
    formatRow width (cols, typs, descs) =
      zipWith3 (\col typ desc ->
                  [[unpack col], [unpack typ], justifyText width $ unpack desc])
      cols typs descs

runSelect :: S.LDQuery -> String -> IO [String]
runSelect ldq cmd = map formatter <$> S.runQuery ldq (pack cmd)
  where
    formatter result =
      if null (S.resultRows result)
        then "No records were retrieved."
        else let titles = unpack <$> S.resultHeaders result
                 rows   = fmap unpack <$> S.resultRows result
                 maxSize = fromIntegral . max 40 <$> S.resultColsMaxSize result
                 colconf = zipWith (,, left) titles maxSize
              in simpleShowTable colconf rows

startSQLRepl :: HeaderConfig AdminAPI -> StartSQLReplOpts -> IO ()
startSQLRepl conf StartSQLReplOpts{..} = do
  let ldq' = buildLDQueryRes conf startSQLReplTimeout startSQLReplUseSsl
  withResource ldq' $ \ldq -> do
    case startSQLReplSQL of
      Just sql -> runSQLCmd ldq sql
      Nothing  -> startRepl ldq
  where
    startRepl ldq = do
      complete <- getCompletionFun ldq
      let setting  = (H.defaultSettings :: H.Settings IO) {H.complete = complete}
      H.runInputT setting $ fix $ \loop -> do
        H.getInputLine "sql> " >>= \case
          Nothing  -> return ()
          Just str -> liftIO (runSQLCmd ldq str) >> loop

getCompletionFun :: S.LDQuery -> IO (H.CompletionFunc IO)
getCompletionFun ldq = do
  let commands = ["show", "describe", "select"]
  tables <- map (unpack . fst) <$> S.showTables ldq
  return $ H.completeWordWithPrev Nothing " \t" $ \leftStr str -> do
    let leftWord = if null (words leftStr)
                      then ""
                      else reverse . head $ words leftStr
    let wordList = case leftWord of
                     ""         -> commands ++ (map toUpper <$> commands)
                     "show"     -> ["tables"]; "SHOW" -> ["TABLES"]
                     "describe" -> tables; "DESCRIBE" -> tables
                     "from"     -> tables; "FROM"     -> tables
                     _          -> []
    return $ map H.simpleCompletion $ filter (str `isPrefixOf`) wordList

runSQLCmd :: S.LDQuery -> String -> IO ()
runSQLCmd ldq str = handleStoreError $ do
  case words (map toLower str) of
    [] -> return ()
    ["show", "tables"]      -> putStrLn =<< runShowTables ldq
    ["show", "tables", ";"] -> putStrLn =<< runShowTables ldq
    ["show", "tables;"]     -> putStrLn =<< runShowTables ldq
    ["describe", name_]     -> if last name_ == ';' then putStrLn =<< runDescribe ldq (init name_)
                                                    else putStrLn =<< runDescribe ldq name_
    ["describe", name, ";"] -> putStrLn =<< runDescribe ldq name
    "select" : _ -> runSelect ldq str >>= mapM_ putStrLn
    _            -> putStrLn $ "unknown command: " <> str <> "\n\n" <> helpMsg

helpMsg :: String
helpMsg = "Commands: \n"
       <> "- show tables: Shows a list of the supported tables\n"
       <> "- describle <table name>: to get detailed information about that table\n"
       <> "- select: a sql query interface for the tier, use `show tables` to get information about the tables available for query"
