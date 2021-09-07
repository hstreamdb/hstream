{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE LambdaCase     #-}

module HStream.Client.Internal (
    completer
  ) where

import           Control.Monad
import           Data.Char
import           Data.Function
import           Data.Functor
import           Data.List
import           System.Console.Isocline

completer :: CompletionEnv -> String -> IO ()
completer cEnv inputStr
  | inputStr !! 0 == ':' = pure ()

  | take 1 inputWords == ["SHOW"]
      && (last trimStr) /= ';'
        = completeWord cEnv inputStr Nothing showCompletions

  | take 1 inputWords == ["SELECT"]
      && (last trimStr) /= ';'
        = selectCompleter cEnv inputStr

  | take 1 inputWords == ["EXPLAIN"]
      && (last trimStr) /= ';'
        = selectCompleter cEnv (trim . invWords $ tail inputWords)

  | take 1 inputWords == ["TERMINATE"]
      && (last trimStr) /= ';'
      && (length inputWords < 2
      || (inputWords !! 1) `notElem` (trim <$> filter1of3 terminateItems))
        = completeWord cEnv inputStr Nothing terminateCompletions

  | length inputWords == 3
      && take 2 inputWords == ["TERMINATE", "QUERY"]
        = followCompletion cEnv inputStr ";"

  | take 1 inputWords == ["DROP"]
      && (length inputWords < 2
      || (inputWords !! 1) `notElem` (trim <$> filter1of3 dropItems))
        = completeWord cEnv inputStr Nothing dropCompletions

  | length inputWords == 3
      && take 2 inputWords !! 0 == "DROP"
      && take 2 inputWords !! 1 `elem` (trim <$> filter1of3 dropItems)
        = followCompletion cEnv inputStr ";"

  | length inputWords <= 4
      && take 2 inputWords == ["INSERT", "INTO"]
        = completeWord cEnv inputStr Nothing intoCompletions

  | length inputWords > 3
      && take 2 inputWords == ["INSERT", "INTO"]
      && last trimStr == ')'
        = followCompletion cEnv inputStr "VALUES ();"

  | length inputWords > 4
      && take 2 inputWords == ["INSERT", "INTO"]
      && '(' `elem` inputStr && ')' `elem` inputStr
      && last inputWords == "VALUES"
        = followCompletion cEnv inputStr "();"

  | length inputWords > 4
      && take 2 inputWords == ["INSERT", "INTO"]
      && '(' `notElem` inputStr && ')' `notElem` inputStr
      && last inputWords /= "VALUES"
        = followCompletion cEnv inputStr ";"

  | (length inputWords == 1
      && (head inputWords) `notElem` (trim <$> filter1of3 primStmts))
      || length inputWords == 0
        = completeWord cEnv inputStr Nothing primCompletions

  | otherwise = pure ()

  where
    inputWords = trim <$> words (trim inputStr)
    trimStr    = trim inputStr

highlighter :: String -> Fmt
highlighter = undefined

--------------------------------------------------------------------------------

-- TODO: help docs

primStmts, showItems, terminateItems, dropItems, intoItems :: [(String, String, String)]

primStmts =
  [ ("CREATE "     , "", "")
  , ("DROP "       , "", "")
  , ("SELECT "     , "", "")
  , ("TERMINATE "  , "", "")
  , ("EXPLAIN "    , "", "")
  , ("SHOW "       , "", "")
  , ("INSERT INTO ", "", "")
  ]

showItems =
  [ ("QUERIES;"   , "", "")
  , ("STREAMS;"   , "", "")
  , ("CONNECTORS;", "", "")
  , ("VIEWS;"     , "", "")
  ]

terminateItems =
  [ ("QUERY ", "", "")
  , ("ALL;"  , "", "")
  ]

dropItems =
  [ ("CONNECTOR ", "", "")
  , ("STREAM "   , "", "")
  , ("VIEW "     , "", "")
  ]

intoItems =
  [ ("() "     , "", "")
  , ("VALUES " , "", "")
  ]

primCompletions :: String -> [Completion]
primCompletions inputStr =
  completionsFor (trim inputStr) (filter1of3 primStmts)

showCompletions :: String -> [Completion]
showCompletions inputStr =
  completionsFor (trim inputStr) (filter1of3 showItems)

terminateCompletions :: String -> [Completion]
terminateCompletions inputStr =
  completionsFor (trim inputStr) (filter1of3 terminateItems)

dropCompletions :: String -> [Completion]
dropCompletions inputStr =
  completionsFor (trim inputStr) (filter1of3 dropItems)

intoCompletions :: String -> [Completion]
intoCompletions inputStr =
  completionsFor (trim inputStr) (filter1of3 intoItems)

--------------------------------------------------------------------------------

-- not for view
selectCompleter :: CompletionEnv -> String -> IO ()
selectCompleter cEnv inputStr
  | length inputWords >= 3
      && last inputWords == "GROUP"
        = followCompletion cEnv inputStr " BY "

  | length inputWords >= 3
      && last inputWords == "EMIT"
        = followCompletion cEnv inputStr " CHANGES;"

  | length inputWords >= 2
      && '*' `elem` inputStr
      && ("FROM" /=) `all` inputWords
      && last trimStr /= ';'
      && ("GROUP" /=) `all` inputWords
      && ("EMIT" /=) `all` inputWords
      && ("HAVING" /=) `all` inputWords
      && ("WHERE" /=) `all` inputWords
        = completeWord cEnv inputStr Nothing \s ->
            completionsFor (trim s) ["FROM "]

  | (length inputWords == 1
      && head inputWords /= trim "SELECT")
      || length inputWords == 0
        = completeWord cEnv inputStr Nothing \s ->
            completionsFor (trim s) ["SELECT "]

  | length inputWords >= 3
      && last inputWords `isPrefixOf` "EMIT CHANGES;"
        = completeWord cEnv inputStr Nothing \s ->
            completionsFor (trim s) ["EMIT CHANGES;"]

  | length inputWords >= 3
      && last inputWords `isPrefixOf` "WHERE"
      && last trimStr /= ';'
      && ("GROUP" /=) `all` inputWords
      && ("EMIT" /=) `all` inputWords
      && ("HAVING" /=) `all` inputWords
      && ("WHERE" /=) `all` inputWords
        = completeWord cEnv inputStr Nothing \s ->
            completionsFor (trim s) ["WHERE "]

  | length inputWords >= 3
      && last inputWords `isPrefixOf` "GROUP BY"
      && last trimStr /= ';'
      && ("EMIT" /=) `all` inputWords
      && ("HAVING" /=) `all` inputWords
        = completeWord cEnv inputStr Nothing \s ->
            completionsFor (trim s) ["GROUP BY "]

  | length inputWords >= 3
      && last inputWords `isPrefixOf` "HAVING"
      && last trimStr /= ';'
        = completeWord cEnv inputStr Nothing \s ->
            completionsFor (trim s) ["HAVING "]

  | otherwise = pure ()
  where
    inputWords = trim <$> words (trim inputStr)
    trimStr    = trim inputStr

createCompletions, selectCompletions :: String -> [Completion]
createCompletions = undefined
selectCompletions = undefined

--------------------------------------------------------------------------------

trimSpace :: String -> String
trimSpace = f . f
  where f = reverse . dropWhile isSpace

trim :: String -> String
trim str = trimSpace str <&> toUpper

instance Show Completion where
  show (Completion replacement display help) = "\n" ++
    "replacement: " ++ show replacement ++ "\n" ++
    "display: "     ++ show display     ++ "\n" ++
    "help: "        ++ show help        ++ "\n"

mkCompletion :: (String, String, String) -> Completion
mkCompletion (replacement, display, help) =
  Completion replacement display help

filter1of3 :: [(a, b, c)] -> [a]
filter1of3 []               = []
filter1of3 ((x, y, z) : xs) = x : filter1of3 xs

filter23of3 :: [(a, b, c)] -> [(b, c)]
filter23of3 []               = []
filter23of3 ((x, y, z) : xs) = (y, z) : filter23of3 xs

followCompletion :: CompletionEnv -> String
                 -> String
                 -> IO ()
followCompletion cEnv inputStr str = completeWord cEnv inputStr
  (Just \c -> c `elem` ("\r\n,.;:/\\(){}[]" :: String))
  (const [completion str])

invWords :: [String] -> String
invWords xs = (++ " ") <$> xs & concat
