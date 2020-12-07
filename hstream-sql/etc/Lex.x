-- -*- haskell -*-
-- This Alex file was machine-generated by the BNF converter
{
{-# OPTIONS -fno-warn-incomplete-patterns #-}
{-# OPTIONS_GHC -w #-}
module Language.SQL.Lex where

import qualified Data.Text
import qualified Data.Bits
import Data.Word (Word8)
import Data.Char (ord)
}


$c = [A-Z\192-\221] # [\215]  -- capital isolatin1 letter (215 = \times) FIXME
$s = [a-z\222-\255] # [\247]  -- small   isolatin1 letter (247 = \div  ) FIXME
$l = [$c $s]         -- letter
$d = [0-9]           -- digit
$i = [$l $d _ ']     -- identifier character
$u = [. \n]          -- universal: any character

@rsyms =    -- symbols and non-identifier-like reserved words
   \; | \( | \) | \, | \= | \* | \+ | \- | \/ | \: | \[ | \] | \{ | \} | \. | "COUNT" \( \* \) | \< \> | \< | \> | \< \= | \> \=

:-

-- Line comments
"//" [.]* ;

-- Block comments
\/ \* [$u # \*]* \* ([$u # [\* \/]] [$u # \*]* \* | \*)* \/ ;

$white+ ;
@rsyms
    { tok (\p s -> PT p (eitherResIdent TV s)) }

$l $i*
    { tok (\p s -> PT p (eitherResIdent TV s)) }
\" ([$u # [\" \\ \n]] | (\\ (\" | \\ | \' | n | t | r | f)))* \"
    { tok (\p s -> PT p (TL $ unescapeInitTail s)) }

$d+
    { tok (\p s -> PT p (TI s))    }
$d+ \. $d+ (e (\-)? $d+)?
    { tok (\p s -> PT p (TD s)) }

{

tok :: (Posn -> Data.Text.Text -> Token) -> (Posn -> Data.Text.Text -> Token)
tok f p s = f p s

data Tok =
   TS !Data.Text.Text !Int    -- reserved words and symbols
 | TL !Data.Text.Text         -- string literals
 | TI !Data.Text.Text         -- integer literals
 | TV !Data.Text.Text         -- identifiers
 | TD !Data.Text.Text         -- double precision float literals
 | TC !Data.Text.Text         -- character literals

 deriving (Eq,Show,Ord)

data Token =
   PT  Posn Tok
 | Err Posn
  deriving (Eq,Show,Ord)

printPosn :: Posn -> String
printPosn (Pn _ l c) = "line " ++ show l ++ ", column " ++ show c

tokenPos :: [Token] -> String
tokenPos (t:_) = printPosn (tokenPosn t)
tokenPos [] = "end of file"

tokenPosn :: Token -> Posn
tokenPosn (PT p _) = p
tokenPosn (Err p) = p

tokenLineCol :: Token -> (Int, Int)
tokenLineCol = posLineCol . tokenPosn

posLineCol :: Posn -> (Int, Int)
posLineCol (Pn _ l c) = (l,c)

mkPosToken :: Token -> ((Int, Int), Data.Text.Text)
mkPosToken t@(PT p _) = (posLineCol p, tokenText t)

tokenText :: Token -> Data.Text.Text
tokenText t = case t of
  PT _ (TS s _) -> s
  PT _ (TL s)   -> Data.Text.pack (show s)
  PT _ (TI s)   -> s
  PT _ (TV s)   -> s
  PT _ (TD s)   -> s
  PT _ (TC s)   -> s
  Err _         -> Data.Text.pack "#error"

prToken :: Token -> String
prToken t = Data.Text.unpack (tokenText t)

data BTree = N | B Data.Text.Text Tok BTree BTree deriving (Show)

eitherResIdent :: (Data.Text.Text -> Tok) -> Data.Text.Text -> Tok
eitherResIdent tv s = treeFind resWords
  where
  treeFind N = tv s
  treeFind (B a t left right) | s < a  = treeFind left
                              | s > a  = treeFind right
                              | s == a = t

resWords :: BTree
resWords = b "INSERT" 36 (b "ARRAY" 18 (b ":" 9 (b "," 5 (b "*" 3 (b ")" 2 (b "(" 1 N N) N) (b "+" 4 N N)) (b "." 7 (b "-" 6 N N) (b "/" 8 N N))) (b "=" 14 (b "<=" 12 (b "<" 11 (b ";" 10 N N) N) (b "<>" 13 N N)) (b ">=" 16 (b ">" 15 N N) (b "AND" 17 N N)))) (b "DATE" 27 (b "COUNT" 23 (b "BETWEEN" 21 (b "AVG" 20 (b "AS" 19 N N) N) (b "BY" 22 N N)) (b "CREATE" 25 (b "COUNT(*)" 24 N N) (b "CROSS" 26 N N))) (b "FULL" 32 (b "FORMAT" 30 (b "DAY" 29 (b "DATETIME" 28 N N) N) (b "FROM" 31 N N)) (b "HAVING" 34 (b "GROUP" 33 N N) (b "HOP" 35 N N))))) (b "SESSION" 54 (b "MINUTE" 45 (b "LEFT" 41 (b "INTO" 39 (b "INTERVAL" 38 (b "INT" 37 N N) N) (b "JOIN" 40 N N)) (b "MAX" 43 (b "MAP" 42 N N) (b "MIN" 44 N N))) (b "OR" 50 (b "NUMBER" 48 (b "NOT" 47 (b "MONTH" 46 N N) N) (b "ON" 49 N N)) (b "SECOND" 52 (b "RIGHT" 51 N N) (b "SELECT" 53 N N)))) (b "WEEK" 63 (b "SUM" 59 (b "STREAM" 57 (b "SOURCE" 56 (b "SINK" 55 N N) N) (b "STRING" 58 N N)) (b "TUMBLE" 61 (b "TIME" 60 N N) (b "VALUES" 62 N N))) (b "[" 68 (b "WITHIN" 66 (b "WITH" 65 (b "WHERE" 64 N N) N) (b "YEAR" 67 N N)) (b "{" 70 (b "]" 69 N N) (b "}" 71 N N)))))
   where b s n = let bs = Data.Text.pack s
                 in  B bs (TS bs n)

unescapeInitTail :: Data.Text.Text -> Data.Text.Text
unescapeInitTail = Data.Text.pack . unesc . tail . Data.Text.unpack
  where
  unesc s = case s of
    '\\':c:cs | elem c ['\"', '\\', '\''] -> c : unesc cs
    '\\':'n':cs  -> '\n' : unesc cs
    '\\':'t':cs  -> '\t' : unesc cs
    '\\':'r':cs  -> '\r' : unesc cs
    '\\':'f':cs  -> '\f' : unesc cs
    '"':[]    -> []
    c:cs      -> c : unesc cs
    _         -> []

-------------------------------------------------------------------
-- Alex wrapper code.
-- A modified "posn" wrapper.
-------------------------------------------------------------------

data Posn = Pn !Int !Int !Int
      deriving (Eq, Show,Ord)

alexStartPos :: Posn
alexStartPos = Pn 0 1 1

alexMove :: Posn -> Char -> Posn
alexMove (Pn a l c) '\t' = Pn (a+1)  l     (((c+7) `div` 8)*8+1)
alexMove (Pn a l c) '\n' = Pn (a+1) (l+1)   1
alexMove (Pn a l c) _    = Pn (a+1)  l     (c+1)

type Byte = Word8

type AlexInput = (Posn,     -- current position,
                  Char,     -- previous char
                  [Byte],   -- pending bytes on the current char
                  Data.Text.Text)   -- current input string

tokens :: Data.Text.Text -> [Token]
tokens str = go (alexStartPos, '\n', [], str)
    where
      go :: AlexInput -> [Token]
      go inp@(pos, _, _, str) =
               case alexScan inp 0 of
                AlexEOF                   -> []
                AlexError (pos, _, _, _)  -> [Err pos]
                AlexSkip  inp' len        -> go inp'
                AlexToken inp' len act    -> act pos (Data.Text.take len str) : (go inp')

alexGetByte :: AlexInput -> Maybe (Byte,AlexInput)
alexGetByte (p, c, (b:bs), s) = Just (b, (p, c, bs, s))
alexGetByte (p, _, [], s) =
  case Data.Text.uncons s of
    Nothing  -> Nothing
    Just (c,s) ->
             let p'     = alexMove p c
                 (b:bs) = utf8Encode c
              in p' `seq` Just (b, (p', c, bs, s))

alexInputPrevChar :: AlexInput -> Char
alexInputPrevChar (p, c, bs, s) = c

-- | Encode a Haskell String to a list of Word8 values, in UTF8 format.
utf8Encode :: Char -> [Word8]
utf8Encode = map fromIntegral . go . ord
 where
  go oc
   | oc <= 0x7f       = [oc]

   | oc <= 0x7ff      = [ 0xc0 + (oc `Data.Bits.shiftR` 6)
                        , 0x80 + oc Data.Bits..&. 0x3f
                        ]

   | oc <= 0xffff     = [ 0xe0 + (oc `Data.Bits.shiftR` 12)
                        , 0x80 + ((oc `Data.Bits.shiftR` 6) Data.Bits..&. 0x3f)
                        , 0x80 + oc Data.Bits..&. 0x3f
                        ]
   | otherwise        = [ 0xf0 + (oc `Data.Bits.shiftR` 18)
                        , 0x80 + ((oc `Data.Bits.shiftR` 12) Data.Bits..&. 0x3f)
                        , 0x80 + ((oc `Data.Bits.shiftR` 6) Data.Bits..&. 0x3f)
                        , 0x80 + oc Data.Bits..&. 0x3f
                        ]
}
