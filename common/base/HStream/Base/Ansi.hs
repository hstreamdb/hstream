-- From Z-IO package
module HStream.Base.Ansi
  ( AnsiColor (..)
  , color
  , setBrightForeground
  , setBackground
  , setBrightBackground
  , setDefaultBackground
  , reset
  ) where

import qualified Data.ByteString.Builder as B
import           Data.Word

-------------------------------------------------------------------------------

-- | ANSI's eight standard colors
data AnsiColor
  = Black
  | Red
  | Green
  | Yellow
  | Blue
  | Magenta
  | Cyan
  | White
  deriving (Eq, Ord, Bounded, Enum, Show, Read)

colorToCode :: AnsiColor -> Word8
{-# INLINABLE colorToCode #-}
colorToCode c = case c of
    Black   -> 0
    Red     -> 1
    Green   -> 2
    Yellow  -> 3
    Blue    -> 4
    Magenta -> 5
    Cyan    -> 6
    White   -> 7

color :: AnsiColor -> B.Builder -> B.Builder
{-# INLINABLE color #-}
color c t = setForeground c <> t <> setDefaultForeground

setForeground, setBrightForeground, setBackground, setBrightBackground
  :: AnsiColor -> B.Builder
{-# INLINABLE setForeground #-}
setForeground c       = sgr [30 + colorToCode c]
{-# INLINABLE setBrightForeground #-}
setBrightForeground c = sgr [90 + colorToCode c]
{-# INLINABLE setBackground #-}
setBackground c       = sgr [40 + colorToCode c]
{-# INLINABLE setBrightBackground #-}
setBrightBackground c = sgr [100 + colorToCode c]

setDefaultForeground, setDefaultBackground :: B.Builder
{-# INLINABLE setDefaultForeground #-}
setDefaultForeground = sgr [39]
{-# INLINABLE setDefaultBackground #-}
setDefaultBackground = sgr [49]

sgr :: [Word8]  -- ^ List of sgr code for the control sequence
    -> B.Builder
{-# INLINABLE sgr #-}
sgr args =
    B.char8 '\ESC'
 <> B.char8 '['
 <> intercalateList (B.char8 ';') B.word8Dec args
 <> B.char8 'm'

intercalateList :: B.Builder           -- ^ the seperator
                -> (a -> B.Builder)    -- ^ value formatter
                -> [a]                 -- ^ value list
                -> B.Builder
{-# INLINE intercalateList #-}
intercalateList s f xs = go xs
  where
    go []      = ""
    go [x]     = f x
    go (x:xs') = f x <> s <> go xs'

reset :: B.Builder
{-# INLINABLE reset #-}
reset = sgr [0]
