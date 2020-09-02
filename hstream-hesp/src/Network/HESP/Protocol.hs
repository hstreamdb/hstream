{-# LANGUAGE OverloadedStrings #-}

module Network.HESP.Protocol
  ( serialize
  , deserialize
  , deserializeWith
  , deserializeWithMaybe
  ) where

import           Control.Monad         (replicateM)
import           Data.ByteString       (ByteString)
import qualified Data.ByteString.Char8 as BS
import           Data.Map.Strict       (Map)
import qualified Data.Map.Strict       as Map
import           Data.Vector           (Vector)
import qualified Data.Vector           as V
import qualified Scanner               as P

import           Network.HESP.Types    (Message (..))
import qualified Network.HESP.Types    as T
import           Network.HESP.Utils    (pairs)

-------------------------------------------------------------------------------

serialize :: Message -> ByteString
serialize (MatchSimpleString bs) = serializeSimpleString bs
serialize (MatchBulkString bs)   = serializeBulkString bs
serialize (MatchSimpleError t m) = serializeSimpleError t m
serialize (Boolean b)            = serializeBoolean b
serialize (Integer i)            = serializeInteger i
serialize (MatchArray xs)        = serializeArray xs
serialize (MatchPush x xs)       = serializePush x xs
serialize (MatchMap x)           = serializeMap x

-- | Deserialize the complete input, without resupplying.
deserialize :: ByteString -> Either String Message
deserialize = P.scanOnly parser

-- | Deserialize with the provided resupply action.
deserializeWith
  :: Monad m
  => m ByteString         -- ^ resupply action
  -> ByteString           -- ^ input
  -> m (Vector (Either String Message))
deserializeWith = flip runScanWith parser

deserializeWithMaybe
  :: Monad m
  => m (Maybe ByteString)  -- ^ resupply action
  -> ByteString            -- ^ initial input, can be null
  -> m (Vector (Either String Message))
deserializeWithMaybe = flip runScanWithMaybe parser

-------------------------------------------------------------------------------
-- Serialize

serializeSimpleString :: ByteString -> ByteString
serializeSimpleString bs = BS.cons '+' bs <> sep

serializeBulkString :: ByteString -> ByteString
serializeBulkString bs = BS.cons '$' $ len <> sep <> bs <> sep
  where len = pack $ BS.length bs

serializeSimpleError :: ByteString -> ByteString -> ByteString
serializeSimpleError errtype errmsg = BS.concat ["-", errtype, " ", errmsg, sep]

serializeBoolean :: Bool -> ByteString
serializeBoolean True  = BS.cons '#' $ "t" <> sep
serializeBoolean False = BS.cons '#' $ "f" <> sep

serializeInteger :: Integer -> ByteString
serializeInteger i = BS.cons ':' $ pack i <> sep

serializeArray :: Vector Message -> ByteString
serializeArray ms =
  let len = pack $ V.length ms
   in BS.cons '*' $ len <> sep <> encodeVectorMsgs ms

serializePush :: ByteString -> Vector Message -> ByteString
serializePush t ms =
  let len = pack $ V.length ms + 1
      pushType = serializeBulkString t
   in BS.cons '>' $ len <> sep <> pushType <> encodeVectorMsgs ms

serializeMap :: Map Message Message -> ByteString
serializeMap m =
  let len = pack $ Map.size m  -- N.B. The size must not exceed maxBound::Int
      eles = V.fromList $ concatMap (\(k, v) -> [k, v]) (Map.toList m)
   in BS.cons '%' $ len <> sep <> encodeVectorMsgs eles

-- __Warning__: working on large messages could be very inefficient.
encodeVectorMsgs :: Vector Message -> ByteString
encodeVectorMsgs = BS.concat . V.toList . V.map serialize

sep :: ByteString
sep = "\r\n"

pack :: (Show a) => a -> ByteString
pack = BS.pack . show

-------------------------------------------------------------------------------
-- Deserialize

parser :: P.Scanner Message
parser = do
  c <- P.anyChar8
  case c of
    '+' -> T.mkSimpleStringUnsafe <$> str
    '$' -> T.mkBulkString <$> fixedstr
    '-' -> uncurry T.mkSimpleError <$> err
    '#' -> T.Boolean <$> bool
    ':' -> T.Integer <$> integer
    '*' -> T.mkArray <$> array
    '>' -> uncurry T.mkPush <$> push
    '%' -> T.mkMap <$> dict
    _   -> fail $ BS.unpack $ "Unknown type: " `BS.snoc` c

array :: P.Scanner (Vector Message)
array = do
  len <- decimal
  V.replicateM len parser
{-# INLINE array #-}

push :: P.Scanner (ByteString, Vector Message)
push = do
  len <- decimal
  if len >= 2
     then do ms <- V.replicateM len parser
             case V.head ms of
               MatchBulkString s -> return (s, V.tail ms)
               _                 -> fail "Invalid type"
     else fail $ "Invalid length of push type: " <> show len
{-# INLINE push #-}

dict :: P.Scanner (Map Message Message)
dict = do
  len <- decimal
  kvs <- pairs <$> replicateM (len * 2) parser
  return $ Map.fromList kvs
{-# INLINE dict #-}

-- | Parse a non-negative decimal number in ASCII. For example, @10\r\n@
decimal :: Integral n => P.Scanner n
decimal = P.decimal <* eol
{-# INLINE decimal #-}

-- | Parse a signed integer.
--
-- >>> P.scanOnly integer "1\r\n"
-- Right 1
--
-- >>> P.scanOnly integer "-1.1\r\n"
-- Right (-1)
integer :: P.Scanner Integer
integer = do
  i <- str
  case BS.readInteger i of
    Just (l, _) -> return l
    Nothing     -> fail "Not an integer"
{-# INLINE integer #-}

bool :: P.Scanner Bool
bool = b <* eol
  where
    b = do
      c <- P.anyChar8
      case c of
        't' -> return True
        _   -> return False
{-# INLINE bool #-}

str :: P.Scanner ByteString
str = P.takeWhileChar8 (/= '\r') <* eol
{-# INLINE str #-}

fixedstr :: P.Scanner ByteString
fixedstr = do
  len <- decimal
  P.take len <* eol
{-# INLINE fixedstr #-}

err :: P.Scanner (ByteString, ByteString)
err = do
  errtype <- word
  errmsg <- str
  return (errtype, errmsg)
{-# INLINE err #-}

word :: P.Scanner ByteString
word = P.takeWhileChar8 (/= ' ') <* P.skipSpace
{-# INLINE word #-}

eol :: P.Scanner ()
eol = P.char8 '\r' *> P.char8 '\n'
{-# INLINE eol #-}

-------------------------------------------------------------------------------

runScanWith
  :: Monad m
  => m ByteString
  -> P.Scanner a
  -> ByteString
  -> m (Vector (Either String a))
runScanWith more = runScanWithMaybe (Just <$> more)

runScanWithMaybe
  :: Monad m
  => m (Maybe ByteString)
  -> P.Scanner a
  -> ByteString
  -> m (Vector (Either String a))
runScanWithMaybe more s input = go (Just input) scanner V.empty
  where
    scanner = P.scan s
    -- FIXME: more efficiently
    go Nothing _ sums = return sums
    go (Just bs) next sums =
      case next bs of
        P.More next'    -> more >>= \bs' -> go bs' next' sums
        P.Done rest r   ->
          if BS.null rest
             then return $ V.snoc sums (Right r)
             else go (Just rest) scanner $! V.snoc sums (Right r)
        P.Fail _ errmsg -> return $ V.snoc sums (Left errmsg)
