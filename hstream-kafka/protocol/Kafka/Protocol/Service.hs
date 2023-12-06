{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
-- Indeed, some constraints are needed but ghc thinks not.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Kafka.Protocol.Service
  ( RequestContext (..)
  , ServiceHandler (..)
  , RpcHandler (..)
  , hd

  , RPC (..)
  , getRpcMethod
  , Service (..)
  , HasAllMethods
  , HasMethodImpl (..)
  , HasMethod
  ) where

import           Data.Int
import           Data.Kind               (Constraint, Type)
import           Data.Proxy              (Proxy (..))
import           GHC.TypeLits

import           Kafka.Protocol.Encoding (NullableString, Serializable)

-------------------------------------------------------------------------------

data RequestContext = RequestContext
  { clientId   :: !(Maybe NullableString)
  , clientHost :: !String
  }

type UnaryHandler i o = RequestContext -> i -> IO o

data RpcHandler where
  UnaryHandler
    :: ( Serializable i, Serializable o
       , Show i, Show o
       ) => UnaryHandler i o -> RpcHandler

instance Show RpcHandler where
  show (UnaryHandler _) = "<UnaryHandler>"

data RPC (s :: Type) (m :: Symbol) = RPC

getRpcMethod :: (HasMethod s m) => RPC s m -> (Int16, Int16)
getRpcMethod r = (methodKey r Proxy, methodVersion r Proxy)
  where
    methodKey :: (HasMethod s m) => RPC s m -> Proxy (MethodKey s m) -> Int16
    methodKey _ p = fromIntegral $ natVal p

    methodVersion :: (HasMethod s m) => RPC s m -> Proxy (MethodVersion s m) -> Int16
    methodVersion _ p = fromIntegral $ natVal p

data ServiceHandler = ServiceHandler
  { rpcMethod  :: !(Int16, Int16)
  , rpcHandler :: !RpcHandler
  } deriving (Show)

hd :: ( HasMethod s m
      , Serializable i, Serializable o
      , Show i, Show o
      , MethodInput s m ~ i
      , MethodOutput s m ~ o
      )
   => (RPC s m)
   -> UnaryHandler i o
   -> ServiceHandler
hd rpc handler =
  ServiceHandler{ rpcMethod = getRpcMethod rpc
                , rpcHandler = UnaryHandler handler
                }

-------------------------------------------------------------------------------
-- Fork from: https://github.com/google/proto-lens/blob/master/proto-lens/src/Data/ProtoLens/Service/Types.hs

class HasAllMethods s (ms :: [Symbol])
instance HasAllMethods s '[]
instance (HasAllMethods s ms, HasMethodImpl s m) => HasAllMethods s (m ': ms)

class ( KnownSymbol (ServiceName s)
      , HasAllMethods s (ServiceMethods s)
      ) => Service s where
  type ServiceName s    :: Symbol
  type ServiceMethods s :: [Symbol]

class ( KnownSymbol m
      , KnownSymbol (MethodName s m)
      , KnownNat (MethodKey s m)
      , KnownNat (MethodVersion s m)
      , Service s
      , Serializable (MethodInput  s m)
      , Serializable (MethodOutput s m)
      ) => HasMethodImpl s (m :: Symbol) where
  type MethodName s m    :: Symbol
  type MethodKey s m     :: Nat
  type MethodVersion s m :: Nat
  type MethodInput s m   :: Type
  type MethodOutput s m  :: Type

type HasMethod s m =
  ( RequireHasMethod s m (ListContains m (ServiceMethods s))
  , HasMethodImpl s m
  )

-- TODO: warning if there is method not provided
type family RequireHasMethod s (m :: Symbol) (h :: Bool) :: Constraint where
  RequireHasMethod s m 'False = TypeError
       ( 'Text "No method "
   ':<>: 'ShowType m
   ':<>: 'Text " available for service '"
   ':<>: 'ShowType s
   ':<>: 'Text "'."
   ':$$: 'Text "Available methods are: "
   ':<>: ShowList (ServiceMethods s)
       )
  RequireHasMethod s m 'True = ()

-- | Expands to 'True' when 'n' is in promoted list 'hs', 'False' otherwise.
type family ListContains (n :: k) (hs :: [k]) :: Bool where
  ListContains n '[]       = 'False
  ListContains n (n ': hs) = 'True
  ListContains n (x ': hs) = ListContains n hs

-- | Pretty prints a promoted list.
type family ShowList (ls :: [k]) :: ErrorMessage where
  ShowList '[]  = 'Text ""
  ShowList '[x] = 'ShowType x
  ShowList (x ': xs) =
    'ShowType x ':<>: 'Text ", " ':<>: ShowList xs
