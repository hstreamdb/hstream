module HStream.Server.Core.View where

import qualified Data.HashMap.Strict         as HM
import           Data.IORef                  (atomicModifyIORef')
import           Data.Text

import qualified HStream.Connector.HStore    as HCS
import           HStream.Server.Core.Common  (deleteStoreStream)
import qualified HStream.Server.Persistence  as P
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf (Empty)

deleteView :: ServerContext -> Text -> Bool -> IO Empty
deleteView sc name checkIfExist = do
  atomicModifyIORef' P.groupbyStores (\hm -> (HM.delete name hm, ()))
  deleteStoreStream sc (HCS.transToViewStreamName name) checkIfExist
