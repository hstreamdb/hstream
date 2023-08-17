module HStream.Admin.Store.Command.AdminCommand
  ( runAdminCommand
  ) where

import           Data.Text                 (Text)

import qualified HStream.Admin.Store.API   as AA
import           HStream.Admin.Store.Types

runAdminCommand :: AA.HeaderConfig AA.AdminAPI -> AdminCommandOpts -> IO Text
runAdminCommand conf (AdminCommandRaw cmd) = sendAdminCommand conf cmd
runAdminCommand conf _                     = error "Not Implemented"

sendAdminCommand :: AA.HeaderConfig AA.AdminAPI -> Text -> IO Text
sendAdminCommand conf req = AA.adminCommandResponse_response <$>
  (AA.sendAdminApiRequest conf $
    AA.executeAdminCommand $ AA.AdminCommandRequest req)
