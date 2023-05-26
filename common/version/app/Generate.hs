{-# LANGUAGE OverloadedStrings #-}

import           Data.Aeson         (object, (.=))
import           Data.Text          (unpack)
import qualified Data.Text.IO       as T
import           Data.Text.Lazy     (toStrict)
import           System.Environment (getArgs)
import           Text.Mustache      (compileMustacheFile, renderMustache)

filePath = "common/version/template/Version.tmpl"
outPath = "common/version/gen-hs/HStream/Version.hs"

main :: IO ()
main = do
  [version, commit] <- getArgs
  template <- compileMustacheFile filePath
  let context = object [ "version" .= version
                       , "commit" .= commit
                       ]
      output = renderMustache template context
  T.writeFile outPath $ toStrict output

