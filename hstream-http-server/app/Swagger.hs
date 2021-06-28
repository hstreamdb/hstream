{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Data.Aeson.Encode.Pretty (encodePretty)
import qualified Data.ByteString.Lazy     as LBS
import           System.Environment       (getArgs)
import           System.FilePath          (joinPath)

import           HStream.HTTP.Server.API  (apiSwagger)

main :: IO ()
main = do
  args <- getArgs
  let path = if length args == 0
                then "swagger.json"
                else joinPath [head args,  "swagger.json"]
  putStrLn $ "Write swagger json file to " <> path
  LBS.writeFile path (encodePretty apiSwagger)
