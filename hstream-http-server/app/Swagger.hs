module Swagger where

import           Data.Swagger
import           Servant
import           Servant.Swagger
import           Servant.Swagger.UI
import           Servant.Swagger.UI.Core

import           HStream.HTTP.Server.API (apiSwagger)

type API' = SwaggerSchemaUI "swagger-ui" "swagger.json"

api' :: Proxy API'
api' = Proxy

main :: IO ()
main = do
  run 8080 $ serve api' $ schemaUiServer apiSwagger
