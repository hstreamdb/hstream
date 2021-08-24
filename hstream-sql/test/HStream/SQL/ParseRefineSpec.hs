{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.ParseRefineSpec where

import           HStream.SQL.AST
import           HStream.SQL.Parse
import           Test.Hspec

spec :: Spec
spec = describe "Create" $ do

  it "create stream without option, alias or SELECT clause" $ do
    parseAndRefine "CREATE STREAM foo;"
      `shouldReturn` RQCreate (RCreate "foo" (RStreamOptions { rRepFactor = 3 }))

  it "bnfc example 0" $ do
    parseAndRefine "SELECT * FROM temperatureSource EMIT CHANGES;"
      `shouldReturn` RQSelect (RSelect RSelAsterisk (RFromSingle "temperatureSource") RWhereEmpty RGroupByEmpty RHavingEmpty)
    parseAndRefine "CREATE STREAM abnormal_weather AS SELECT * FROM weather WHERE temperature > 30 AND humidity > 80 EMIT CHANGES;"
      `shouldReturn` RQCreate (RCreateAs "abnormal_weather" (RSelect RSelAsterisk (RFromSingle "weather") (RWhere (RCondAnd (RCondOp RCompOpGT (RExprCol "temperature" Nothing "temperature") (RExprConst "30" (ConstantInt 30))) (RCondOp RCompOpGT (RExprCol "humidity" Nothing "humidity") (RExprConst "80" (ConstantInt 80))))) RGroupByEmpty RHavingEmpty) (RStreamOptions {rRepFactor = 3}))

  it "bnfc example 1" $ do
    parseAndRefine
      "CREATE STREAM demoStream ;" `shouldReturn`
        RQCreate (RCreate "demoStream" (RStreamOptions {rRepFactor = 3}))

  -- #TODO: enable it when 'FORMAT' is available
  -- it "bnfc example 2" $ do parseAndRefine
  --   "CREATE STREAM demoSink AS SELECT SUM(source2.humidity) AS result FROM source2 INNER JOIN source1 WITHIN (INTERVAL 5 SECOND) ON (source2.temperature = source1.temperature) WHERE source2.humidity > 20 GROUP BY source2.humidity, TUMBLING (INTERVAL 10 SECOND) EMIT CHANGES WITH (FORMAT = "JSON");"
  --     `shouldReturn`

  it "bnfc example 3" $ do
    parseAndRefine
      "INSERT INTO demoStream (temperature, humidity) VALUES (30, 75);" `shouldReturn`
        RQInsert (RInsert "demoStream" [("temperature",ConstantInt 30),("humidity",ConstantInt 75)])

--------------------------------------------------------------------------------

  it "CREATE VIEW" $ do
    parseAndRefine "CREATE VIEW foo AS SELECT a, SUM(a), COUNT(*) FROM bar GROUP BY b EMIT CHANGES;"
      `shouldReturn` RQCreate (RCreateView "foo" (RSelect (RSelList [(Left (RExprCol "a" Nothing "a"),"a"),(Right (Unary AggSum (RExprCol "a" Nothing "a")),"SUM(a)"),(Right (Nullary AggCountAll),"COUNT(*)")]) (RFromSingle "bar") RWhereEmpty (RGroupBy Nothing "b" Nothing) RHavingEmpty))

  it "CREATE CONNECTOR" $ do
    parseAndRefine "CREATE SINK CONNECTOR mysql_conn WITH (TYPE = mysql, STREAM = foo, host = \"127.0.0.1\");"
      `shouldReturn` RQCreate (RCreateSinkConnector "mysql_conn" False "foo" "mysql" (RConnectorOptions [("host",ConstantString "127.0.0.1")]))

  it "SELECT (Stream)" $ do
    parseAndRefine "SELECT * FROM my_stream EMIT CHANGES;"
      `shouldReturn` RQSelect (RSelect RSelAsterisk (RFromSingle "my_stream") RWhereEmpty RGroupByEmpty RHavingEmpty)
    parseAndRefine "SELECT temperature, humidity FROM weather WHERE temperature > 10 AND humidity < 75 EMIT CHANGES;"
      `shouldReturn` RQSelect (RSelect (RSelList [(Left (RExprCol "temperature" Nothing "temperature"),"temperature"),(Left (RExprCol "humidity" Nothing "humidity"),"humidity")]) (RFromSingle "weather") (RWhere (RCondAnd (RCondOp RCompOpGT (RExprCol "temperature" Nothing "temperature") (RExprConst "10" (ConstantInt 10))) (RCondOp RCompOpLT (RExprCol "humidity" Nothing "humidity") (RExprConst "75" (ConstantInt 75))))) RGroupByEmpty RHavingEmpty)
    parseAndRefine "SELECT stream1.temperature, stream2.humidity FROM stream1 INNER JOIN stream2 WITHIN (INTERVAL 5 SECOND) ON stream1.humidity = stream2.humidity EMIT CHANGES;"
      `shouldReturn`
        RQSelect (RSelect (RSelList [(Left (RExprCol "stream1.temperature" (Just "stream1") "temperature"),"stream1.temperature"),(Left (RExprCol "stream2.humidity" (Just "stream2") "humidity"),"stream2.humidity")]) (RFromJoin ("stream1","humidity") ("stream2","humidity") RJoinInner (fromInteger 5)) RWhereEmpty RGroupByEmpty RHavingEmpty)
    parseAndRefine "SELECT COUNT(*) FROM weather GROUP BY cityId, TUMBLING (INTERVAL 10 SECOND) EMIT CHANGES;"
      `shouldReturn` RQSelect (RSelect (RSelList [(Right (Nullary AggCountAll),"COUNT(*)")]) (RFromSingle "weather") RWhereEmpty (RGroupBy Nothing "cityId" (Just (RTumblingWindow (fromInteger 10)))) RHavingEmpty)

  it "SELECT (View)" $ do
    parseAndRefine "SELECT `SUM(a)`, cnt, a FROM my_view WHERE b = 1;"
      `shouldReturn` RQSelectView (RSelectView {rSelectViewSelect = SVSelectFields [("`SUM(a)`","`SUM(a)`"),("cnt","cnt"),("a","a")], rSelectViewFrom = "my_view", rSelectViewWhere = ("b",RExprConst "1" (ConstantInt 1))})

  it "INSERT" $ do
    parseAndRefine "INSERT INTO weather (cityId, temperature, humidity) VALUES (11254469, 12, 65);"
      `shouldReturn` RQInsert (RInsert "weather" [("cityId",ConstantInt 11254469),("temperature",ConstantInt 12),("humidity",ConstantInt 65)])
    parseAndRefine "INSERT INTO foo VALUES '{\"a\": 1, \"b\": \"abc\"}';"
      `shouldReturn` RQInsert (RInsertJSON "foo" "{\"a\": 1, \"b\": \"abc\"}")
    parseAndRefine "INSERT INTO bar VALUES \"some binary value \x01\x02\x03\";"
      `shouldReturn` RQInsert (RInsertBinary "bar" "some binary value \SOH\STX\ETX")

  it "DROP" $ do
    parseAndRefine "DROP CONNECTOR foo;"           `shouldReturn` RQDrop (RDrop   RDropConnector "foo")
    parseAndRefine "DROP CONNECTOR foo IF EXISTS;" `shouldReturn` RQDrop (RDropIf RDropConnector "foo")
    parseAndRefine "DROP STREAM foo;"              `shouldReturn` RQDrop (RDrop   RDropStream    "foo")
    parseAndRefine "DROP STREAM foo IF EXISTS;"    `shouldReturn` RQDrop (RDropIf RDropStream    "foo")
    parseAndRefine "DROP VIEW foo;"                `shouldReturn` RQDrop (RDrop   RDropView      "foo")
    parseAndRefine "DROP VIEW foo IF EXISTS;"      `shouldReturn` RQDrop (RDropIf RDropView      "foo")
