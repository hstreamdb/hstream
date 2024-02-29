{-# LANGUAGE CPP         #-}
{-# LANGUAGE QuasiQuotes #-}

{-
Since ghc doesn't support raw string literals, we have to use the
`Text.RawString.QQ` module to define raw string literals.

Related proposals:

- https://github.com/ghc-proposals/ghc-proposals/issues/260
- https://github.com/ghc-proposals/ghc-proposals/pull/569

However, when using `Text.RawString.QQ`(template-haskell), it will cause the
following errors:

1. Fail to build with ASAN. The workaroud is to use `#ifndef HSTREAM_ENABLE_ASAN`
   to disable the raw string literals.

2. A more complex issue is that when using `Text.RawString.QQ` with hstream
   packages(e.g. hstream-client-cpp), it will cause the following error:
   (At least with current hstreamdb/haskell image which uses system provided
   jemalloc)

   ```
   <no location info>: error:
       /lib/x86_64-linux-gnu/libjemalloc.so.2: cannot allocate memory in static TLS block
   ```

   A simple reproducible example is to create a library which using
   `Text.RawString.QQ` and has a dependency on hstream-common.

   FIXME: figure out the root cause of this issue.

   There are two workarouds for this issue:

   - Build a custom jemalloc with `--disable-initial-exec-tls` option,
     See https://github.com/jemalloc/jemalloc/issues/1237
   - Put all the raw string literals in a separate library which doesn't
     depend on hstream-*. This is the current solution we use.

-}

module HStream.RawString
  ( banner
  , cliBanner
  , cliSqlHelpInfo
  , cliSqlHelpInfos
  ) where

import qualified Data.Map          as M
#ifndef HSTREAM_ENABLE_ASAN
import           Text.RawString.QQ (r)
#endif

banner, cliBanner, cliSqlHelpInfo :: String
cliSqlHelpInfos :: M.Map String String

#ifndef HSTREAM_ENABLE_ASAN
banner =
  [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]

cliBanner =
  [r|
      __  _________________  _________    __  ___
     / / / / ___/_  __/ __ \/ ____/   |  /  |/  /
    / /_/ /\__ \ / / / /_/ / __/ / /| | / /|_/ /
   / __  /___/ // / / _, _/ /___/ ___ |/ /  / /
  /_/ /_//____//_/ /_/ |_/_____/_/  |_/_/  /_/

  |]

cliSqlHelpInfo =
  [r|
Command
  :h                           To show these help info
  :q                           To exit command line interface
  :help [sql_operation]        To show full usage of sql statement

SQL STATEMENTS:
  To create a simplest stream:
    CREATE STREAM stream_name;

  To create a query select all fields from a stream:
    SELECT * FROM stream_name EMIT CHANGES;

  To insert values to a stream:
    INSERT INTO stream_name (field1, field2) VALUES (1, 2);
  |]

cliSqlHelpInfos = M.fromList
  [ ("CREATE", [r|
  CREATE STREAM <stream_name> [AS <select_query>] [ WITH ( {stream_options} ) ];
  CREATE {SOURCE|SINK} CONNECTOR <connector_name> [IF NOT EXIST] WITH ( {connector_options} ) ;
  CREATE VIEW <stream_name> AS <select_query> ;
  |])
  , ("INSERT", [r|
  INSERT INTO <stream_name> ( {field_name} ) VALUES ( {field_value} );
  INSERT INTO <stream_name> VALUES CAST ('json_value'   AS JSONB);
  INSERT INTO <stream_name> VALUES CAST ('binary_value' AS BYTEA);
  |])
  , ("SELECT", [r|
  SELECT <* | {expression [ AS field_alias ]}>
  FROM stream_name_1
       [ join_type JOIN stream_name_2
         WITHIN (some_interval)
         ON stream_name_1.field_1 = stream_name_2.field_2 ]
  [ WHERE search_condition ]
  [ GROUP BY field_name [, window_type] ]
  EMIT CHANGES;
  |])
  , ("SHOW", [r|
  SHOW <CONNECTORS|STREAMS|QUERIES|VIEWS>;
  |])
  , ("TERMINATE", [r|
  TERMINATE <QUERY <query_id>|ALL>;
  |])
  , ("DROP", [r|
  DROP <STREAM <stream_name>|VIEW <view_name>|QUERY <query_id>> [IF EXISTS];
  |])
  ]

#else
banner = "!!! ASAN Enabled !!!"
cliBanner = "!!! ASAN Enabled !!!"
cliSqlHelpInfo = "!!! ASAN Enabled !!!"
cliSqlHelpInfos = M.empty
#endif
