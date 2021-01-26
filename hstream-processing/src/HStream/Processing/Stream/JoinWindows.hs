{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Stream.JoinWindows
  ( JoinWindows (..),
  )
where

import           RIO

data JoinWindows
  = JoinWindows
      { jwBeforeMs :: Int64,
        jwAfterMs :: Int64,
        jwGraceMs :: Int64
      }
