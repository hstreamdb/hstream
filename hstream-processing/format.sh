#!/bin/bash

HS_FILES=$(find src example -name "*.hs")
echo $HS_FILES
ormolu -m inplace $HS_FILES
stylish-haskell -i $HS_FILES

CABAL_FILES=$(find . -name "*.cabal")
echo $CABAL_FILES
cabal-fmt --inplace $CABAL_FILES
