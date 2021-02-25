#!/usr/local/bin/bash

make distclean
bnfc --haskell --functor --text-token -p HStream -m -d SQL.cf
make
cp HStream/SQL/Lex.x .
cp HStream/SQL/Par.y .
cp HStream/SQL/Lex.hs ../src/HStream/SQL/
cp HStream/SQL/Par.hs ../src/HStream/SQL/
cp HStream/SQL/Print.hs ../src/HStream/SQL/
