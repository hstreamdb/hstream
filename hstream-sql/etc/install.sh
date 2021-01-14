#!/usr/local/bin/bash

make distclean
bnfc-comm --haskell --functor --text-token -p Language -m -d SQL.cf
make
cp Language/SQL/Lex.x .
cp Language/SQL/Par.y .
cp Language/SQL/Lex.hs ../src/Language/SQL/
cp Language/SQL/Par.hs ../src/Language/SQL/
cp Language/SQL/Print.hs ../src/Language/SQL/
