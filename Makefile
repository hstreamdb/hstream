CABAL ?= cabal

all:: thrift sql

THRIFT_COMPILE = thrift-compiler
BNFC = bnfc

thrift::
	(cd external/hsthrift && THRIFT_COMPILE=$(THRIFT_COMPILE) make thrift)
	(cd hstream-store/admin/if && $(THRIFT_COMPILE) logdevice/admin/if/admin.thrift --hs -r -o ..)

sql:: sql-deps
	(cd hstream-sql/etc && $(BNFC) --haskell --functor --text-token -p HStream -m -d SQL.cf -o ../gen-sql)
	(awk -v RS= -v ORS='\n\n' '/\neitherResIdent/{system("cat hstream-sql/etc/replace/eitherResIdent");next } {print}' \
		hstream-sql/gen-sql/HStream/SQL/Lex.x > hstream-sql/gen-sql/HStream/SQL/Lex.x.new)
	(diff hstream-sql/gen-sql/HStream/SQL/Lex.x hstream-sql/gen-sql/HStream/SQL/Lex.x.new || true)
	(cd hstream-sql/gen-sql && mv HStream/SQL/Lex.x.new HStream/SQL/Lex.x && make)

sql-deps::
	(command -v bnfc || cabal install BNFC --constraint 'BNFC >= 2.9')
	(command -v alex || cabal install alex)
	(command -v happy || cabal install happy)
