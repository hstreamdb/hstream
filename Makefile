CABAL ?= cabal

all:: thrift grpc sql

THRIFT_COMPILE = thrift-compiler
BNFC = bnfc
PROTO_COMPILE = protoc
PROTO_COMPILE_HS = compile-proto-file
PROTO_CPP_PLUGIN ?= /usr/local/bin/grpc_cpp_plugin

thrift::
	(cd external/hsthrift && THRIFT_COMPILE=$(THRIFT_COMPILE) make thrift)
	(cd hstream-store/admin/if && $(THRIFT_COMPILE) logdevice/admin/if/admin.thrift --hs -r -o ..)

grpc:: grpc-cpp grpc-hs

# grpc-hs requires some generated cpp files
grpc-hs: grpc-cpp
	(cd ~ && command -v $(PROTO_COMPILE_HS) || cabal install proto3-suite --constraint 'proto3-suite == 0.4.1')
	(cd common/proto && $(PROTO_COMPILE_HS) --includeDir /usr/local/include --proto google/protobuf/struct.proto --out ../gen-hs)
	(cd common/proto && $(PROTO_COMPILE_HS) --includeDir /usr/local/include --proto google/protobuf/empty.proto --out ../gen-hs)
	(cd common/proto && $(PROTO_COMPILE_HS) --includeDir /usr/local/include --includeDir . --proto HStream/Server/HStreamApi.proto --out ../gen-hs)
	(cd common/proto && $(PROTO_COMPILE_HS) --includeDir /usr/local/include --includeDir . --proto HStream/Server/HStreamInternal.proto --out ../gen-hs)

grpc-cpp:
	(cd common && mkdir -p gen-cpp && \
		$(PROTO_COMPILE) --cpp_out gen-cpp --grpc_out gen-cpp -I proto --plugin=protoc-gen-grpc=$(PROTO_CPP_PLUGIN) \
		proto/HStream/Server/HStreamApi.proto \
	)

sql:: sql-deps
	(cd hstream-sql/etc && $(BNFC) --haskell --functor --text-token -p HStream -m -d SQL.cf -o ../gen-sql)
	(awk -v RS= -v ORS='\n\n' '/\neitherResIdent/{system("cat hstream-sql/etc/replace/eitherResIdent");next } {print}' \
		hstream-sql/gen-sql/HStream/SQL/Lex.x > hstream-sql/gen-sql/HStream/SQL/Lex.x.new)
	(diff hstream-sql/gen-sql/HStream/SQL/Lex.x hstream-sql/gen-sql/HStream/SQL/Lex.x.new || true)
	(cd hstream-sql/gen-sql && mv HStream/SQL/Lex.x.new HStream/SQL/Lex.x && make)

sql-deps::
	(cd ~ && command -v bnfc || cabal install BNFC --constraint 'BNFC >= 2.9')
	(cd ~ && command -v alex || cabal install alex)
	(cd ~ && command -v happy || cabal install happy)

clean:
	(find . -type d -name "gen-hs2"|xargs rm -rf)
	(find . -type d -name "gen-hs"|xargs rm -rf)
	(find . -type d -name "gen-cpp"|xargs rm -rf)
	(find . -type d -name "gen-sql"|xargs rm -rf)
	(find . -type d -name "gen-src"|xargs rm -rf)
