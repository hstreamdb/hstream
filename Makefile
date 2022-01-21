CABAL ?= cabal

all:: thrift grpc sql

THRIFT_COMPILE = thrift-compiler
BNFC = bnfc
PROTO_COMPILE = protoc
PROTO_COMPILE_HS = compile-proto-file
PROTO_CPP_PLUGIN ?= /usr/local/bin/grpc_cpp_plugin

thrift::
	(cd external/hsthrift && THRIFT_COMPILE=$(THRIFT_COMPILE) make thrift)
	(cd hstream-admin/if && $(THRIFT_COMPILE) logdevice/admin/if/admin.thrift --hs -r -o ..)

grpc:: grpc-cpp grpc-hs grpc-gateway

grpc-hs: grpc-cpp
	(cabal build proto3-suite && mkdir -p ~/.local/bin && find dist-newstyle/ -type f -name "compile-proto-file" | xargs -I{} cp {} ~/.local/bin/)
	(cd common/proto && $(PROTO_COMPILE_HS) --includeDir /usr/local/include --proto google/protobuf/struct.proto --out ../gen-hs)
	(cd common/proto && $(PROTO_COMPILE_HS) --includeDir /usr/local/include --proto google/protobuf/empty.proto --out ../gen-hs)
	(cd common/proto && $(PROTO_COMPILE_HS) --includeDir /usr/local/include --includeDir . --proto HStream/Server/HStreamApi.proto --out ../gen-hs)
	(cd common/proto && $(PROTO_COMPILE_HS) --includeDir /usr/local/include --includeDir . --proto HStream/Server/HStreamInternal.proto --out ../gen-hs)

grpc-cpp:
	(cd common && mkdir -p gen-cpp && \
		$(PROTO_COMPILE) --cpp_out gen-cpp --grpc_out gen-cpp -I proto --plugin=protoc-gen-grpc=$(PROTO_CPP_PLUGIN) \
		proto/HStream/Server/HStreamApi.proto \
	)

grpc-go:
	(cd common && mkdir -p gen-go && \
		$(PROTO_COMPILE) -I proto -I /usr/local/include \
			--go_out ./gen-go --go_opt paths=source_relative \
			--go-grpc_out ./gen-go --go-grpc_opt paths=source_relative \
			proto/HStream/Server/HStreamApi.proto)

grpc-gateway: grpc-go
	(cd common && mkdir -p gen-go && \
		$(PROTO_COMPILE) -I proto -I /usr/local/include	\
			--grpc-gateway_out ./gen-go --grpc-gateway_opt paths=source_relative \
			--grpc-gateway_opt grpc_api_configuration=./proto/HStream/Server/HStreamApi.yaml \
			proto/HStream/Server/HStreamApi.proto)

sql:: sql-deps
	(cd hstream-sql/etc && $(BNFC) --haskell --functor --text-token -p HStream -m -d SQL.cf -o ../gen-sql)
	(awk -v RS= -v ORS='\n\n' '/\neitherResIdent/{system("cat hstream-sql/etc/replace/eitherResIdent");next } {print}' \
		hstream-sql/gen-sql/HStream/SQL/Lex.x > hstream-sql/gen-sql/HStream/SQL/Lex.x.new)
	(diff hstream-sql/gen-sql/HStream/SQL/Lex.x hstream-sql/gen-sql/HStream/SQL/Lex.x.new || true)
	(cd hstream-sql/gen-sql && mv HStream/SQL/Lex.x.new HStream/SQL/Lex.x && make)

sql-deps::
	(cd ~ && command -v bnfc || cabal install BNFC --constraint 'BNFC >= 2.9')
	# alex == 3.2.7 will fail to build language-c, see: https://github.com/simonmar/alex/issues/197
	(cd ~ && command -v alex || cabal install alex --constraint 'alex == 3.2.6')
	(cd ~ && command -v happy || cabal install happy)

clean:
	find . -maxdepth 2 -type d \
		-name "gen-hs2" \
		-o -name "gen-hs" \
		-o -name "gen-cpp" \
		-o -name "gen-sql" \
		-o -name "gen-src" \
		| xargs rm -rf
	rm -rf ~/.local/bin/$(PROTO_COMPILE_HS)
