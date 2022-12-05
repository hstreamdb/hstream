CABAL ?= cabal
GHC_MAJOR_VERSION := $(shell ghc --numeric-version | cut -d'.' -f1)

ifeq ($(GHC_MAJOR_VERSION), 9)
CABAL_PROJECT_FILE ?= cabal.project.ghc9
all:: grpc sql
else
CABAL_PROJECT_FILE ?= cabal.project
all:: thrift grpc sql
endif

THRIFT_COMPILE = thrift-compiler
BNFC = bnfc
PROTO_COMPILE = protoc
PROTO_COMPILE_HS = ~/.cabal/bin/compile-proto-file_hstream
PROTO_CPP_PLUGIN ?= /usr/local/bin/grpc_cpp_plugin

thrift::
	(cd external/hsthrift && THRIFT_COMPILE=$(THRIFT_COMPILE) make thrift)
	(cd hstream-admin/store/if && $(THRIFT_COMPILE) logdevice/admin/if/admin.thrift --hs -r -o ..)

grpc:: grpc-cpp grpc-hs

grpc-hs-deps::
	# Always install proto-lens-protoc to avoid inconsistency
	(cd ~ && cabal install --overwrite-policy=always proto-lens-protoc)
	($(CABAL) build --project-file $(CABAL_PROJECT_FILE) proto3-suite && \
		mkdir -p ~/.cabal/bin && \
		$(CABAL) exec --project-file $(CABAL_PROJECT_FILE) \
			which compile-proto-file_hstream | tail -1 | xargs -I{} cp {} $(PROTO_COMPILE_HS)\
	)

grpc-hs: grpc-hs-deps grpc-cpp
	($(PROTO_COMPILE_HS) \
		--includeDir /usr/local/include \
		--proto google/protobuf/struct.proto \
		--out common/api/gen-hs)
	($(PROTO_COMPILE_HS) \
		--includeDir /usr/local/include \
		--proto google/protobuf/empty.proto \
		--out common/api/gen-hs)
	(cd common/api/protos && $(PROTO_COMPILE_HS) \
		--includeDir /usr/local/include \
		--includeDir . \
		--proto HStream/Server/HStreamApi.proto \
		--out ../gen-hs)
	(cd common/api/protos && $(PROTO_COMPILE_HS) \
		--includeDir /usr/local/include \
		--includeDir . \
		--proto HStream/Server/HStreamInternal.proto \
		--out ../gen-hs)
	(cd common/api/protos && $(PROTO_COMPILE_HS) \
		--includeDir . \
		--proto HStream/Gossip/HStreamGossip.proto \
		--out ../gen-hs)

grpc-cpp:
	(cd common/api && mkdir -p gen-cpp && \
		$(PROTO_COMPILE) --cpp_out gen-cpp --grpc_out gen-cpp -I protos --plugin=protoc-gen-grpc=$(PROTO_CPP_PLUGIN) \
			protos/HStream/Server/HStreamApi.proto \
	)

sql:: sql-deps
	(cd hstream-sql/etc && $(BNFC) --haskell --functor --text-token -p HStream -m -d SQL.cf -o ../gen-sql)
	(awk -v RS= -v ORS='\n\n' '/\neitherResIdent/{system("cat hstream-sql/etc/replace/eitherResIdent");next } {print}' \
		hstream-sql/gen-sql/HStream/SQL/Lex.x > hstream-sql/gen-sql/HStream/SQL/Lex.x.new)
	(diff hstream-sql/gen-sql/HStream/SQL/Lex.x hstream-sql/gen-sql/HStream/SQL/Lex.x.new || true)
	(cd hstream-sql/gen-sql && mv HStream/SQL/Lex.x.new HStream/SQL/Lex.x && make)

sql-deps::
	(cd ~ && command -v bnfc || cabal install BNFC --constraint 'BNFC ^>= 2.9')
	(cd ~ && command -v alex || cabal install alex --constraint 'alex ^>= 3.2.7.1')
	(cd ~ && command -v happy || cabal install happy)

clean:
	find ./common -maxdepth 10 -type d \
		-name "gen-hs2" \
		-o -name "gen-hs" \
		-o -name "gen-cpp" \
		-o -name "gen-go" \
		-o -name "gen-sql" \
		-o -name "gen-src" \
		| xargs rm -rf
	rm -rf $(PROTO_COMPILE_HS)
