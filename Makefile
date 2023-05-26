CABAL ?= cabal
CABAL_BUILD_PARALLEL ?= $(shell nproc)
GHC_MAJOR_VERSION := $(shell ghc --numeric-version | cut -d'.' -f1)

ifeq ($(GHC_MAJOR_VERSION), 8)
CABAL_PROJECT_FILE ?= cabal.project.ghc810
all:: thrift grpc sql generate-version
else
CABAL_PROJECT_FILE ?= cabal.project
all:: grpc sql generate-version
endif

VERSION_TEMPLATE = "common/version/template/Version.tmpl"
VERSION_OUTPUT = "common/version/gen-hs/HStream/Version.hs"
VERSION ?= "unknown"
COMMIT ?= "unknown"

ENGINE_VERSION ?= v1
BNFC = bnfc
PROTO_COMPILE = protoc
PROTO_COMPILE_HS = ~/.cabal/bin/compile-proto-file_hstream
PROTO_CPP_PLUGIN ?= /usr/local/bin/grpc_cpp_plugin

THRIFT_COMPILE = thrift-compiler
thrift::
	(cd external/hsthrift && THRIFT_COMPILE=$(THRIFT_COMPILE) make thrift)
	(cd hstream-admin/store/if && $(THRIFT_COMPILE) logdevice/admin/if/admin.thrift --hs -r -o ..)

grpc:: grpc-cpp grpc-hs

grpc-hs-deps::
	# 1. Always install proto-lens-protoc to avoid inconsistency
	# 2. Change to a temporary dir to avoid create hstream dists.
	(cd $(shell mktemp -d) && \
		cabal install -j$(CABAL_BUILD_PARALLEL) --overwrite-policy=always proto-lens-protoc)
	($(CABAL) build -j$(CABAL_BUILD_PARALLEL) --project-file $(CABAL_PROJECT_FILE) proto3-suite && \
		mkdir -p ~/.cabal/bin && \
		$(CABAL) exec --project-file $(CABAL_PROJECT_FILE) \
			which compile-proto-file_hstream | tail -1 | xargs -I{} cp {} $(PROTO_COMPILE_HS)\
	)

grpc-hs: grpc-hs-deps
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
	(cd common/api && mkdir -p cpp/gen && \
		$(PROTO_COMPILE) --cpp_out cpp/gen --grpc_out cpp/gen -I protos --plugin=protoc-gen-grpc=$(PROTO_CPP_PLUGIN) \
			protos/HStream/Server/HStreamApi.proto \
	)

sql:: sql-deps
	(cd hstream-sql/etc && if [ $(ENGINE_VERSION) = v2 ]; then cp SQL-v2.cf SQL.cf; else cp SQL-v1.cf SQL.cf; fi)
	(cd hstream-sql/etc && $(BNFC) --haskell --functor --text-token -p HStream -m -d SQL.cf -o ../gen-sql)
	(awk -v RS= -v ORS='\n\n' '/\neitherResIdent/{system("cat hstream-sql/etc/replace/eitherResIdent");next } {print}' \
		hstream-sql/gen-sql/HStream/SQL/Lex.x > hstream-sql/gen-sql/HStream/SQL/Lex.x.new)
	(diff hstream-sql/gen-sql/HStream/SQL/Lex.x hstream-sql/gen-sql/HStream/SQL/Lex.x.new || true)
	(cd hstream-sql/gen-sql && mv HStream/SQL/Lex.x.new HStream/SQL/Lex.x && make)

sql-deps::
	# Change to a temporary dir to avoid create hstream dists.
	(cd $(shell mktemp -d) && command -v bnfc || cabal install BNFC --constraint 'BNFC ^>= 2.9')
	(cd $(shell mktemp -d) && command -v alex || cabal install alex --constraint 'alex ^>= 3.2.7.1')
	(cd $(shell mktemp -d) && command -v happy || cabal install happy)

generate-version:
	@gitEnv=$$(git rev-parse --is-inside-work-tree 2>/dev/null); \
    mkdir -p common/version/gen-hs/HStream; \
	if [ $$gitEnv = "true" ]; then \
		HSTREAM_VERSION=$$(git describe --tag --abbrev=0); \
		HSTREAM_COMMIT=$$(git rev-parse HEAD); \
	else \
		HSTREAM_VERSION=${VERSION}; \
		HSTREAM_COMMIT=${COMMIT}; \
	fi; \
    echo "Generating $(VERSION_OUTPUT) from $(VERSION_TEMPLATE)"; \
	sed -e 's/{{version}}/'"$$HSTREAM_VERSION"'/g' -e 's/{{commit}}/'"$$HSTREAM_COMMIT"'/g' $(VERSION_TEMPLATE) > $(VERSION_OUTPUT)

clean:
	find ./common -maxdepth 10 -type d \
		-name "gen" \
		-o -name "gen-hs2" \
		-o -name "gen-hs" \
		-o -name "gen-cpp" \
		-o -name "gen-go" \
		-o -name "gen-sql" \
		-o -name "gen-src" \
		| xargs rm -rf
	rm -rf $(PROTO_COMPILE_HS)
