CABAL ?= cabal

all:: thrift

THRIFT_COMPILE = thrift-compiler

thrift::
	(cd external/hsthrift && THRIFT_COMPILE=$(THRIFT_COMPILE) make thrift)
	(cd hstream-store/admin/if && $(THRIFT_COMPILE) logdevice/admin/if/admin.thrift --hs -r -o ..)
