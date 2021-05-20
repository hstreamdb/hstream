CABAL ?= cabal

all:: thrift

THRIFT_COMPILE = thrift-compiler

thrift::
	(cd ../../external/hsthrift && make thrift)
	(cd if && $(THRIFT_COMPILE) logdevice/admin/if/admin.thrift --hs -r -o ..)
