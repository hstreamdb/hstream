CABAL ?= cabal

all:: thrift-hs

THRIFT_COMPILE = thrift-compiler

thrift-hs::
	(cd ../external/hsthrift && make thrift-hs)
	(cd if && $(THRIFT_COMPILE) logdevice/admin/if/admin.thrift --hs -r -o ..)
