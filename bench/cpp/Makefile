# On the project root, run 
#
# $ make -f bench/cpp/Makefile

CXX = g++
LDFLAGS += -lgrpc++ -lgpr -lpthread -lprotobuf -llogdevice -lfolly -lboost_program_options

all: fake_cpp_server

fake_cpp_server:
	$(CXX) -std=c++17 -O3 -Wall \
	    -Icommon/api/gen-cpp \
	    -I/usr/local/include \
	    -L/usr/local/lib \
	    common/api/gen-cpp/HStream/Server/HStreamApi.pb.cc \
	    common/api/gen-cpp/HStream/Server/HStreamApi.grpc.pb.cc \
	    bench/cpp/fake_server.cpp \
	    -o local-data/fake_cpp_server \
	    $(LDFLAGS) 

clean:
	rm -f local-data/fake_cpp_server
