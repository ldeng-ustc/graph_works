INCFLAGS = -I/usr/local/include/ -I../../src/

CPP = g++
# CPPFLAGS = -g -O0 $(INCFLAGS)  -fopenmp -Wall -Wno-strict-aliasing 
CPPFLAGS = -g -O3 $(INCFLAGS)  -fopenmp -Wall -Wno-strict-aliasing 
LDFLAGS = -shared -fPIC
LINKERFLAGS = -lz -lpmem -lpmemobj -lnuma
DEBUGFLAGS = -g -ggdb $(INCFLAGS)
HEADERS=$(shell find .. -name '*.hpp' -o -name '*.h')

LIB = lib/libxpgraph.so
OBJ = lib/libxpgraph.o

all: $(LIB)

$(LIB) : $(OBJ)
	$(CPP) $(CPPFLAGS) $^ -o $@ $(LINKERFLAGS) $(LDFLAGS)

$(OBJ) : libxpgraph.cpp $(HEADERS)
	@mkdir -p lib/
	$(CPP) $(CPPFLAGS) -std=c++17 -c $< -o $@ $(LINKERFLAGS) $(LDFLAGS)
 
clean:
	@rm -rf lib/

ssd: lib/libxpgraph_s.so
lib/libxpgraph_s.o: libxpgraph.cpp $(HEADERS)
	@mkdir -p lib/
	$(CPP) $(CPPFLAGS) -std=c++17 -DXPGRAPH_S -c $< -o $@ $(LINKERFLAGS) $(LDFLAGS)
lib/libxpgraph_s.so: lib/libxpgraph_s.o
	$(CPP) -std=c++17 $< -o $@ $(LINKERFLAGS) $(LDFLAGS)
