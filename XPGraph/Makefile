INCFLAGS = -I/usr/local/include/ -I./src/api/ -I./apps/ 

CPP = g++
CPPFLAGS = -g -O3 $(INCFLAGS)  -fopenmp -Wall -Wno-strict-aliasing  -Wno-unknown-pragmas
LDFLAGS = -fPIC -L./src/api/lib/
LINKERFLAGS = -lz -lpmem -lpmemobj -lnuma -lxpgraph
DEBUGFLAGS = -g -ggdb $(INCFLAGS)
HEADERS=$(shell find . -name '*.hpp')

NEW_LIB_PATH:=$(addsuffix ./src/api/lib/:, $(LD_LIBRARY_PATH))

all : lib main

lib:
	cd ./src/api/ && $(MAKE)

clean:
	@rm -rf bin/
	@rm -rf src/api/lib

main: lib main.cpp $(HEADERS)
	@mkdir -p bin/
	$(CPP) $(CPPFLAGS) $(LDFLAGS) main.cpp -o bin/main $(LINKERFLAGS)

VSPARSITY_DIR = $(abspath ../vertex_sparsity)
GENERATOR_DIR = $(abspath ../../scatter_graph/generator)

vsparsity: lib $(VSPARSITY_DIR)/xpgraph_vsparsity.cc
	@mkdir -p bin/
	$(CPP) $(CPPFLAGS) -I$(GENERATOR_DIR) -I$(VSPARSITY_DIR) $(LDFLAGS) \
		$(VSPARSITY_DIR)/xpgraph_vsparsity.cc $(VSPARSITY_DIR)/libkrongen.a \
		-o bin/xpgraph_vsparsity $(LINKERFLAGS)

# test ingest
testm:export LD_LIBRARY_PATH=$(NEW_LIB_PATH)
testm: lib clean main
	@echo LD_LIBRARY_PATH=$(LD_LIBRARY_PATH)
	./bin/main

# test recovery
testr:export LD_LIBRARY_PATH=$(NEW_LIB_PATH)
testr: lib main
	@echo LD_LIBRARY_PATH=$(LD_LIBRARY_PATH)
	./bin/main -j 3

testgdb:export LD_LIBRARY_PATH=$(NEW_LIB_PATH)
testgdb: lib clean main
	@echo LD_LIBRARY_PATH=$(LD_LIBRARY_PATH)
	gdb ./bin/main

.PHONY: oneway ssd-lib
ssd-lib:
	$(MAKE) -C src/api ssd
oneway: ssd-lib
	@mkdir -p bin/
	$(CPP) $(CPPFLAGS) -std=c++17 -DXPGRAPH_S apps/xpgraph_s_oneway.cpp -o bin/xpgraph_s_oneway \
		-L./src/api/lib -Wl,-rpath,'$$ORIGIN/../src/api/lib' -lxpgraph_s -lnuma -lpmem -lpthread

ssd-probe: apps/ssd_mmap_probe.cpp
	@mkdir -p bin/
	$(CPP) $(CPPFLAGS) -std=c++17 $< -o bin/ssd_mmap_probe

ssd-allocator-test: tests/ssd_allocator_test.cpp $(HEADERS)
	@mkdir -p bin/
	$(CPP) $(CPPFLAGS) -I./src -std=c++17 -DXPGRAPH_S $< -o bin/ssd_allocator_test -lpmem -lpmemobj -lnuma -lz -lpthread
