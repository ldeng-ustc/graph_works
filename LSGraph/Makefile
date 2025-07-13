TARGETS= LSGraph LSGraph-Bench LSGraph-TC LSGraph-Ingestion

ifdef D
	DEBUG=-g -DDEBUG_MODE
	OPT=
else
	DEBUG=
	OPT=-Ofast -DNDEBUG
endif

ifdef NH
	ARCH=
else
	#ARCH=-msse4.2 -D__SSE4_2_
	ARCH=-march=native
endif

ifdef P
	PROFILE=-pg -no-pie # for bug in gprof.
endif

# sudo apt install libstdc++-8-dev
# CXX = /home/ldeng/open-source/OpenCilk_v1.0/OpenCilk-10.0.1-Linux/bin/clang++ -std=c++17
# CC = /home/ldeng/open-source/OpenCilk_v1.0/OpenCilk-10.0.1-Linux/bin/clang -std=gnu11
# LD= /home/ldeng/open-source/OpenCilk_v1.0/OpenCilk-10.0.1-Linux/bin/clang++ -std=c++17


# Only check for OpenCilk if not cleaning
ifneq ($(filter clean,$(MAKECMDGOALS)),clean)

ifndef CILK_HOME
    OPENCILK_DIR := $(CURDIR)/opencilk/OpenCilk-10.0.1-Linux
    OPENCILK_CLANGXX := $(OPENCILK_DIR)/bin/clang++
    OPENCILK_CLANG := $(OPENCILK_DIR)/bin/clang

    # Check if local OpenCilk is already available
    ifneq ("$(wildcard $(OPENCILK_CLANGXX))","")
        $(info CILK_HOME not set. Using local OpenCilk at $(OPENCILK_DIR))
        CILK_HOME := $(OPENCILK_DIR)
    else
        $(info CILK_HOME not set. Local OpenCilk not found. Will attempt to download.)
        # This block will execute if OpenCilk is not found.
        # It uses a shell command to perform the setup.
        # The result of the shell command is assigned to CILK_HOME.
        CILK_HOME := $(shell \
            mkdir -p opencilk && \
            TAR_FILE="opencilk/opencilk.tar.gz" && \
            EXPECTED_MD5="959ca8c530c3f739f0f2806f066b8456" && \
            if [ -f "$$TAR_FILE" ] && echo "$$EXPECTED_MD5  $$TAR_FILE" | md5sum -c --status >/dev/null 2>&1; then \
                echo "opencilk.tar.gz already exists and MD5 is correct." >&2; \
            else \
                echo "Downloading OpenCilk..." >&2 && \
                wget -O $$TAR_FILE https://github.com/OpenCilk/opencilk-project/releases/download/opencilk%2Fv1.0/OpenCilk-1.0-LLVM-10.0.1-Ubuntu-20.04-x86_64.tar.gz; \
            fi && \
            echo "Extracting OpenCilk..." >&2 && \
            tar -xzf opencilk/opencilk.tar.gz -C opencilk && \
            if [ -f "$(OPENCILK_CLANGXX)" ] && [ -f "$(OPENCILK_CLANG)" ]; then \
                echo "OpenCilk setup successful. Using $(OPENCILK_DIR)" >&2; \
                echo "$(OPENCILK_DIR)"; \
            else \
                echo "Error: OpenCilk setup failed. clang++ or clang not found after extraction." >&2; \
                exit 1; \
            fi \
        )
        # Check if CILK_HOME was set successfully
        ifneq ($(CILK_HOME),$(OPENCILK_DIR))
            $(error Failed to download and set up OpenCilk. \
			        Please set CILK_HOME manually, \
					or remove ./opencilk and retry.)
        endif
    endif
endif

CXX = $(CILK_HOME)/bin/clang++ -std=c++17
CC = $(CILK_HOME)/bin/clang -std=gnu11
LD = $(CILK_HOME)/bin/clang++ -std=c++17

endif # ifneq ($(filter clean,$(MAKECMDGOALS)),clean)

# 严重的，涉及多个文件的头文件名不一致，无法使用
# CXX = /home/ldeng/open-source/OpenCilk/build/bin/clang++ -std=c++17
# CC = /home/ldeng/open-source/OpenCilk/build/bin/clang -std=gnu11
# LD= /home/ldeng/open-source/OpenCilk/build/bin/clang++ -std=c++17


LOC_INCLUDE=include
LOC_LIB=lib
LOC_SRC=src
OBJDIR=obj
SER=ser
CILK=1

CXXFLAGS += -mcx16 $(DEBUG) -g $(PROFILE) $(OPT) $(ARCH) -DOPENMP=$(OPENMP) -DCILK=$(CILK) -DPARLAY_OPENCILK -m64 -I. -I$(LOC_INCLUDE)
# CXXFLAGS += -Wall
CXXFLAGS += -fopenmp=libomp
CXXFLAGS += -Wno-format -Wno-return-type -Wno-source-uses-cilk-plus -Wno-macro-redefined -Wno-c++20-extensions -Wno-deprecated-declarations
CXXFLAGS += -Wno-constant-conversion

CFLAGS += -mcx16  $(DEBUG) $(PROFILE) $(OPT) $(ARCH) -DOPENMP=$(OPENMP) -DCILK=$(CILK) -DPARLAY_OPENCILK -m64 -I. -I$(LOC_INCLUDE)
CFLAGS += -fopenmp=libomp

# LDFLAGS += $(DEBUG) $(PROFILE) $(OPT) -L$(LOC_LIB) -lm -lpthread -lssl -lcrypto -ldl -lomp5
LDFLAGS += $(DEBUG) $(PROFILE) $(OPT) -L$(LOC_LIB) -lm -lpthread -ldl
# LDFLAGS += -L /usr/lib/llvm-10/lib/libomp.so

OPENMP?=0
ifeq ($(OPENMP),1)
  CILK=0
else
  CILK?=0
endif

ifeq ($(CILK),1)
  CFLAGS += -fcilkplus
  CXXFLAGS += -fcilkplus
  LDFLAGS += -lcilkrts
endif

ifeq ($(OPENMP),1)
  CFLAGS += -fopenmp
  CXXFLAGS += -fopenmp
  LDFLAGS += -lomp
endif

LDFLAGS +="-Wl,-rpath,lib/"
all: LSGraph LSGraph-Bench LSGraph-TC LSGraph-Ingestion
LSGraph:							$(OBJDIR)/LSGraph.o \
												$(OBJDIR)/util.o
# dependencies between .o files and .cc (or .c) files
$(OBJDIR)/LSGraph.o: 					$(LOC_SRC)/LSGraph.cc \
																	$(LOC_INCLUDE)/graph.h \
																	$(LOC_INCLUDE)/util.h


LSGraph-Bench: $(OBJDIR)/LSGraph-Bench.o $(OBJDIR)/util.o

LSGraph-TC: $(OBJDIR)/LSGraph-TC.o $(OBJDIR)/util.o

LSGraph-Ingestion: $(OBJDIR)/LSGraph-Ingestion.o $(OBJDIR)/util.o

$(OBJDIR)/LSGraph-Bench.o: $(LOC_SRC)/LSGraph-Bench.cc $(LOC_INCLUDE)/graph.h $(LOC_INCLUDE)/util.h

$(OBJDIR)/LSGraph-TC.o: $(LOC_SRC)/LSGraph-TC.cc $(LOC_INCLUDE)/graph.h $(LOC_INCLUDE)/util.h

$(OBJDIR)/LSGraph-Ingestion.o: $(LOC_SRC)/LSGraph-Ingestion.cc $(LOC_INCLUDE)/graph.h $(LOC_INCLUDE)/util.h


#
# generic build rules
#

$(TARGETS):
	$(LD) $^ $(LDFLAGS) -o $@ 

$(OBJDIR)/%.o: $(LOC_SRC)/%.cc | $(OBJDIR)
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

$(OBJDIR)/%.o: $(LOC_SRC)/%.c | $(OBJDIR)
	$(CXX) $(CFLAGS) $(INCLUDE) -c -o $@ $<

$(OBJDIR)/%.o: $(LOC_SRC)/gqf/%.c | $(OBJDIR)
	$(CXX) $(CFLAGS) $(INCLUDE) -c -o $@ $<

$(OBJDIR):
	@mkdir -p $(OBJDIR)

$(SER):
	@mkdir -p $(SER)

clean:
	rm -rf $(OBJDIR) core $(TARGETS)
