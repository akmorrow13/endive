ifndef JAVA_HOME
$(error JAVA_HOME must be set.)
endif

SCALA_VERSION = 2.10
PROJECT = endive
PROJECT_VERSION = 0.1
TARGET_JAR = target/scala-$(SCALA_VERSION)/$(PROJECT)-assembly-$(PROJECT_VERSION).jar

CC = g++ -I/home/eecs/akmorrow/Programs/eigen3


# Auto-detect architecture
UNAME := $(shell uname -sm)

Darwin_x86_64_CFLAGS := -O3
Linux_x86_64_CFLAGS := -O3 -fPIC -fopenmp -march=native -shared -std=c++0x -pedantic -Wall -Wshadow -Wpointer-arith -Wcast-qual -Wstrict-prototypes -Wmissing-prototypes -mavx -g3
CFLAGS ?= $($(shell echo "$(UNAME)" | tr \  _)_CFLAGS)

# Set dynamic lib extension for architecture
Darwin_x86_64_EXT := dylib
Linux_x86_64_EXT := so

SOEXT ?= $($(shell echo "$(UNAME)" | tr \  _)_EXT)

#Set java extension for architecture
Darwin_x86_64_JAVA := darwin
Linux_x86_64_JAVA := linux

JAVAEXT ?= $($(shell echo "$(UNAME)" | tr \  _)_JAVA)

SRCDIR := src/main/cpp

ODIR := /tmp
LDIR := lib

_OBJ := NativeRoutines.o
OBJ := $(addprefix $(ODIR)/,$(_OBJ))

all: $(LDIR)/libNativeRoutines.$(SOEXT)

$(TARGET_JAR):
	sbt/sbt assembly

$(SRCDIR)/NativeRoutines.h: $(TARGET_JAR) src/main/scala/net/akmorrow13/endive/utils/NativeRoutines.scala
	CLASSPATH=$< javah -o $@ utils.external.NativeRoutines

%.o: %.cxx 
	$(CC) -c -o $@ $< $(CFLAGS)

$(ODIR)/%.o: $(SRCDIR)/%.cxx $(SRCDIR)/%.h
	$(CC) -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/$(JAVAEXT) -c -o $@ $< $(CFLAGS)

$(LDIR)/libNativeRoutines.$(SOEXT): $(OBJ)
	$(CC) -dynamiclib -o $@ $(OBJ) $(CFLAGS)

.PHONY: clean enceval

clean:
	rm -f $(LDIR)/libNativeRoutines.$(SOEXT)
	rm -rf $(ODIR)/*.o
