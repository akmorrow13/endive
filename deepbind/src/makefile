SHELL := /bin/bash

all: ../deepbindArg

../deepbindArg: deepbindArg.c
	@gcc -o ../deepbindArg -O3 -DNDEBUG -Wno-unused-result -Wno-format-security deepbindArg.c

clean:
	@rm ../deepbindArg
