#
# Makefile for pg_streamrecv
#

# Override this with make PGC=/some/where/pg_config if it's not in the path
PGC=pg_config

CFLAGS=-I$(shell $(PGC) --includedir-server) -I$(shell $(PGC) --includedir) -Wall
LDFLAGS=-L$(shell $(PGC) --libdir) -lpq

all: pg_streamrecv

pg_streamrecv: pg_streamrecv.c

clean:
	rm -f pg_streamrecv.o pg_streamrecv
