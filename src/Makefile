CFLAGS=-g -O2 -fPIC -DPIC $(shell pkg-config --cflags flux-core) $(shell pkg-config --cflags json-c)
LDFLAGS=$(shell pkg-config --libs flux-core) $(shell pkg-config --libs json-c)

all: sched_proxy.so

.PHONY:all clean

sched_proxy.so: sched_proxy.o
	gcc ${LDFLAGS} -shared $^ -o $@

clean:
	rm -f *.so *.o
