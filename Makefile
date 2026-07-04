CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -std=c11 -O2 -g -pthread \
          -fstack-protector-strong -fPIE -D_FORTIFY_SOURCE=3 -fstack-clash-protection
LDFLAGS = -pthread -pie -Wl,-z,relro,-z,now -Wl,-z,noexecstack

SRCS    = object_graph.c test_graph.c
OBJS    = $(SRCS:.c=.o)
TARGET  = test_graph

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^

%.o: %.c object_graph.h
	$(CC) $(CFLAGS) -c $< -o $@

test: $(TARGET)
	./$(TARGET)

valgrind: $(TARGET)
	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET) /tmp/pog_test.bin /tmp/pog_checklist.bin

.PHONY: all test valgrind clean
