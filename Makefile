all: main 

CC = gcc
CFLAGS = -Wall -Wextra -fopenmp -g
CFLAGS += -DCPU_MHZ_SH=\"./cpu_mhz.sh\"

main: main.c
	$(CC) $(CFLAGS) -mcx16 -o $@ $^

test: test.c
	$(CC) $(CFLAGS) -mcx16 -o $@ $^

clean:
	rm -f main test
