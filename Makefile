CFLAGS = -g3 -std=gnu99

odb: odb.c smoothsort.o
	gcc $(CFLAGS) $^ -o $@

%.o: %.c %.h
	gcc $(CFLAGS) -c $< -o $@

clean:
	rm -rf odb odb.dSYM *.o

.PHONY: clean test-encode
