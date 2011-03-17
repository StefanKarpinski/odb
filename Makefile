CFLAGS = -g3

odb: odb.c smoothsort.o
	gcc $(CFLAGS) -std=gnu99 $^ -o $@ -lcmph

%.o: %.c %.h
	gcc $(CFLAGS) -c $< -o $@

clean:
	rm -rf odb odb.dSYM *.o

.PHONY: clean
