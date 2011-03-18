CFLAGS = -g3

odb: odb.c smoothsort.o
	gcc $(CFLAGS) -std=gnu99 $^ -o $@ -lcmph

%.o: %.c %.h
	gcc $(CFLAGS) -c $< -o $@

export:
	git archive --format tar --prefix odb/ HEAD | tar -C ~/etsy/analytics -xvf -

clean:
	rm -rf odb odb.dSYM *.o

.PHONY: clean export
