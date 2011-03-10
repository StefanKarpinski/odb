odb: odb.c
	gcc -g3 -std=gnu99 $< -o $@

clean:
	rm -rf odb odb.dSYM

.PHONY: clean
