odb: odb.c
	gcc -g3 $< -o $@

clean:
	rm -rf odb odb.dSYM

.PHONY: clean
