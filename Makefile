odb: odb.c
	gcc -g3 -std=gnu99 $< -o $@

test-encode:
	./odb encode term:int correction:int p:float -- test.tsv > test.odb

clean:
	rm -rf odb odb.dSYM

.PHONY: clean test-encode
