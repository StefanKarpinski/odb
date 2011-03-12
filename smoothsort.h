void su_smoothsort(void *base, size_t r, size_t N,
                   int  (*less)(void *m, size_t a, size_t b),
                   void (*swap)(void *m, size_t a, size_t b));
