#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <getopt.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <sys/sysctl.h>

#include "smoothsort.h"

#define errstr strerror(errno)

void warn(const char *fmt, ...) {
  va_list args;
  va_start(args,fmt);
  vfprintf(stderr,fmt,args);
  va_end(args);
}

void die(const char *fmt, ...) {
  va_list args;
  va_start(args,fmt);
  vfprintf(stderr,fmt,args);
  va_end(args);
  exit(1);
}

void dieif(int test, const char *fmt, ...) {
    if (!test) return;
    va_list args;
    va_start(args,fmt);
    vfprintf(stderr,fmt,args);
    va_end(args);
    exit(1);
}

static const char *const usage =
    "usage: odb [command] [options] [arguments...]";

static const char *const cmdstr =
    "  encode               Encode data to ODB format\n"
    "  decode               Decode data from ODB format\n"
    "  help                 Print this message\n"
;

static const char *const optstr =
    " -e --float-format-e   Use %e to print floats\n"
    " -g --float-format-g   Use %g to print floats\n"
    " -h --help             Print this message\n"
;

static char *float_format = "%20.6f%c";

void parse_opts(int *argcp, char ***argvp) {
    static char* shortopts = "+egh";
    static struct option longopts[] = {
        { "help", no_argument, 0, 'h' },
        { 0, 0, 0, 0 }
    };
    int c;
    while ((c = getopt_long(*argcp, *argvp, shortopts, longopts, 0)) != -1) {
        switch(c) {
            case 'e':
                float_format = "%20.6e%c";
                break;
            case 'g':
                float_format = "%20.6g%c";
                break;
            case 'h':
                printf("%s\n\ncommands:\n%s\noptions:\n%s\n", usage, cmdstr, optstr);
                exit(0);
            case '?':
                die("\noptions:\n%s\n", optstr);
            default:
                die("unhandled option -- %c\n", c);
        }
    }
    *argvp += optind;
    *argcp -= optind;
}

const int INVALID = -1;

typedef enum {
    STRINGS,
    ENCODE,
    DECODE,
    SORT,
    HELP
} cmd_t;

cmd_t parse_cmd(char *str) {
    return !strcmp(str, "strings") ? STRINGS :
           !strcmp(str, "encode")  ? ENCODE  :
           !strcmp(str, "decode")  ? DECODE  :
           !strcmp(str, "sort")    ? SORT    :
           !strcmp(str, "help")    ? HELP    : INVALID ;
}

typedef enum {
    INTEGER,
    FLOAT,
    STRING
} field_type_t;

char *typestr(field_type_t type) {
    switch (type) {
        case INTEGER: return "int";
        case FLOAT:   return "float";
        case STRING:  return "string";
        default:      return "unknown";
    }
}

typedef struct {
    char magic[4];
    unsigned long long bom;
} __attribute__ ((__packed__)) preamble_t;

static const preamble_t preamble = {"odb", 0x0123456789abcdef};

typedef struct {
    field_type_t type;
    char name[256-sizeof(field_type_t)];
} __attribute__ ((__packed__)) field_spec_t;

field_spec_t parse_field_spec(const char *const str) {
    field_spec_t spec;
    bzero(&spec, sizeof(spec));
    char *colon = strchr(str, ':');
    dieif(!colon, "invalid field spec: %s\n", str);
    int n = colon++ - str;
    dieif(n >= sizeof(spec.name), "field name too long: %s\n", str);
    memcpy(spec.name, str, n);
    spec.type = !strcmp(colon, "int")    ? INTEGER :
                !strcmp(colon, "float")  ? FLOAT   :
                !strcmp(colon, "string") ? STRING  :
                                           INVALID ;
    dieif(spec.type == INVALID, "invalid field type: %s\n", colon);
    return spec;
}

char *get_line(FILE *file, char **buffer, size_t *len) {
#if defined(__MACOSX__) || defined(__APPLE__)
  *buffer = fgetln(file,len);
  dieif(ferror(file), "error reading line: %s\n", errstr);
  return *buffer;
#else
  int r = getline(buffer,len,file);
  if (r != -1) return *buffer;
  if (feof(file)) {
    if (buffer) free(*buffer);
    return *buffer = NULL;
  } else if (ferror(file)) {
    die("error reading line: %s\n", errstr);
  }
#endif
  die("unknown get_line badness\n");
}

void fwriten(const void *restrict ptr, size_t size, size_t n, FILE *restrict stream) {
    dieif(fwrite(ptr, size, n, stream) != n, "write error: %s\n", errstr);
}

void fwrite1(const void *restrict ptr, size_t size, FILE *restrict stream) {
    fwriten(ptr, size, 1, stream);
}

void freadn(void *restrict ptr, size_t size, size_t n, FILE *restrict stream) {
    dieif(fread(ptr, size, n, stream) != n, "read error: %s\n", errstr);
}

void fread1(void *restrict ptr, size_t size, FILE *restrict stream) {
    freadn(ptr, size, 1, stream);
}

typedef struct {
    long long field_count;
    field_spec_t *field_specs;
} header_t;

void write_header(FILE *file, long long n, field_spec_t *specs) {
    fwrite1(&preamble, sizeof(preamble_t), file);
    fwrite1(&n, sizeof(n), file);
    fwriten(specs, sizeof(field_spec_t), n, file);
}

header_t read_header(FILE *file) {
    char preamble_b[sizeof(preamble_t)];
    fread1(preamble_b, sizeof(preamble_t), file);
    dieif(memcmp(preamble_b, &preamble, sizeof(preamble_t)), "invalid odb file\n");

    header_t h;
    fread1(&h.field_count, sizeof(h.field_count), file);
    h.field_specs = malloc(h.field_count*sizeof(field_spec_t));
    freadn(h.field_specs, sizeof(field_spec_t), h.field_count, file);
    return h;
}

void free_header(header_t h) { free(h.field_specs); }

int check_header(FILE *file, header_t hh) {
    header_t h = read_header(file);
    int match = hh.field_count == h.field_count &&
        !memcmp(hh.field_specs, h.field_specs, h.field_count*sizeof(field_spec_t));
    free_header(h);
    return match;
}

header_t read_headers(char **files, int n) {
    header_t h;
    for (int i = 0; i < n; i++) {
        FILE *file = fopen(files[i], "r");
        dieif(!file, "error opening %s: %s\n", files[i], errstr);

        if (!i) h = read_header(file);
        else dieif(!check_header(file,h), "field spec mismatch: %s\n", files[i]);

        dieif(fclose(file), "error closing %s: %s\n", files[i], errstr);
    }
    return h;
}

size_t header_size(header_t h) {
    return sizeof(preamble_t) +
           sizeof(h.field_count) +
           sizeof(field_spec_t) * h.field_count;
}

static header_t h;
int sort_n, *sort_order;

#define data(j,k) data[(j)*h.field_count+(k)]

int lt_records(void *d, size_t a, size_t b) {
    long long *data = (long long*) d;
    for (int i = 0; i < sort_n; i++) {
        int j = sort_order[i];
        int s = j < 0 ? -1 : 1;
        j = s*j - 1;
        if (data(a,j) != data(b,j))
            return s*data(a,j) < s*data(b,j) ? 1 : 0;
    }
    return 0;
}

void swap_records(void *d, size_t a, size_t b) {
    long long *data = (long long*) d;
    for (int i = 0; i < h.field_count; i++) {
        if (data(a,i) != data(b,i)) {
            long long t = data(a,i);
            data(a,i) = data(b,i);
            data(b,i) = t;
        }
    }
}

int main(int argc, char **argv) {
    parse_opts(&argc,&argv);
    dieif(argc < 1, "usage: %s\n", usage);

    cmd_t cmd = parse_cmd(argv[0]);
    argv++; argc--;

    switch (cmd) {
        case ENCODE: {
            long long n;
            field_spec_t *specs = malloc(argc*sizeof(field_spec_t));
            for (n = 0; n < argc; n++) {
                if (!strcmp(argv[n], "--")) {
                    specs = realloc(specs, n++);
                    break;
                }
                specs[n] = parse_field_spec(argv[n]);
            }
            argv += n; argc -= n; n--;

            write_header(stdout, n, specs);

            for (int i = 0; i < argc; i++) {
                FILE *file = fopen(argv[i], "r");
                dieif(!file, "error opening %s: %s\n", argv[i], errstr);
                size_t length;
                char *line, *buffer = NULL;
                while (line = get_line(file,&buffer,&length)) {
                    for (int j = 0; j < n; j++) {
                        switch (specs[j].type) {
                            case INTEGER: {
                                errno = 0;
                                long long v = strtoll(line,&line,10);
                                dieif(errno == EINVAL && v == 0, "invalid integer at: %s", line);
                                dieif(errno == ERANGE && v == LLONG_MIN, "integer underflow: %s", line);
                                dieif(errno == ERANGE && v == LLONG_MAX, "integer overflow: %s", line);
                                fwrite1(&v, sizeof(v), stdout);
                                break;
                            }
                            case FLOAT: {
                                char *p;
                                errno = 0;
                                double v = strtod(line,&p);
                                dieif(p == line && v == 0.0, "invalid float at: %s", line);
                                dieif(errno == ERANGE && v == 0.0, "float underflow: %s", line);
                                dieif(errno == ERANGE && abs(v) == HUGE_VAL, "float overflow: %s", line);
                                fwrite1(&v, sizeof(v), stdout);
                                line = p;
                                break;
                            }
                            default:
                                die("encoding type %s not yet implemented\n", typestr(specs[j].type));
                        }
                        if (j < n-1) {
                            dieif(line[0] != '\t', "tab expected, got '%c'", line[0]);
                            line++;
                        } else {
                            dieif(!(line[0] == '\n' || line[0] == '\r'),
                                  "end of line expected, got '%c'", line[0]);
                        }
                    }
                }
                dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
            }

            return 0;
        }
        case DECODE: {
            h = read_headers(argv, argc);
            size_t h_size = header_size(h);

            for (int j = 0; j < h.field_count; j++)
                printf("%20s%c", h.field_specs[j].name, j < h.field_count-1 ? ' ' : '\n');
            for (int j = 0; j < 21*h.field_count; j++)
                printf("-");
            printf("\n");

            for (int i = 0; i < argc; i++) {
                struct stat fs;
                FILE *file = fopen(argv[i], "r");
                dieif(!file, "error opening %s: %s\n", argv[i], errstr);
                dieif(fstat(fileno(file), &fs), "stat error for %s: %s\n", file, errstr);

                dieif(fseek(file, h_size, SEEK_SET),
                      "seek error for %s: %s\n", argv[i], errstr);

                while (ftell(file) < fs.st_size) {
                    for (int j = 0; j < h.field_count; j++) {
                        switch (h.field_specs[j].type) {
                            case INTEGER: {
                                long long v;
                                fread1(&v, sizeof(v), file);
                                printf("%20lld%c", v, j < h.field_count-1 ? ' ' : '\n');
                                break;
                            }
                            case FLOAT: {
                                double v;
                                fread1(&v, sizeof(v), file);
                                printf(float_format, v, j < h.field_count-1 ? ' ' : '\n');
                                break;
                            }
                            default:
                                die("decoding type %s not yet implemented\n",
                                    typestr(h.field_specs[j].type));
                        }
                    }
                }

                dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
            }

            return 0;
        }
        case SORT: {
            int n;
            sort_order = malloc(argc*sizeof(int));
            for (n = 0; n < argc; n++) {
                if (!strcmp(argv[n], "--")) {
                    sort_order = realloc(sort_order, n++);
                    break;
                }
                sort_order[n] = atoi(argv[n]);
            }
            argv += n; argc -= n;
            sort_n = n-1;

            h = read_headers(argv, argc);
            size_t h_size = header_size(h);

            for (int i = 0; i < argc; i++) {
                struct stat fs;
                FILE *file = fopen(argv[i], "r+");
                dieif(!file, "error opening %s: %s\n", argv[i], errstr);
                dieif(fstat(fileno(file), &fs), "stat error for %s: %s\n", file, errstr);

                char *mapped = mmap(
                  NULL,
                  fs.st_size,
                  PROT_READ | PROT_WRITE,
                  MAP_FILE  | MAP_SHARED,
                  fileno(file),
                  0
                );
                dieif(mapped == MAP_FAILED, "mmap failed for %s: %s\n", argv[i], errstr);
                dieif(memcmp(mapped, &preamble, sizeof(preamble_t)), "invalid odb file\n");

                long long *data = (long long*)(mapped+h_size);
                size_t n = (fs.st_size-h_size)/(h.field_count*sizeof(long long));
                su_smoothsort(data, 0, n, lt_records, swap_records);

                dieif(munmap(mapped, fs.st_size), "munmap failed for %s: %s\n", argv[i], errstr);
                dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
            }

            return 0;
        }
        case HELP:
            printf("%s\n\ncommands:\n%s\noptions:\n%s\n", usage, cmdstr, optstr);
            return 0;
        default:
            die("invalid command: %s\n", argv[-1]);
    }

    die("end of main reached\n");
}
