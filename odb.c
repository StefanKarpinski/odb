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

// external dependencies:
#include <cmph.h>

// internal headers:
#include "smoothsort.h"

#define errstr                  strerror(errno)

#define warn(fmt,args...)     { fflush(stdout); fsync(fileno(stdout)); \
                                fprintf(stderr,fmt,##args);            \
                                fflush(stderr); fsync(fileno(stderr)); }

#define die(fmt,args...)      { fflush(stdout); fsync(fileno(stdout)); \
                                fprintf(stderr,fmt,##args);            \
                                fflush(stderr); fsync(fileno(stderr)); \
                                exit(1); }

#define dieif(cond,fmt,args...) if (cond) die(fmt,##args);

static const char *const usage =
    "usage: odb [command] [options] [arguments...]";

static const char *const cmdstr =
    "  strings    Generate strings index\n"
    "  encode     Encode data to ODB format\n"
    "  cat        Concatenate files with like schemas\n"
    "  cut        Cut selected columns\n"
    "  paste      Paste columns from different files\n"
    "  join       Join files on specified fields\n"
    "  print      Print data in tabular format\n"
    "  sort       Sort by specified fields (in place)\n"
    "  rename     Rename fields (in place)\n"
    "  cast       Cast fields as different types (in place)\n"
    "  help       Print this message\n"
;

static const char *const optstr =
    " -f --fields=<fields>   Comma-sparated fields\n"
    " -s --strings=<file>    Use <file> as string index\n"
    " -e --format-e          Use %e to print floats\n"
    " -g --format-g          Use %g to print floats\n"
    " -h --help              Print this message\n"
;

static char *fields_arg = NULL;
static char *strings_file = "strings.idx";
static char *float_format = " %20.6f";

void parse_opts(int *argcp, char ***argvp) {
    static char* shortopts = "s:f:egh";
    static struct option longopts[] = {
        { "fields",   required_argument, 0, 'f' },
        { "strings",  required_argument, 0, 's' },
        { "format-e", no_argument,       0, 'e' },
        { "format-g", no_argument,       0, 'g' },
        { "help",     no_argument,       0, 'h' },
        { 0, 0, 0, 0 }
    };
    int c;
    while ((c = getopt_long(*argcp, *argvp, shortopts, longopts, 0)) != -1) {
        switch(c) {
            case 'f':
                fields_arg = optarg;
                break;
            case 's':
                strings_file = optarg;
                break;
            case 'e':
                float_format = " %20.6e";
                break;
            case 'g':
                float_format = " %20.6g";
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

typedef enum {
    STRINGS,
    ENCODE,
    CAT,
    CUT,
    PASTE,
    JOIN,
    PRINT,
    SORT,
    RENAME,
    CAST,
    HELP,
    INVALID
} cmd_t;

cmd_t parse_cmd(char *str) {
    return !strcmp(str, "strings") ? STRINGS :
           !strcmp(str, "encode")  ? ENCODE  :
           !strcmp(str, "cat")     ? CAT     :
           !strcmp(str, "cut")     ? CUT     :
           !strcmp(str, "paste")   ? PASTE   :
           !strcmp(str, "join")    ? JOIN    :
           !strcmp(str, "print")   ? PRINT   :
           !strcmp(str, "sort")    ? SORT    :
           !strcmp(str, "rename")  ? RENAME  :
           !strcmp(str, "cast")    ? CAST    :
           !strcmp(str, "help")    ? HELP    : INVALID;
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
                !strcmp(colon, "string") ? STRING  : -1;
    dieif(spec.type < 0, "invalid field type: %s\n", colon);
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

FILE *fopenr_arg(int argc, char **argv, int i) {
    if (argc == 0 && i == 0)    return stdin;
    if (argc <= i)              return NULL;
    if (!strcmp(argv[i], "-"))  return stdin;

    FILE *file = fopen(argv[i], "r");
    dieif(!file, "error opening %s: %s\n", argv[i], errstr);
    return file;
}

char *argstr(char *arg) { arg ? arg : "-"; }

header_t read_headers(int argc, char **argv) {
    header_t h;
    FILE *file;
    int stdin_seen = 0;
    for (int i = 0; file = fopenr_arg(argc, argv, i); i++) {
        if (!fileno(file)) {
            if (stdin_seen) continue;
            stdin_seen = 1;
        }
        if (i == 0) h = read_header(file);
        else dieif(!check_header(file,h), "field spec mismatch: %s\n", argstr(argv[i]));
        if (fileno(file)) dieif(fclose(file), "error closing %s: %s\n", argstr(argv[i]), errstr);
    }
    return h;
}

size_t header_size(header_t h) {
    return sizeof(preamble_t) +
           sizeof(h.field_count) +
           sizeof(field_spec_t) * h.field_count;
}

size_t h_size;
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

char *ltrunc(char *line) {
    char *nl = strchr(line, '\n');
    if (nl) *nl = '\0';
    return line;
}

typedef struct {
    off_t *offsets;
    off_t index;
    char *data;
} cmph_state_t;

static int key_read(void *state, char **key, cmph_uint32 *len) {
    cmph_state_t *s = (cmph_state_t*) state;
    *key = s->data + s->offsets[s->index++];
    return *len = strlen(*key);
}
static void key_rewind(void *state) {
    cmph_state_t *s = (cmph_state_t*) state;
    s->index = 0;
}
static void key_dispose(void *state, char *key, cmph_uint32 len) { }

void ff_align(FILE *file, size_t unit) {
    dieif(fseeko(file, unit*(ftello(file)/unit+1), SEEK_SET), "seek error: %s\n", errstr);
}

off_t string_count;
char *string_data;
off_t *string_offsets;
off_t *string_reverse;
cmph_t *string_hash;
off_t string_maxlen;

void load_strings() {
    struct stat fs;
    FILE *strings = fopen(strings_file, "r");
    dieif(!strings, "error opening %s: %s", strings_file, errstr);
    dieif(fstat(fileno(strings), &fs), "stat error for %s: %s\n", strings_file, errstr);
    char *data = mmap(
        NULL,
        fs.st_size,
        PROT_READ,
        MAP_PRIVATE,
        fileno(strings),
        0
    );
    dieif(data == MAP_FAILED, "mmap failed for %s: %s\n", strings_file, errstr);
    off_t *offsets = (off_t*) data;
    off_t i = fs.st_size/sizeof(off_t);
    string_count = offsets[--i];
    string_data = data + offsets[--i];
    string_offsets = (off_t*)(data + offsets[--i]);
    string_reverse = (off_t*)(data + offsets[--i]);
    dieif(fseeko(strings, offsets[--i], SEEK_SET), "seek error in %s: %s", strings_file, errstr);
    string_hash = cmph_load(strings);
    dieif(fseeko(strings, 0, SEEK_SET), "seek error in %s: %s", strings_file, errstr);
    string_maxlen = offsets[--i];
    dieif(!string_hash, "error loading string hash\n");
    return;
}

long long string_to_index(char *str, off_t len) {
    cmph_uint32 h = cmph_search(string_hash, str, len);
    if (!(0 <= h && h < string_count)) goto unexpected;
    off_t index = string_reverse[h];
    off_t offset = string_offsets[index];
    if (strncmp(str, string_data + offset, len)) goto unexpected;
    return index;
unexpected:
    str[len] = '\0';
    die("unexpected string: %s\n", str);
}

char *index_to_string(long long index) {
    dieif(!(0 <= index && index <= string_count),
          "invalid string index: %lld\n", index);
    char *str = string_data + string_offsets[index];
    dieif(index && str[-1], "string index mismatch\n");
    return str;
}

typedef void (*fork_callback_t)(void);

pid_t fork_child() {
    int fd[2];
    dieif(pipe(fd), "pipe failed: %s\n", errstr);
    pid_t pid = fork();
    dieif(pid == -1, "fork failed: %s\n", errstr);
    if (pid) {
        dieif(close(fd[0]), "close failed: %s\n", errstr);
        dieif(dup2(fd[1], fileno(stdout)) == -1, "dup2 failed: %s\n", errstr);
        dieif(dup2(fd[1], fileno(stderr)) == -1, "dup2 failed: %s\n", errstr);
        dieif(close(fd[1]), "close failed: %s\n", errstr);
    } else {
        dieif(close(fd[1]), "close failed: %s\n", errstr);
        dieif(dup2(fd[0], fileno(stdin)) == -1, "dup2 failed: %s\n", errstr);
        dieif(close(fd[0]), "close failed: %s\n", errstr);
    }
    return pid;
}

void wait_child() {
    dieif(fflush(stdout), "fflush error: %s\n", errstr);
    dieif(fflush(stderr), "fflush error: %s\n", errstr);
    dieif(fclose(stdout), "close failed: %s\n", errstr);
    dieif(fclose(stderr), "close failed: %s\n", errstr);
    int status;
    dieif(wait(&status) == -1, "wait failed: %s\n", errstr);
}

size_t strcnt(char *str, char c) {
    int n = 0;
    while (str = strchr(str, c)) { str++; n++; }
    return n;
}

int main(int argc, char **argv) {
    parse_opts(&argc,&argv);
    dieif(argc < 1, "usage: %s\n", usage);

    cmd_t cmd = parse_cmd(argv[0]);
    argv++; argc--;

    int is_tty = isatty(1);
    if (is_tty && cmd == CAT) cmd = PRINT;

    switch (cmd) {
        case STRINGS: {
            off_t strings_off, offsets_off, reverse_off, cmph_off, maxlen = 0;

            FILE *strings = fopen(strings_file, "w+");
            dieif(!strings, "error opening %s: %s\n", strings_file, errstr);
            strings_off = ftello(strings);

            off_t n = 0;
            off_t allocated = 4096;
            off_t *offsets = malloc(allocated*sizeof(off_t));

            FILE *file;
            char *last = NULL;
            for (int i = 0; file = fopenr_arg(argc, argv, i); i++) {
                size_t length;
                char *line, *buffer = NULL;
                while (line = get_line(file, &buffer, &length)) {
                    char *nl = strchr(line, '\n');
                    if (nl) *nl = '\0';
                    dieif(last && !strcmp(last, line), "strings not unique: %s\n", last);
                    free(last); last = strdup(line);
                    if (allocated <= n) {
                        allocated *= 2;
                        offsets = realloc(offsets, allocated*sizeof(off_t));
                    }
                    offsets[n++] = ftello(strings);
                    off_t len = strlen(line);
                    if (maxlen < len) maxlen = len;
                    fwrite1(line, len+1, strings);
                }
                dieif(fileno(file) && fclose(file),
                      "error closing %s: %s\n", argstr(argv[i]), errstr);
            }
            dieif(!n, "no strings provided\n");
            offsets = realloc(offsets, n*sizeof(off_t));

            // write out the table of offsets
            ff_align(strings, sizeof(off_t));
            offsets_off = ftello(strings);
            fwriten(offsets, sizeof(off_t), n, strings);

            // mmap the written strings data for reading
            char *data = mmap(
                NULL,
                ftello(strings),
                PROT_READ,
                MAP_PRIVATE,
                fileno(strings),
                0
            );
            dieif(data == MAP_FAILED, "mmap failed for %s: %s\n", strings_file, errstr);

            // generate a minimal perfect hash for strings
            cmph_state_t state = {offsets, 0, data};
            cmph_io_adapter_t adapter;
            adapter.data = (void*)&state;
            adapter.nkeys = n;
            adapter.read = key_read;
            adapter.rewind = key_rewind;
            adapter.dispose = key_dispose;

            // TODO: cmph segfaults on some binary data.
            cmph_config_t *config = cmph_config_new(&adapter);
            cmph_config_set_algo(config, CMPH_CHD);
            cmph_t *hash = cmph_new(config);
            dieif(!hash, "error generating hash\n");
            cmph_config_destroy(config);

            off_t *reverse = offsets;
            offsets = (off_t*)(data + offsets_off);
            for (int i = 0; i < n; i++) {
                char *str = data + offsets[i];
                cmph_uint32 h = cmph_search(hash, str, strlen(str));
                reverse[h] = i;
            }

            // write out reverse map of offsets
            ff_align(strings, sizeof(off_t));
            reverse_off = ftello(strings);
            fwriten(reverse, sizeof(off_t), n, strings);

            // write out cmph structure
            ff_align(strings, sizeof(off_t));
            cmph_off = ftello(strings);
            cmph_dump(hash, strings);

            // write n and table of offsets
            ff_align(strings, sizeof(off_t));
            fwrite1(&maxlen, sizeof(off_t), strings);
            fwrite1(&cmph_off, sizeof(off_t), strings);
            fwrite1(&reverse_off, sizeof(off_t), strings);
            fwrite1(&offsets_off, sizeof(off_t), strings);
            fwrite1(&strings_off, sizeof(off_t), strings);
            fwrite1(&n, sizeof(off_t), strings);

            dieif(fclose(strings), "error closing %s: %s\n", strings_file, errstr);
            return 0;
        }
        case ENCODE: {
            int string_fields = 0;
            dieif(!fields_arg, "use -f to provide fields\n");
            long long n = strcnt(fields_arg, ',') + 1;
            field_spec_t *specs = malloc(n*sizeof(field_spec_t));
            for (int i = 0; i < n; i++) {
                char *comma = strchr(fields_arg, ',');
                if (comma) *comma = '\0';
                specs[i] = parse_field_spec(fields_arg);
                if (specs[i].type == STRING) string_fields++;
                fields_arg = comma + 1;
            }
            if (string_fields) load_strings();
            write_header(stdout, n, specs);

            FILE *file;
            for (int i = 0; file = fopenr_arg(argc, argv, i); i++) {
                size_t length;
                char *line, *buffer = NULL;
                while (line = get_line(file, &buffer, &length)) {
                    for (int j = 0; j < n; j++) {
                        switch (specs[j].type) {
                            case INTEGER: {
                                errno = 0;
                                long long v = strtoll(line, &line, 10);
                                dieif(errno == EINVAL && v == 0,
                                      "invalid integer: %s\n", ltrunc(line));
                                dieif(errno == ERANGE && v == LLONG_MIN,
                                      "integer underflow: %s\n", ltrunc(line));
                                dieif(errno == ERANGE && v == LLONG_MAX,
                                      "integer overflow: %s\n", ltrunc(line));
                                fwrite1(&v, sizeof(v), stdout);
                                break;
                            }
                            case FLOAT: {
                                char *p;
                                errno = 0;
                                double v = strtod(line, &p);
                                dieif(p == line && v == 0.0,
                                      "invalid float: %s\n", ltrunc(line));
                                dieif(errno == ERANGE && v == 0.0,
                                      "float underflow: %s\n", ltrunc(line));
                                dieif(errno == ERANGE && abs(v) == HUGE_VAL,
                                      "float overflow: %s\n", ltrunc(line));
                                fwrite1(&v, sizeof(v), stdout);
                                line = p;
                                break;
                            }
                            case STRING: {
                                off_t len = buffer + length - line;
                                if (j < n-1) {
                                    char *end = memchr(line, '\t', len);
                                    dieif(!end, "tab expected after: %s\n", ltrunc(line));
                                    len = end - line;
                                }
                                long long v = string_to_index(line, len);
                                fwrite1(&v, sizeof(v), stdout);
                                line += len;
                                break;
                            }
                            default:
                                die("encoding type %s not yet implemented\n", typestr(specs[j].type));
                        }
                        if (j < n-1) {
                            dieif(line[0] != '\t', "tab expected: %s\n", ltrunc(line));
                            line++;
                        } else {
                            dieif(!(line[0] == '\n' || line[0] == '\r'),
                                  "end of line expected: %s\n", ltrunc(line));
                        }
                    }
                }
                dieif(fileno(file) && fclose(file),
                      "error closing %s: %s\n", argstr(argv[i]), errstr);
            }
            return 0;
        }
        case CAT: {
            h = read_headers(argc, argv);
            h_size = header_size(h);
            write_header(stdout, h.field_count, h.field_specs);

            FILE *file;
            for (int i = 0; file = fopenr_arg(argc, argv, i); i++) {
                dieif(fileno(file) && fseeko(file, h_size, SEEK_SET),
                      "seek error for %s: %s\n", argstr(argv[i]), errstr);

                for (;;) {
                    char buffer[1<<15];
                    size_t r = fread(buffer, 1, sizeof(buffer), file);
                    dieif(ferror(file), "error reading %s: %s\n", argstr(argv[i]), errstr);
                    size_t w = fwrite(buffer, 1, r, stdout);
                    dieif(w < r && ferror(stdout), "write error: %s\n", errstr);
                    if (r < sizeof(buffer) && feof(file)) break;
                }

                dieif(fileno(file) && fclose(file),
                      "error closing %s: %s\n", argstr(argv[i]), errstr);
            }
            return 0;
        }
        case CUT: {
            h = read_headers(argc, argv);
            h_size = header_size(h);

            int n, *cut;
            if (!fields_arg) {
                n = h.field_count;
                cut = malloc(n*sizeof(int));
                for (int i = 0; i < h.field_count; i++) cut[i] = i;
            } else {
                n = strcnt(fields_arg, ',') + 1;
                cut = malloc(n*sizeof(int));
                for (int i = 0; i < n; i++) {
                    char *comma = strchr(fields_arg, ',');
                    if (comma) *comma = '\0';
                    cut[i] = -1;
                    for (int j = 0; j < h.field_count; j++)
                        if (!strcmp(fields_arg, h.field_specs[j].name))
                            cut[i] = j;
                    dieif(cut[i] == -1, "invalid field: %s\n", fields_arg);
                    fields_arg = comma + 1;
                }
            }

            field_spec_t *specs = malloc(n*sizeof(field_spec_t));
            for (int i = 0; i < n; i++) specs[i] = h.field_specs[cut[i]];
            if (is_tty) {
                if (!fork_child()) {
                    h.field_count = n;
                    free(h.field_specs);
                    h.field_specs = specs;
                    argc = 0;
                    argv = NULL;
                    goto print;
                }
            } else {
                write_header(stdout, n, specs);
            }
            free(specs);

            FILE *file;
            long long *record = malloc(h.field_count*sizeof(long long));
            for (int i = 0; file = fopenr_arg(argc, argv, i); i++) {
                dieif(fileno(file) && fseeko(file, h_size, SEEK_SET),
                      "seek error for %s: %s\n", argstr(argv[i]), errstr);

                for (;;) {
                    int r = fread(record, sizeof(long long), h.field_count, file);
                    if (!r && feof(file)) break;
                    dieif(r < h.field_count, "unexpected eof in %s: %s\n", argstr(argv[i]), errstr);
                    for (int j = 0; j < n; j++)
                        fwrite1(record + cut[j], sizeof(long long), stdout);
                }

                dieif(fileno(file) && fclose(file),
                      "error closing %s: %s\n", argstr(argv[i]), errstr);
            }
            if (is_tty) wait_child();
            return 0;
        }
        case SORT: {
            dieif(!argc, "sorting stdin not supported\n");
            h = read_headers(argc, argv);
            h_size = header_size(h);

            if (!fields_arg) {
                sort_n = h.field_count;
                sort_order = malloc(sort_n*sizeof(int));
                for (int i = 0; i < h.field_count; i++) sort_order[i] = i+1;
            } else {
                sort_n = strcnt(fields_arg, ',') + 1;
                sort_order = calloc(sort_n, sizeof(int));
                for (int i = 0; i < sort_n; i++) {
                    int sign = 1;
                    if (fields_arg[0] == '-') {
                        fields_arg++;
                        sign = -1;
                    }
                    char *comma = strchr(fields_arg, ',');
                    if (comma) *comma = '\0';
                    for (int j = 0; j < h.field_count; j++)
                        if (!strcmp(fields_arg, h.field_specs[j].name))
                            sort_order[i] = sign*(j+1);
                    dieif(!sort_order[i], "invalid field: %s\n", fields_arg);
                    fields_arg = comma + 1;
                }
            }

            for (int i = 0; i < argc; i++) {
                dieif(!strcmp(argv[i], "-"), "sorting stdin not supported\n");
                FILE *file = fopen(argv[i], "r+");
                dieif(!file, "error opening %s: %s\n", argv[i], errstr);

                struct stat fs;
                dieif(fstat(fileno(file), &fs), "stat error for %s: %s\n", argv[i], errstr);

                char *mapped = mmap(
                    NULL,
                    fs.st_size,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED,
                    fileno(file),
                    0
                );
                dieif(mapped == MAP_FAILED, "mmap failed for %s: %s\n", argv[i], errstr);
                dieif(memcmp(mapped, &preamble, sizeof(preamble_t)), "invalid odb file\n");

                long long *data = (long long*)(mapped+h_size);
                size_t n = (fs.st_size-h_size)/(h.field_count*sizeof(long long));
                su_smoothsort(data, 0, n, lt_records, swap_records);

                dieif(munmap(mapped, fs.st_size), "munmap failed for %s: %s\n", argv[i], errstr);
                if (is_tty && argc == 1) goto print;
                dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
            }
            return 0;
        }
        case PRINT: {
            h = read_headers(argc, argv);
            h_size = header_size(h);
        print:
            if (is_tty && !fork_child()) {
                execlp("less", "less", NULL);
                die("exec failed: %s\n", errstr);
            }

            int string_fields = 0;
            for (int j = 0; j < h.field_count; j++) {
                printf(" %20s", h.field_specs[j].name);
                if (h.field_specs[j].type == STRING) string_fields++;
            }
            printf("\n");
            if (string_fields) load_strings();
            int dashes = 21*(h.field_count-string_fields)+(string_maxlen+1)*string_fields+1;
            for (int j = 0; j < dashes; j++) printf("-");
            printf("\n");
            char *string_format;
            asprintf(&string_format, " %%-%ds", string_maxlen);

            FILE *file;
            for (int i = 0; file = fopenr_arg(argc, argv, i); i++) {
                dieif(fileno(file) && fseeko(file, h_size, SEEK_SET),
                      "seek error for %s: %s\n", argstr(argv[i]), errstr);

                for (;;) {
                    int c = getc(file);
                    if (c == EOF) {
                        dieif(ferror(file), "read error for %s: %s\n", argstr(argv[i]), errstr);
                        break;
                    }
                    dieif(ungetc(c, file) == EOF, "ungetc failed: %s\n", errstr);
                    for (int j = 0; j < h.field_count; j++) {
                        switch (h.field_specs[j].type) {
                            case INTEGER: {
                                long long v;
                                fread1(&v, sizeof(v), file);
                                printf(" %20lld", v);
                                break;
                            }
                            case FLOAT: {
                                double v;
                                fread1(&v, sizeof(v), file);
                                printf(float_format, v);
                                break;
                            }
                            case STRING: {
                                long long v;
                                fread1(&v, sizeof(v), file);
                                char *s = index_to_string(v);
                                printf(string_format, s);
                                break;
                            }
                            default:
                                die("invalid type: %s (%u)\n",
                                    typestr(h.field_specs[j].type),
                                    h.field_specs[j].type);
                        }
                    }
                    printf("\n");
                }

                dieif(fileno(file) && fclose(file),
                      "error closing %s: %s\n", argstr(argv[i]), errstr);
            }
            if (is_tty) wait_child();
            return 0;
        }
        case HELP:
            printf("%s\n\ncommands:\n%s\noptions:\n%s\n", usage, cmdstr, optstr);
            return 0;
        case INVALID:
            die("invalid command: %s\n", argv[-1]);
        default:
            die("command not implemented: %s\n", argv[-1]);
    }
}
