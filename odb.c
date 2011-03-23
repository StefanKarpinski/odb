#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <getopt.h>
#include <math.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <sys/sysctl.h>

#ifndef __APPLE__
#include <stdio.h>
#include <stdio_ext.h>
#define fpurge(stream) __fpurge(stream)
#endif

// external dependencies:
#include <cmph.h>

// internal headers:
#include "smoothsort.h"

#define errstr                  strerror(errno)

#define warn(fmt,args...)     { fflush(stdout); fsync(fileno(stdout)); \
                                fprintf(stderr,fmt,##args);            \
                                fflush(stderr); fsync(fileno(stderr)); }

#define die(fmt,args...)      { fpurge(stdout); fsync(fileno(stdout)); \
                                fprintf(stderr,fmt,##args);            \
                                fflush(stderr); fsync(fileno(stderr)); \
                                fclose(stderr); exit(1); }

#define dieif(cond,fmt,args...) if (cond) die(fmt,##args);

#define reinterpret(type,value) *((type*)&value)

static const char *const usage =
    "usage: odb [command] [options] [arguments...]";

static const char *const cmdstr =
    "  strings    Generate strings index\n"
    "  encode     Encode data to ODB format\n"
    "  decode     Decode data from ODB format\n"
    "  print      Print data in tabular format\n"
    "  cat        Output data from files with like schemas\n"
    "  paste      Paste columns from different files\n"
    "  join       Join files on specified fields\n"
    "  sort       Sort by specified fields (in place)\n"
    "  help       Print this message\n"
;

static const char *const optstr =
    " -d --delim=<char>         Delimit fields by <char>\n"
    " -C --csv                  CSV encode/decode mode\n"
    " -P --psql=<table>         PosgreSQL encode/decode mode\n"
    " -M --mysql=<table>        MySQL encode/decode mode\n"
    " -f --fields=<fields>      Comma-sparated fields\n"
    " -x --extract              String extraction mode for encode\n"
    " -s --strings=<file>       Use <file> as string index\n"
    " -r --range=<range>        Output a range slice of records\n"
    " -n --count=<n>            Output at most <n> records\n"
    " -N --line-numbers[=<b>]   Output with line numbers\n"
    " -e --float-e              Use %e to print floats\n"
    " -g --float-g              Use %g to print floats\n"
    " -T --timestamp[=<fmt>]    Use <fmt> as a timestamp format\n"
    " -D --date[=<fmt>]         Use <fmt> as a date format\n"
    " -q --quiet                Suppress output for sort\n"
    " -y --tty                  Force acting as for a TTY\n"
    " -Y --no-tty               Force acting as not for a TTY\n"
    " -h --help                 Print this message\n"
;

typedef enum {
    DELIMITED,
    TABLE,
    CSV,
    PSQL,
    MYSQL
} codec_t;

typedef struct {
    off_t start, step, stop;
} range_t;

static codec_t codec = DELIMITED;
static char *table_name = NULL;
static char *delim = "\t";
static char *fields_arg = NULL;
static char *strings_file = "strings.idx";
static int extract = 0;
static range_t range = {1,1,-1};
static long long count = LLONG_MAX;
static long long line_number = 1;
static int print_line_numbers = 0;
static char float_format_char = 'f';
static char *timestamp_fmt = "%F %T";
static char *date_fmt = "%F";
static int quiet = 0;
static int tty = 0;

char *ltrunc(char *line) {
    char *nl = strchr(line, '\n');
    if (nl) *nl = '\0';
    return line;
}

long long parse_ll(char **str) {
    errno = 0;
    long long v = strtoll(*str, str, 10);
    dieif(errno == EINVAL && v == 0, "invalid integer: %s\n", ltrunc(*str));
    dieif(errno == ERANGE && v == LLONG_MIN, "integer underflow: %s\n", ltrunc(*str));
    dieif(errno == ERANGE && v == LLONG_MAX, "integer overflow: %s\n", ltrunc(*str));
    return v;
}

double parse_d(char **str) {
    char *p;
    errno = 0;
    double v = strtod(*str, &p);
    dieif(p == *str && v == 0.0, "invalid float: %s\n", ltrunc(*str));
    dieif(errno == ERANGE && v == 0.0, "float underflow: %s\n", ltrunc(*str));
    dieif(errno == ERANGE && abs(v) == HUGE_VAL, "float overflow: %s\n", ltrunc(*str));
    *str = p;
    return v;
}

range_t make_range(long long start, long long step, long long stop) {
    range_t r;
    r.start = start;
    r.step = step;
    r.stop = stop;
    return r;
}

range_t parse_range(char *str) {
    char *p = str;
    long long a = 1;
    if (*p == ':') { p++; goto post_a; };
    a = parse_ll(&p);
    if (!*p) return make_range(a,1,a);
    if (*p++ != ':') goto invalid;
post_a:
    if (!*p) return make_range(a,1,-1);
    long long b = parse_ll(&p);
    if (!*p) return make_range(a,1,b);
    if (*p++ != ':') goto invalid;
    if (!*p) return make_range(a,b,-1);
    long long c = parse_ll(&p);
    if (!*p) return make_range(a,b,c);
invalid:
    die("invalid range: %s\n", str);
}

void parse_opts(int *argcp, char ***argvp) {
    static char* shortopts = "d:CP:M:f:s:xr:n:N::egT::D::qyYh";
    static struct option longopts[] = {
        { "delim",          required_argument, 0, 'd' },
        { "csv",            no_argument,       0, 'C' },
        { "psql",           required_argument, 0, 'P' },
        { "mysql",          required_argument, 0, 'M' },
        { "fields",         required_argument, 0, 'f' },
        { "strings",        required_argument, 0, 's' },
        { "extract",        no_argument,       0, 'x' },
        { "range",          required_argument, 0, 'r' },
        { "count",          required_argument, 0, 'n' },
        { "line-numbers",   optional_argument, 0, 'N' },
        { "float-e",        no_argument,       0, 'e' },
        { "float-g",        no_argument,       0, 'g' },
        { "timestamp",      required_argument, 0, 'T' },
        { "date",           required_argument, 0, 'D' },
        { "quiet",          no_argument,       0, 'q' },
        { "tty",            no_argument,       0, 'y' },
        { "no-tty",         no_argument,       0, 'y' },
        { "help",           no_argument,       0, 'h' },
        { 0, 0, 0, 0 }
    };
    int c;
    while ((c = getopt_long(*argcp, *argvp, shortopts, longopts, 0)) != -1) {
        switch(c) {
            case 'd':
                delim = optarg;
                break;
            case 'C':
                codec = CSV;
                break;
            case 'P':
                codec = PSQL;
                table_name = optarg;
                break;
            case 'M':
                codec = MYSQL;
                table_name = optarg;
                break;
            case 'f':
                fields_arg = optarg;
                break;
            case 's':
                strings_file = optarg;
                break;
            case 'x':
                extract = 1;
                break;
            case 'r':
                range = parse_range(optarg);
                dieif(!range.start, "invalid range: start zero\n");
                dieif(!range.step, "invalid range: step zero\n");
                dieif(!range.stop, "invalid range: stop zero\n");
                break;
            case 'n':
                count = parse_ll(&optarg);
                break;
            case 'N':
                if (optarg) line_number = parse_ll(&optarg);
                print_line_numbers = 1;
                break;
            case 'e':
                float_format_char = 'e';
                break;
            case 'g':
                float_format_char = 'g';
                break;
            case 'T':
                timestamp_fmt = optarg;
                break;
            case 'D':
                date_fmt = optarg;
                break;
            case 'q':
                quiet = 1;
                break;
            case 'y':
                tty = 1;
                break;
            case 'Y':
                tty = -1;
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
    DECODE,
    PRINT,
    CAT,
    PASTE,
    JOIN,
    SORT,
    RENAME,
    CAST,
    HELP,
    INVALID
} cmd_t;

cmd_t parse_cmd(char *str) {
    return !strcmp(str, "strings") ? STRINGS :
           !strcmp(str, "encode")  ? ENCODE  :
           !strcmp(str, "decode")  ? DECODE  :
           !strcmp(str, "print")   ? PRINT   :
           !strcmp(str, "cat")     ? CAT     :
           !strcmp(str, "cut")     ? CAT     :
           !strcmp(str, "paste")   ? PASTE   :
           !strcmp(str, "join")    ? JOIN    :
           !strcmp(str, "sort")    ? SORT    :
           !strcmp(str, "rename")  ? RENAME  :
           !strcmp(str, "cast")    ? CAST    :
           !strcmp(str, "help")    ? HELP    : INVALID;
}

const int n_types = 5;

typedef enum {
    INTEGER,
    FLOAT,
    STRING,
    TIMESTAMP,
    DATE,
    UNSPECIFIED
} field_type_t;

char *typestrs[] = {
    "int",
    "float",
    "string",
    "timestamp",
    "date"
};

char *psql_types[] = {
    "bigint",
    "double precision",
    "text",
    "timestamp",
    "date"
};

char *typestr(field_type_t t) {
    if (t < 0 || n_types <= t) return "<unknown>";
    return typestrs[t];
}

field_type_t parse_type(const char *const str) {
    for (field_type_t t = 0; t < n_types; t++)
        if (!strcmp(typestrs[t], str)) return t;
    die("invalid type: %s\n", str);
}

typedef struct {
    char magic[4];
    unsigned long long bom;
} __attribute__ ((__packed__)) preamble_t;

static const preamble_t preamble = {"odb", 0x0123456789abcdef};

#define name_size (256-sizeof(field_type_t))

typedef struct {
    field_type_t type;
    char name[name_size];
} __attribute__ ((__packed__)) field_spec_t;

field_spec_t parse_field_spec(const char *const str) {
    field_spec_t spec;
    bzero(&spec, sizeof(spec));
    char *colon = strchr(str, ':');
    dieif(!colon, "invalid field spec: %s\n", str);
    int n = colon++ - str;
    dieif(n >= name_size, "field name too long: %s\n", str);
    memcpy(spec.name, str, n);
    spec.type = parse_type(colon);
    return spec;
}

typedef struct {
    char from_name[name_size];
    char to_name[name_size];
    field_type_t to_type;
} cut_spec_t;

cut_spec_t parse_cut_spec(const char *str) {
    cut_spec_t spec;
    bzero(&spec, sizeof(spec));
    spec.to_type = UNSPECIFIED;
    off_t n = strcspn(str, "=:");
    dieif(n >= name_size, "invalid field: %s\n", str);
    memcpy(spec.from_name, str, n);
    if (str[n] != '=') memcpy(spec.to_name, str, n);
    if (!str[n]) return spec;
    if (str[n] == '=') {
        str += n + 1;
        n = strcspn(str, "=:");
        dieif(n >= name_size, "field name too long: %s\n", str);
        memcpy(spec.to_name, str, n);
    }
    if (str[n]) {
        dieif(str[n] != ':', "invalid field cut: %s\n", str);
        spec.to_type = parse_type(str + n + 1);
    }
    return spec;
}

typedef struct {
    int from;
    field_spec_t field_spec;
} cut_t;

char *get_line(FILE *file, char **buffer, size_t *len) {
#ifdef __APPLE__
    *buffer = fgetln(file,len);
    dieif(ferror(file), "error reading line: %s\n", errstr);
    return *buffer;
#else
    size_t n;
    int r = getline(buffer,&n,file);
    if (r != -1) {
        *len = strlen(*buffer);
        return *buffer;
    }
    if (feof(file)) {
        if (buffer) free(*buffer);
        return *buffer = NULL;
    } else if (ferror(file)) {
        die("error reading line: %s\n", errstr);
    }
#endif
}

void fwriten(const void *restrict ptr, size_t size, size_t n, FILE *restrict stream) {
    dieif(fwrite(ptr, size, n, stream) != n, "write error: %s\n", errstr);
}

void fwrite1(const void *restrict ptr, size_t size, FILE *restrict stream) {
    fwriten(ptr, size, 1, stream);
}

void freadn(void *restrict ptr, size_t size, size_t n, FILE *restrict stream) {
    size_t r = fread(ptr, size, n, stream);
    if (r == n) return;
    dieif(errno, "read error: %s\n", errstr);
    exit(1); // die silently
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

int string_fields;

header_t read_header(FILE *file) {
    char preamble_b[sizeof(preamble_t)];
    fread1(preamble_b, sizeof(preamble_t), file);
    dieif(memcmp(preamble_b, &preamble, sizeof(preamble_t)), "invalid odb file\n");

    header_t h;
    fread1(&h.field_count, sizeof(h.field_count), file);
    h.field_specs = malloc(h.field_count*sizeof(field_spec_t));
    freadn(h.field_specs, sizeof(field_spec_t), h.field_count, file);

    string_fields = 0;
    for (int i = 0; i < h.field_count; i++)
        if (h.field_specs[i].type == STRING) string_fields++;

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

int seekable(FILE *file) {
    if (!fseeko(file, 0, SEEK_CUR)) return 1;
    if (errno == EBADF || errno == ESPIPE) return 0;
    die("seek error: %s\n", errstr);
}

FILE **files = NULL;
int *writable = NULL;

FILE *fopenr_arg(int argc, char **argv, int i, int try_write) {
    if (argc <= i) return NULL;
    if (!files) {
        files = calloc(argc+1, sizeof(FILE*));
        writable = calloc(argc+1, sizeof(int*));
    }
    if (files[i]) {
        writable[i] &= try_write;
        if (seekable(files[i])) {
            off_t offset = ftello(files[i]);
            char *mode = writable[i] ? "r+" : "r";
            files[i] = freopen(NULL, mode, files[i]);
            dieif(!files[i], "error reopening %s: %s\n", argv[i], errstr);
            dieif(fseeko(files[i], offset, SEEK_SET),
                  "seek error for %s: %s\n", argv[i], errstr);
        }
        goto lock_and_return;
    }
    if (!strcmp(argv[i], "-")) return files[i] = stdin;

    char *mode = try_write ? "r+" : "r";
    files[i] = fopen(argv[i], mode);
    writable[i] = try_write && files[i];
    if (try_write && !files[i] && errno == EACCES)
        files[i] = fopen(argv[i], "r");
    dieif(!files[i], "error opening %s: %s\n", argv[i], errstr);
lock_and_return:
    dieif(flock(fileno(files[i]), writable[i] ? LOCK_EX : LOCK_SH) && errno != ENOTSUP,
          "error locking %s: %s\n", argv[i], errstr);
    return files[i];
}

header_t read_headers(int argc, char **argv, int w) {
    header_t h;
    FILE *file;
    for (int i = 0; file = fopenr_arg(argc, argv, i, w); i++) {
        if (i == 0) h = read_header(file);
        else dieif(!check_header(file,h), "field spec mismatch: %s\n", argv[i]);
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
    dieif(!strings, "error opening %s: %s\n", strings_file, errstr);
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

pid_t fork_child(int redirect_stderr) {
    int fd[2];
    dieif(pipe(fd), "pipe failed: %s\n", errstr);
    pid_t pid = fork();
    dieif(pid == -1, "fork failed: %s\n", errstr);
    if (pid) {
        dieif(close(fd[0]), "close failed: %s\n", errstr);
        dieif(dup2(fd[1], fileno(stdout)) == -1, "dup2 failed: %s\n", errstr);
        if (redirect_stderr)
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

char *timelikefmt(field_type_t t) {
    switch (t) {
        case TIMESTAMP: return timestamp_fmt;
        case DATE:      return date_fmt;
    }
    die("type %s is not time-like\n", typestr(t));
}

void type_as_float(field_type_t type, field_spec_t *specs, size_t n) {
    for (int i = 0; i < n; i++)
        if (specs[i].type == type) specs[i].type = FLOAT;
}

#define pipe_to_print(cmd) ((cmd) == ENCODE && !extract || \
                            (cmd) == CAT || cmd == PASTE || \
                            (cmd) == SORT && !quiet)

int main(int argc, char **argv) {
    parse_opts(&argc,&argv);
    dieif(argc < 1, "usage: %s\n", usage);

    cmd_t cmd = parse_cmd(argv[0]);
    argv++; argc--;

    int is_tty = tty < 0 ? 0 : tty || isatty(fileno(stdout));
    if (is_tty && pipe_to_print(cmd) && !fork_child(0)) {
        argc = 0;
        cmd = PRINT;
    }
    if (!argc) {
        argc = 1;
        argv[0] = "-";
    }
    if (cmd == PRINT) {
        cmd = DECODE;
        codec = TABLE;
    }

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
            for (int i = 0; file = fopenr_arg(argc, argv, i, 0); i++) {
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
                dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
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
            long long n;
            field_spec_t *specs;

            switch (codec) {
                case DELIMITED: {
                    dieif(!fields_arg, "use -f to provide a field schema\n");
                    n = strcnt(fields_arg, ',') + 1;
                    specs = malloc(n*sizeof(field_spec_t));
                    string_fields = 0;
                    for (int i = 0; i < n; i++) {
                        char *comma = strchr(fields_arg, ',');
                        if (comma) *comma = '\0';
                        specs[i] = parse_field_spec(fields_arg);
                        if (specs[i].type == STRING) string_fields++;
                        fields_arg = comma + 1;
                    }
                    break;
                }
                case TABLE: die("formated table encoding not supported\n");
                case CSV:   die("CSV encoding not yet supported (try -d, instead)\n");
                case PSQL:  die("PostgreSQL encoding not yet supported\n");
                case MYSQL: die("MySQL encoding not yet supported\n");
                default: die("unsupported codec\n");
            }

            if (!extract) {
                write_header(stdout, n, specs);
                if (string_fields) load_strings();
            }

            if (!timestamp_fmt) type_as_float(TIMESTAMP, specs, n);
            if (!date_fmt) type_as_float(DATE, specs, n);

            FILE *file;
            for (int i = 0; file = fopenr_arg(argc, argv, i, 0); i++) {
                size_t length;
                char *line, *buffer = NULL;
                while (line = get_line(file, &buffer, &length)) {
                    for (int j = 0; j < n; j++) {
                        switch (specs[j].type) {
                            case INTEGER: {
                                long long v = parse_ll(&line);
                                if (!extract) fwrite1(&v, sizeof(v), stdout);
                                break;
                            }
                            case FLOAT: {
                                double v = parse_d(&line);
                                if (!extract) fwrite1(&v, sizeof(v), stdout);
                                break;
                            }
                            case STRING: {
                                off_t len = buffer+length-line-1;
                                if (j < n-1) {
                                    char *end = memchr(line, delim[0], len);
                                    dieif(!end, "tab expected after: %s\n", ltrunc(buffer));
                                    len = end-line;
                                }
                                if (extract) {
                                    fwriten(line, 1, len, stdout);
                                    putchar('\n');
                                } else {
                                    long long v = string_to_index(line, len);
                                    fwrite1(&v, sizeof(v), stdout);
                                }
                                line += len;
                                break;
                            }
                            case TIMESTAMP:
                            case DATE: {
                                struct tm st;
                                char *fmt = timelikefmt(specs[j].type);
                                char *p = strptime(line, fmt, &st);
                                dieif(!p, "invalid timestamp: %s\n", ltrunc(buffer));
                                double v = (double) timegm(&st);
                                if (!extract) fwrite1(&v, sizeof(v), stdout);
                                line = p;
                                break;
                            }
                            default:
                                die("encoding type %s not yet implemented\n", typestr(specs[j].type));
                        }
                        if (j < n-1) {
                            if (line[0] != delim[0]) {
                                char *delim_name = delim[0] == '\t' ? "tab" : "delimiter";
                                die("%s expected: %s\n", delim_name, ltrunc(buffer));
                            }
                            line++;
                        } else {
                            dieif(!(line[0] == '\n' || line[0] == '\r'),
                                  "end of line expected: %s\n", ltrunc(buffer));
                        }
                    }
                }
                dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
            }
            if (is_tty) wait_child();
            return 0;
        }

        case DECODE: {
            h = read_headers(argc, argv, 0);
            h_size = header_size(h);
            if (string_fields) load_strings();

            if (!timestamp_fmt)
                type_as_float(TIMESTAMP, h.field_specs, h.field_count);
            if (!date_fmt)
                type_as_float(DATE, h.field_specs, h.field_count);

            char *pre, *inter, *post;
            char *integer_format, *float_format, *string_format, *time_format;

            switch (codec) {
                case DELIMITED:
                case PSQL: {
                    pre = print_line_numbers ? "%lld" : "";
                    inter = delim;
                    post = "\n";
                    integer_format = "%lld";
                    asprintf(&float_format, "%%.6%c", float_format_char);
                    string_format = "%s";
                    time_format = "%s";
                    break;
                }
                case TABLE: {
                    pre = print_line_numbers ? "%8lld:    " : " ";
                    inter = " ";
                    post = "\n";
                    integer_format = "%20lld";
                    asprintf(&float_format, "%%20.6%c", float_format_char);
                    asprintf(&string_format, "%%-%ds", string_maxlen);
                    time_format = "%20s";
                    break;
                }
                case CSV:   die("CSV decoding not yet supported (try -d, instead)\n");
                case MYSQL: die("MySQL decoding not yet supported\n");
                default: die("unsupported codec\n");
            }

            if (is_tty && !fork_child(1)) {
                switch (codec) {
                    case DELIMITED:
                    case TABLE:
                    case CSV:   execlp("less",  "less",  NULL); break;
                    case PSQL:  execlp("psql",  "psql",  NULL); break;
                    case MYSQL: execlp("mysql", "mysql", NULL); break;
                    default: die("unsupported codec\n");
                }
                die("exec failed: %s\n", errstr);
            }

            switch (codec) {
                case DELIMITED: break;
                case TABLE: {
                    if (print_line_numbers)
                        for (int k = 0; k < 12; k++) putchar(' ');

                    for (int j = 0; j < h.field_count; j++) {
                        char *name = h.field_specs[j].name;
                        size_t len = strlen(name);
                        switch (h.field_specs[j].type) {
                            case INTEGER:
                            case TIMESTAMP:
                            case DATE: {
                                int space = 21 - strlen(name);
                                for (int k = 0; k < space; k++) putchar(' ');
                                fwriten(name, 1, len, stdout);
                                break;
                            }
                            case FLOAT: {
                                int space = 21 - strlen(name);
                                for (int k = 0; k < space-7; k++) putchar(' ');
                                fwriten(name, 1, len, stdout);
                                if (j < h.field_count-1)
                                    for (int k = 0; k < 7; k++) putchar(' ');
                                break;
                            }
                            case STRING: {
                                int space = string_maxlen + 1 - strlen(name);
                                putchar(' ');
                                fwriten(name, 1, len, stdout);
                                if (j < h.field_count-1)
                                    for (int k = 0; k < space-1; k++) putchar(' ');
                                break;
                            }
                            default:
                                die("unsupported type: %s (%d)\n",
                                    typestr(h.field_specs[j].type),
                                    h.field_specs[j].type)
                        }
                    }
                    putchar('\n');

                    int dashes = 21*(h.field_count-string_fields)+(string_maxlen+1)*string_fields+1;
                    if (print_line_numbers) dashes += 12;
                    for (int j = 0; j < dashes; j++) putchar('-');
                    putchar('\n');
                    break;
                }
                case PSQL: {
                    printf("create table \"%s\" (\n", table_name);
                    for (int j = 0; j < h.field_count; j++) {
                        printf("  \"%s\" %s%s\n",
                               h.field_specs[j].name,
                               psql_types[h.field_specs[j].type],
                               j < h.field_count-1 ? "," : "");
                    }
                    printf(");\n");
                    printf("copy %s from stdin;\n", table_name);
                    break;
                }
                default: die("unsupported codec\n");
            }

            FILE *file;
            long long *record = malloc(h.field_count*sizeof(long long));
            for (int i = 0; file = fopenr_arg(argc, argv, i, 0); i++) {
                for (;;) {
                    int r = fread(record, sizeof(long long), h.field_count, file);
                    if (!r && feof(file)) break;
                    dieif(r < h.field_count,
                          "unexpected eof %s: %s\n", argv[i], errstr);
                    if (*pre) printf(pre, line_number++, delim);
                    for (int j = 0; j < h.field_count; j++) {
                        switch (h.field_specs[j].type) {
                            case INTEGER: {
                                printf(integer_format, record[j]);
                                break;
                            }
                            case FLOAT: {
                                printf(float_format, reinterpret(double,record[j]));
                                break;
                            }
                            case STRING: {
                                printf(string_format, index_to_string(record[j]));
                                break;
                            }
                            case TIMESTAMP:
                            case DATE: {
                                double dt = reinterpret(double,record[j]);
                                time_t tt = (time_t) round(dt);
                                struct tm st;
                                gmtime_r(&tt, &st);
                                char buffer[256];
                                char *fmt = timelikefmt(h.field_specs[j].type);
                                strftime(buffer, sizeof(buffer)-1, fmt, &st);
                                printf(time_format, buffer);
                            }
                        }
                        if (j < h.field_count-1) printf("%s", inter);
                    }
                    printf("%s", post);
                }
                dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
            }
            if (is_tty) wait_child();
            return 0;
        }

        case CAT: {
            int n;
            cut_t *cut;
            h = read_headers(argc, argv, 0);
            h_size = header_size(h);
        slice:
            if (!fields_arg) {
                n = h.field_count;
                cut = malloc(n*sizeof(cut_t));
                for (int i = 0; i < h.field_count; i++) {
                    cut[i].field_spec = h.field_specs[i];
                    cut[i].from = i;
                }
            } else {
                n = strcnt(fields_arg, ',') + 1;
                cut = malloc(n*sizeof(cut_t));
                for (int i = 0; i < n; i++) {
                    char *comma = strchr(fields_arg, ',');
                    if (comma) *comma = '\0';
                    cut_spec_t c = parse_cut_spec(fields_arg);
                    cut[i].from = -1;
                    for (int j = 0; j < h.field_count; j++) {
                        if (!strcmp(c.from_name, h.field_specs[j].name)) {
                            cut[i].from = j;
                            memcpy(cut[i].field_spec.name, c.to_name, name_size);
                            cut[i].field_spec.type = c.to_type != UNSPECIFIED ?
                                c.to_type : h.field_specs[j].type;
                            break;
                        }
                    }
                    dieif(cut[i].from == -1, "invalid field cut: %s\n", fields_arg);
                    fields_arg = comma + 1;
                }
            }

            field_spec_t *specs = malloc(n*sizeof(field_spec_t));
            for (int i = 0; i < n; i++) specs[i] = cut[i].field_spec;
            write_header(stdout, n, specs);
            free(specs);

            FILE *file;
            long long *record = malloc(h.field_count*sizeof(long long));
            for (int i = 0; file = fopenr_arg(argc, argv, i, 0); i++) {
                const int is_seekable = seekable(file);
                range_t r = range;
                if (is_seekable) {
                    if (r.start < 0 || r.stop < 0) {
                        struct stat fs;
                        dieif(fstat(fileno(file), &fs), "stat error for %s: %s\n", argv[i], errstr);
                        off_t end = (fs.st_size - h_size)/(h.field_count*sizeof(long long)) + 1;
                        if (r.start < 0) r.start += end;
                        if (r.stop  < 0) r.stop  += end;
                        if (r.start < 0) r.start = 1;
                    }
                    off_t ff = (r.start-1)*h.field_count*sizeof(long long);
                    fseeko(file, ff, SEEK_CUR);
                } else {
                    dieif(r.start < 0 && r.start != -1 || r.stop  < 0 && r.stop  != -1,
                          "negative range offsets cannot be used with streamed inputs\n");
                    dieif(r.step < 0,
                          "negative range strides cannot be used with streamed inputs\n");
                    if (r.start == -1) break;
                    if (r.stop  == -1) r.stop = LLONG_MAX;

                    for (int k = 0; k < r.start-1; k++) {
                        int r = fread(record, sizeof(long long), h.field_count, file);
                        if (!r && feof(file)) break;
                        dieif(r < h.field_count,
                              "unexpected eof %s: %s\n", argv[i], errstr);
                    }
                }
                for (long long j = 0; j < count; j++) {
                    off_t x = r.start + j*r.step;
                    if (r.step < 0 ? x < r.stop : x > r.stop) break;

                    int rn = fread(record, sizeof(long long), h.field_count, file);
                    if (!rn && feof(file)) break;
                    dieif(rn < h.field_count,
                          "unexpected eof %s: %s\n", argv[i], errstr);
                    for (int k = 0; k < n; k++)
                        fwrite1(record + cut[k].from, sizeof(long long), stdout);

                    if (is_seekable) {
                        off_t ff = (r.step-1)*h.field_count*sizeof(long long);
                        fseeko(file, ff, SEEK_CUR);
                    } else {
                        for (int k = 0; k < r.step-1; k++) {
                            int r = fread(record, sizeof(long long), h.field_count, file);
                            if (!r && feof(file)) break;
                            dieif(r < h.field_count,
                                  "unexpected eof %s: %s\n", argv[i], errstr);
                        }
                    }
                }
                dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
            }
            if (is_tty) wait_child();
            return 0;
        }

        case PASTE: {
            FILE *file;
            header_t ht = {0, NULL};
            int max_field_count = 0;
            int *field_counts = malloc(argc*sizeof(int));
            for (int i = 0; file = fopenr_arg(argc, argv, i, 0); i++) {
                header_t hi = read_header(file);
                ht.field_specs = realloc(
                    ht.field_specs,
                    (ht.field_count + hi.field_count)*sizeof(field_spec_t)
                );
                memcpy(
                    ht.field_specs + ht.field_count,
                    hi.field_specs, hi.field_count*sizeof(field_spec_t)
                );
                ht.field_count += hi.field_count;
                field_counts[i] = hi.field_count;
                if (max_field_count < hi.field_count)
                    max_field_count = hi.field_count;
                free_header(hi);
            }
            write_header(stdout, ht.field_count, ht.field_specs);
            long long *record = malloc(max_field_count*sizeof(long long));
            for (;;) {
                int done = 0;
                for (int i = 0; file = fopenr_arg(argc, argv, i, 0); i++) {
                    int r = fread(record, sizeof(long long), field_counts[i], file);
                    if (!r && feof(file)) {
                        done++;
                        dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
                        continue;
                    };
                    dieif(r < field_counts[i],
                          "unexpected eof %s: %s\n", argv[i], errstr);
                    fwriten(record, sizeof(long long), field_counts[i], stdout);
                }
                dieif(done && done < argc, "unequal records in inputs\n");
                if (done) break;
            }
            if (is_tty) wait_child();
            return 0;
        }

        case SORT: {
            h = read_headers(argc, argv, 1);
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
                    } else if (fields_arg[0] == '+') {
                        fields_arg++;
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

            FILE *file;
            for (int i = 0; file = fopenr_arg(argc, argv, i, 1); i++) {
                if (!seekable(file)) {
                    FILE *tmp = tmpfile();
                    write_header(tmp, h.field_count, h.field_specs);
                    long long *record = malloc(h.field_count*sizeof(long long));
                    for (;;) {
                        int r = fread(record, sizeof(long long), h.field_count, file);
                        if (!r && feof(file)) break;
                        dieif(r < h.field_count,
                              "unexpected eof %s: %s\n", argv[i], errstr);
                        fwriten(record, sizeof(long long), h.field_count, tmp);
                    }
                    free(record);
                    dieif(fseeko(tmp, h_size, SEEK_SET), "seek error: %s", errstr);
                    dieif(dup2(fileno(tmp), fileno(file)) == -1, "dup2 failed: %s\n", errstr);
                    file = files[i] = tmp;
                }
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

                long long *data = (long long*)(mapped + h_size);
                size_t n = (fs.st_size-h_size)/(h.field_count*sizeof(long long));
                su_smoothsort(data, 0, n, lt_records, swap_records);

                dieif(munmap(mapped, fs.st_size),
                      "munmap failed for %s: %s\n", argv[i], errstr);
                dieif(flock(fileno(file), LOCK_SH),
                      "error downgrading lock on %s: %s\n", argv[i], errstr);
            }
            fields_arg = NULL;
            if (quiet) return 0;

            write_header(stdout, h.field_count, h.field_specs);
            long long *records = malloc(argc*h.field_count*sizeof(long long));
            int *done = calloc(argc, sizeof(int));
            int donecount = 0;
            for (int i = 0; file = fopenr_arg(argc, argv, i, 0); i++) {
                int r = fread(records + i*h.field_count,
                              sizeof(long long), h.field_count, file);
                if (!r && feof(file)) {
                    done[i] = 1;
                    donecount++;
                    dieif(fclose(file), "error closing %s: %s\n", argv[i], errstr);
                    break;
                }
                dieif(r < h.field_count,
                      "unexpected eof %s: %s\n", argv[i], errstr);
            }
            while (donecount < argc) {
                int min = -1;
                for (int i = 0; i < argc; i++)
                    if (!done[i] && (min < 0 || lt_records(records, i, min)))
                        min = i;
                dieif(min < 0, "unexpected merge error\n");
                fwriten(records + min*h.field_count,
                        sizeof(long long), h.field_count, stdout);

                int r = fread(records + min*h.field_count,
                              sizeof(long long), h.field_count, files[min]);
                if (!r && feof(files[min])) {
                    done[min] = 1;
                    donecount++;
                    dieif(fclose(files[min]), "error closing %s: %s\n", argv[min], errstr);
                } else {
                    dieif(r < h.field_count,
                          "unexpected eof %s: %s\n", argv[min], errstr);
                }
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
            die("sorry, the %s command isn't implemented yet\n", argv[-1]);
    }
}
