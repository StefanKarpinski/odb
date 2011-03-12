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
    "  encode             Encode data to ODB format\n"
    "  decode             Decone data from ODB format\n"
    "  help <command>     Print help for <command>\n"
    "  help               Print general help message\n"
;

static const char *const optstr =
    " -h --help           Print this message\n"
;

void parse_opts(int *argcp, char ***argvp) {
    static char* shortopts = "+h";
    static struct option longopts[] = {
        { "help", no_argument, 0, 'h' },
        { 0, 0, 0, 0 }
    };
    int c;
    while ((c = getopt_long(*argcp, *argvp, shortopts, longopts, 0)) != -1) {
        switch(c) {
            case 'h':
                printf("%s\n\ncommands:\n%s\noptions:\n%s\n", usage, cmdstr, optstr);
                exit(0);
            case '?':
                die("valid options:\n%s", optstr);
            default:
                die("unhandled option -- %c\n", c);
        }
    }
    *argvp += optind;
    *argcp -= optind;
}

const int INVALID = -1;

typedef enum {
    ENCODE,
    DECODE,
    HELP
} cmd_t;

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

static const char *const magic = "odb";
static const unsigned long long bom = 0x0123456789abcdef;

int main(int argc, char **argv) {
    parse_opts(&argc,&argv);
    dieif(argc < 1, "usage: %s\n", usage);

    cmd_t cmd = !strcmp(argv[0], "encode") ? ENCODE  :
                !strcmp(argv[0], "decode") ? DECODE  :
                !strcmp(argv[0], "help")   ? HELP    :
                                             INVALID ;
    argv++; argc--;

    switch (cmd) {
        case ENCODE: {
            fwrite1(magic, strlen(magic)+1, stdout);
            fwrite1(&bom, sizeof(bom), stdout);

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

            fwrite1(&n, sizeof(n), stdout);
            fwriten(specs, sizeof(field_spec_t), n, stdout);

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
            warn("decoding...\n");
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
