#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <getopt.h>

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
    static char* shortopts = "h";
    static struct option longopts[] = {
        { "help", no_argument, 0, 'h' },
        { 0, 0, 0, 0 }
    };
    int c;
    while ((c = getopt_long(*argcp,*argvp,shortopts,longopts,0)) != -1) {
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

typedef enum {
    INVALID = 0,
    ENCODE,
    DECODE,
    HELP
} cmd_t;

int main(int argc, char **argv) {
    parse_opts(&argc,&argv);
    dieif(argc < 1, "usage: %s\n", usage);

    cmd_t cmd =
        !strcmp(argv[0], "encode") ? ENCODE :
        !strcmp(argv[0], "decode") ? DECODE :
        !strcmp(argv[0], "help")   ? HELP   :
        INVALID;
    argv++; argc--;

    switch (cmd) {
    case ENCODE:
        warn("encoding...\n");
        return 0;
    case DECODE:
        warn("decoding...\n");
        return 0;
    case HELP:
        printf("%s\n\ncommands:\n%s\noptions:\n%s\n", usage, cmdstr, optstr);
        return 0;
    default:
        die("invalid command: %s\n", argv[-1]);
    }

    die("end of main reached\n");
}
