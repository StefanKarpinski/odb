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
    "usage: odb [sub-command] [options] [arguments]";
static const char *const optstr =
    " -h --help               Print this message\n";

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
            printf("%s\n%s", usage, optstr);
            exit(0);
        case '?':
            die("valid options:\n%s", optstr);
        default:
            die("odb: unhandled option -- %c\n", c);
        }
    }
    *argvp += optind;
    *argcp -= optind;
}

int main(int argc, char **argv) {
    parse_opts(&argc,&argv);
}
