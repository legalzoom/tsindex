#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/prctl.h>
#include <unistd.h>

static void *worker(void *unused) {
    (void)unused;
    for (;;) pause();
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 2) return 1;
    if (strcmp(argv[1], "unicode") == 0) {
        /* Linux truncates comm to 15 bytes, splitting this final character. */
        if (prctl(PR_SET_NAME, "12345678901234\xc3\xa9", 0, 0, 0) != 0) return 1;
    } else if (strcmp(argv[1], "threads") == 0) {
        pthread_t thread;
        if (pthread_create(&thread, NULL, worker, NULL) != 0) return 1;
    } else {
        return 1;
    }
    puts("ready");
    fflush(stdout);
    if (strcmp(argv[1], "threads") == 0) {
        /* The test chooses whether the leader exits before or after capture. */
        char command;
        if (read(STDIN_FILENO, &command, 1) != 1) return 1;
        pthread_exit(NULL);
    }
    for (;;) pause();
    return 0;
}
