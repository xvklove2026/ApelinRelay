// chatprobe.c — one streaming chat/completions request to the local relay,
// printing when each chunk arrives so incremental delivery is visible.
//   chatprobe ["prompt"] [turns]
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

static char g_body[1 << 20];
static char g_acc[1 << 21];

int main(int argc, char **argv) {
    const char *prompt = argc > 1 ? argv[1] : "Count from 1 to 8, one number per line.";
    int turns = argc > 2 ? atoi(argv[2]) : 0;

    size_t off = 0;
    off += (size_t)snprintf(g_body + off, sizeof(g_body) - off,
        "{\"model\":\"gpt-6.1-sol\",\"max_tokens\":512,\"stream\":true,\"messages\":[");
    for (int i = 0; i < turns; i++) {
        off += (size_t)snprintf(g_body + off, sizeof(g_body) - off,
            "%s{\"role\":\"user\",\"content\":\"Historical question %d\"},"
            "{\"role\":\"assistant\",\"content\":\"Historical answer %d with enough words to pad.\"}",
            i ? "," : "", i, i);
    }
    off += (size_t)snprintf(g_body + off, sizeof(g_body) - off,
        "%s{\"role\":\"user\",\"content\":\"%s\"}]}", turns ? "," : "", prompt);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a; memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET; a.sin_port = htons(18791);
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0) { perror("connect"); return 1; }

    static char req[1 << 21];
    int n = snprintf(req, sizeof(req),
        "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1:18791\r\n"
        "Content-Type: application/json\r\nAccept: text/event-stream\r\n"
        "Connection: close\r\nContent-Length: %lu\r\n\r\n%s",
        (unsigned long)strlen(g_body), g_body);
    double t0 = now_ms();
    if (write(fd, req, (size_t)n) < 0) { perror("write"); return 1; }

    size_t accLen = 0;
    double tHeader = -1, tFirstData = -1, tFirstContent = -1;
    int dataFrames = 0, contentFrames = 0, sawDone = 0;
    char buf[16384]; ssize_t r;
    while ((r = read(fd, buf, sizeof(buf))) > 0) {
        double t = now_ms() - t0;
        if (tHeader < 0) tHeader = t;
        if (accLen + (size_t)r < sizeof(g_acc)) { memcpy(g_acc + accLen, buf, (size_t)r); accLen += (size_t)r; }
        g_acc[accLen] = 0;
        // count complete "data: " lines seen so far
        int prevData = dataFrames;
        dataFrames = 0; contentFrames = 0; sawDone = 0;
        for (char *p = g_acc; (p = strstr(p, "\ndata: ")); ) {
            p += 7;
            dataFrames++;
            if (!strncmp(p, "[DONE]", 6)) sawDone = 1;
            if (strstr(p, "\"content\":\"") && strncmp(p, "[DONE]", 6)) contentFrames++;
        }
        if (!dataFrames && !strncmp(g_acc, "data: ", 6)) dataFrames = 1;
        if (dataFrames > prevData && tFirstData < 0) tFirstData = t;
        if (contentFrames > 0 && tFirstContent < 0) tFirstContent = t;
    }
    close(fd);

    // crude chunk-line extraction from the accumulated body
    printf("first-byte=%6.0fms  first-data-frame=%6.0fms  first-content=%6.0fms  total=%6.0fms\n",
           tHeader, tFirstData, tFirstContent, now_ms() - t0);
    printf("data frames=%d  content frames=%d  done=%d  bytes=%zu  turns=%d\n",
           dataFrames, contentFrames, sawDone, accLen, turns);
    char *body = strstr(g_acc, "\r\n\r\n");
    if (body) {
        body += 4;
        size_t bl = strlen(body);
        if (bl > 700) bl = 700;
        printf("--- first 700 bytes of body ---\n%.*s\n", (int)bl, body);
    }
    return dataFrames > 1 && contentFrames > 0 ? 0 : 1;
}