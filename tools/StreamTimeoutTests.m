// SPDX-License-Identifier: MIT
// Standalone regression tests, using the actual relay implementation and a
// loopback fixture. No upstream credentials, real preferences or UI hooks.
// Build/run: sh tools/run-stream-tests.sh
#define APELIN_RELAY_TESTING 1
#ifndef APELIN_RELAY_TEST_TIMEOUT_SCALE
#define APELIN_RELAY_TEST_TIMEOUT_SCALE 0.05
#endif
#define APELIN_PACKAGE_ID "local.apelin.relay-regression"
#import "../Sources/Tweak.m"
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>

static NSUInteger failures;
static void Check(BOOL value, NSString *message) {
    printf("%s %s\n", value ? "PASS" : "FAIL", message.UTF8String);
    fflush(stdout);
    if (!value) failures++;
}
static NSData *Frame(NSString *name, NSDictionary *payload) {
    NSData *json = [NSJSONSerialization dataWithJSONObject:payload options:0 error:NULL];
    NSString *str = [NSString stringWithFormat:@"event: %@\ndata: %@\n\n", name,
        [[NSString alloc] initWithData:json encoding:NSUTF8StringEncoding]];
    return [str dataUsingEncoding:NSUTF8StringEncoding];
}
static BOOL WriteData(int fd, NSData *data) {
    const uint8_t *p = data.bytes;
    NSUInteger off = 0;
    while (off < data.length) {
        ssize_t n = send(fd, p + off, data.length - off, 0);
        if (n > 0) off += (NSUInteger)n;
        else if (n < 0 && errno == EINTR) continue;
        else return NO;
    }
    return YES;
}
static BOOL WriteString(int fd, NSString *str) {
    return WriteData(fd, [str dataUsingEncoding:NSUTF8StringEncoding]);
}
static void CompleteText(int fd, NSString *text, BOOL split) {
    NSData *delta = Frame(@"response.output_text.delta", @{@"delta": text});
    if (split) {
        // Split inside the event name, not just at HTTP chunk boundaries.
        NSUInteger n = MIN((NSUInteger)13, delta.length);
        WriteData(fd, [delta subdataWithRange:NSMakeRange(0, n)]);
        usleep(20000);
        WriteData(fd, [delta subdataWithRange:NSMakeRange(n, delta.length - n)]);
    } else WriteData(fd, delta);
    WriteData(fd, Frame(@"response.content_part.done", @{}));
    WriteData(fd, Frame(@"response.completed", @{@"response": @{@"status": @"completed"}}));
}

@interface RelayFixture : NSObject
@property(nonatomic) int listener;
@property(nonatomic) uint16_t port;
@property(nonatomic) BOOL stopping;
@property(nonatomic, copy) NSString *scenario;
@property(nonatomic, strong) NSMutableArray<NSDictionary *> *requests;
@property(nonatomic, strong) dispatch_group_t workers;
- (instancetype)initWithScenario:(NSString *)scenario;
- (void)stop;
@end
@implementation RelayFixture
- (instancetype)initWithScenario:(NSString *)scenario {
    self = [super init];
    if (!self) return nil;
    _scenario = [scenario copy];
    _requests = [NSMutableArray array];
    _workers = dispatch_group_create();
    _listener = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(_listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (_listener < 0 || bind(_listener, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(_listener, 32) != 0) { perror("fixture listen"); exit(2); }
    socklen_t len = sizeof(addr);
    if (getsockname(_listener, (struct sockaddr *)&addr, &len) != 0) exit(2);
    _port = ntohs(addr.sin_port);
    dispatch_group_async(_workers, dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
        for (;;) {
            int conn = accept(self.listener, NULL, NULL);
            if (conn < 0) break;
            BOOL stop;
            @synchronized(self) { stop = self.stopping; }
            if (stop) { close(conn); break; }
            dispatch_group_async(self.workers, dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
                @autoreleasepool {
                    int yes = 1;
                    struct timeval timeout = {6, 0};
                    setsockopt(conn, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes));
                    setsockopt(conn, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
                    setsockopt(conn, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
                    [self handle:conn];
                    close(conn);
                }
            });
        }
    });
    return self;
}
- (void)handle:(int)fd {
    NSData *request = RelayReadRequest(fd);
    NSData *end = [@"\r\n\r\n" dataUsingEncoding:NSUTF8StringEncoding];
    NSRange r = [request rangeOfData:end options:0 range:NSMakeRange(0, request.length)];
    if (!request.length || r.location == NSNotFound) return;
    NSData *body = [request subdataWithRange:NSMakeRange(r.location + r.length,
        request.length - r.location - r.length)];
    NSDictionary *json = [NSJSONSerialization JSONObjectWithData:body options:0 error:NULL];
    NSUInteger index;
    @synchronized(self) { [self.requests addObject:json ?: @{}]; index = self.requests.count; }
    NSString *s = self.scenario;
    if ([s isEqual:@"header-stall"] && index == 1) {
        usleep((useconds_t)((RelayStreamLimitsForAttempt(0).headers + 0.2) * 1000000.0));
        return;
    }
    if ([s isEqual:@"http-error"]) {
        NSString *b = @"{\"error\":{\"message\":\"fixture unavailable\"}}";
        WriteString(fd, [NSString stringWithFormat:@"HTTP/1.1 503 Service Unavailable\r\nContent-Length: %lu\r\nConnection: close\r\n\r\n%@",
            (unsigned long)[b lengthOfBytesUsingEncoding:NSUTF8StringEncoding], b]);
        return;
    }
    if ([s isEqual:@"json"]) {
        NSData *b = [NSJSONSerialization dataWithJSONObject:@{@"output": @[
            @{@"type": @"message", @"content": @[@{@"type": @"output_text", @"text": @"JSON_OK"}]}]}
            options:0 error:NULL];
        WriteString(fd, [NSString stringWithFormat:@"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %lu\r\nConnection: close\r\n\r\n", (unsigned long)b.length]);
        WriteData(fd, b); return;
    }
    WriteString(fd, @"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n");
    WriteData(fd, Frame(@"response.created", @{@"response": @{@"id": @"fixture"}}));
    if (([s isEqual:@"body-stall"] || [s isEqual:@"heartbeat"]) && index == 1) {
        NSTimeInterval until = NSProcessInfo.processInfo.systemUptime +
            RelayStreamLimitsForAttempt(0).firstContent + 0.5;
        while (NSProcessInfo.processInfo.systemUptime < until) {
            if ([s isEqual:@"heartbeat"]) WriteString(fd, @": heartbeat\n\n");
            usleep(50000);
        }
        CompleteText(fd, @"STALE_MUST_NOT_APPEAR", NO); return;
    }
    if ([s isEqual:@"both-stall"]) { usleep(3100000); return; }
    if ([s isEqual:@"retry-grace"] && index == 1) {
        usleep((useconds_t)((RelayStreamLimitsForAttempt(0).firstContent + 0.2) * 1000000.0));
        return;
    }
    if ([s isEqual:@"retry-grace"] && index == 2) usleep(1300000);
    if ([s isEqual:@"late-normal"]) usleep(650000);
    if ([s isEqual:@"partial-idle"] || [s isEqual:@"missing-end"]) {
        WriteData(fd, Frame(@"response.output_text.delta", @{@"delta": @"PREFIX"}));
        if ([s isEqual:@"partial-idle"]) usleep(3400000);
        return;
    }
    if ([s isEqual:@"tool-idle"] || [s isEqual:@"tool"]) {
        WriteData(fd, Frame(@"response.output_item.added", @{@"item": @{
            @"type": @"function_call", @"call_id": @"call_fixture", @"name": @"shell"}}));
        if ([s isEqual:@"tool-idle"]) { usleep(3400000); return; }
        WriteData(fd, Frame(@"response.function_call_arguments.delta", @{@"delta": @"{\"command\":\"true\"}"}));
        WriteData(fd, Frame(@"response.output_item.done", @{}));
        WriteData(fd, Frame(@"response.completed", @{})); return;
    }
    if ([s isEqual:@"event-error"]) {
        WriteData(fd, Frame(@"response.failed", @{@"response": @{
            @"error": @{@"message": @"fixture failed"}}})); return;
    }
    if ([s isEqual:@"notice"] && index == 1) {
        CompleteText(fd, @"当前会话模型已回复100轮，请新建会话。", YES); return;
    }
    if ([s isEqual:@"parallel"]) {
        CompleteText(fd, json[@"model"] ?: @"missing-model", YES);
        return;
    }
    CompleteText(fd, @"OK", [s isEqual:@"split"]);
    if ([s isEqual:@"completed-open"]) usleep(3400000);
}
- (void)stop {
    @synchronized(self) { self.stopping = YES; }
    shutdown(self.listener, SHUT_RDWR);
    close(self.listener);
    self.listener = -1;
    dispatch_group_wait(self.workers, dispatch_time(DISPATCH_TIME_NOW, 7 * NSEC_PER_SEC));
}
@end

static NSUInteger Count(NSString *str, NSString *needle) {
    NSUInteger n = 0, pos = 0;
    while (pos < str.length) {
        NSRange r = [str rangeOfString:needle options:0 range:NSMakeRange(pos, str.length - pos)];
        if (r.location == NSNotFound) break;
        n++; pos = NSMaxRange(r);
    }
    return n;
}
static NSDictionary *Run(NSString *scenario) {
    RelayFixture *fixture = [[RelayFixture alloc] initWithScenario:scenario];
    kUpstreamURL = [NSString stringWithFormat:@"http://127.0.0.1:%u/v1/responses", fixture.port];
    int pair[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0) exit(2);
    NSTimeInterval budget = RelayStreamLimitsForAttempt(0).firstContent +
        RelayStreamLimitsForAttempt(1).firstContent + 10.0;
    struct timeval timeout = {(time_t)MAX(8.0, budget), 0};
    int yes = 1;
    for (int i = 0; i < 2; i++) {
        setsockopt(pair[i], SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes));
        setsockopt(pair[i], SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(pair[i], SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    }
    NSMutableArray *messages = [NSMutableArray array];
    for (int i = 0; i < 23; i++) {
        [messages addObject:@{@"role": @"user", @"content": @"fixture prompt"}];
        [messages addObject:@{@"role": @"assistant", @"content": @"fixture answer"}];
    }
    [messages addObject:@{@"role": @"user", @"content": @"Reply OK"}];
    NSMutableData *reply = [NSMutableData data];
    int readFD = pair[1];
    dispatch_group_t reader = dispatch_group_create();
    dispatch_group_async(reader, dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
        uint8_t bytes[4096]; ssize_t n;
        while ((n = recv(readFD, bytes, sizeof(bytes), 0)) > 0) {
            if (reply.length + (NSUInteger)n > 1024 * 1024) break;
            [reply appendBytes:bytes length:(NSUInteger)n];
        }
    });
    NSTimeInterval start = NSProcessInfo.processInfo.systemUptime;
    RelayHandleAnthropicStream(pair[0], @"TOKEN", @"fixture-model", messages,
        @"fixture system", @128, nil, nil, 16, 6);
    NSTimeInterval elapsed = NSProcessInfo.processInfo.systemUptime - start;
    shutdown(pair[0], SHUT_WR);
    dispatch_group_wait(reader, dispatch_time(DISPATCH_TIME_NOW, 9 * NSEC_PER_SEC));
    close(pair[0]); close(pair[1]);
    [fixture stop];
    NSString *text = [[NSString alloc] initWithData:reply encoding:NSUTF8StringEncoding] ?: @"";
    NSArray *requests;
    @synchronized(fixture) { requests = [fixture.requests copy]; }
    BOOL retry = [@[@"header-stall", @"body-stall", @"heartbeat", @"both-stall", @"retry-grace", @"notice"] containsObject:scenario];
    BOOL error = [@[@"both-stall", @"partial-idle", @"tool-idle", @"missing-end", @"http-error", @"event-error"] containsObject:scenario];
    Check(requests.count == (retry ? 2U : 1U), [scenario stringByAppendingString:@": correct attempt count"]);
    Check(Count(text, @"event: error\n") == (error ? 1U : 0U), [scenario stringByAppendingString:@": correct error outcome"]);
    Check(Count(text, @"event: message_start\n") == 1 && Count(text, @"event: message_stop\n") == 1,
        [scenario stringByAppendingString:@": one start and one stop"]);
    Check(![text containsString:@"STALE_MUST_NOT_APPEAR"], [scenario stringByAppendingString:@": no cancelled-attempt output"]);
    Check(Count(text, @"event: content_block_start\n") == Count(text, @"event: content_block_stop\n"),
        [scenario stringByAppendingString:@": balanced content blocks"]);
    if (!error && ![scenario isEqual:@"tool"] && ![scenario isEqual:@"json"])
        Check(Count(text, @"\"text\":\"OK\"") == 1, [scenario stringByAppendingString:@": exactly one answer"]);
    if ([scenario isEqual:@"partial-idle"] || [scenario isEqual:@"missing-end"])
        Check(Count(text, @"PREFIX") == 1, [scenario stringByAppendingString:@": partial output not replayed"]);
    if (retry && ![scenario isEqual:@"notice"]) Check([requests[0] isEqual:requests[1]],
        [scenario stringByAppendingString:@": retry preserves complete request"]);
    if ([scenario isEqual:@"notice"]) {
        Check(![text containsString:@"100轮"] && [requests[1][@"input"] count] < [requests[0][@"input"] count],
            @"notice: suppressed, then smaller rescue window");
    }
    if ([scenario isEqual:@"body-stall"] || [scenario isEqual:@"heartbeat"])
        Check(elapsed >= RelayStreamLimitsForAttempt(0).firstContent - 0.05 &&
              elapsed < RelayStreamLimitsForAttempt(0).firstContent + 1.0,
              [scenario stringByAppendingString:@": first-content deadline really fires"]);
    if ([scenario isEqual:@"completed-open"])
        Check(elapsed < 1.0, @"completed-open: finishes without waiting for EOF");
    if ([scenario isEqual:@"tool"]) Check([text containsString:@"input_json_delta"] && [text containsString:@"tool_use"],
        @"tool: arguments preserved");
    if ([scenario isEqual:@"json"]) Check([text containsString:@"JSON_OK"], @"json: fallback preserved");
    return @{@"scenario": scenario, @"attempts": @(requests.count), @"seconds": @(elapsed)};
}

static void RunParallel(void) {
    RelayFixture *fixture = [[RelayFixture alloc] initWithScenario:@"parallel"];
    kUpstreamURL = [NSString stringWithFormat:@"http://127.0.0.1:%u/v1/responses", fixture.port];
    dispatch_group_t group = dispatch_group_create();
    NSMutableArray *outcomes = [NSMutableArray array];
    for (NSUInteger i = 0; i < 8; i++) {
        dispatch_group_async(group, dispatch_get_global_queue(QOS_CLASS_USER_INITIATED, 0), ^{
            @autoreleasepool {
                NSString *model = [NSString stringWithFormat:@"concurrent-%lu", (unsigned long)i];
                NSMutableString *text = [NSMutableString string];
                BOOL stalled = NO;
                NSString *error = nil;
                BOOL ok = RelayStreamUpstream(@"TOKEN", @{@"model": model, @"input": @[]},
                    RelaySharedStreamSession(), [RelayStreamHub shared], 0,
                    ^BOOL(NSString *name, NSDictionary *payload) {
                        if ([name isEqual:@"response.output_text.delta"]) [text appendString:payload[@"delta"]];
                        return YES;
                    }, NULL, &error, &stalled);
                @synchronized(outcomes) { [outcomes addObject:@(ok && !stalled && !error && [text isEqual:model])]; }
            }
        });
    }
    BOOL finished = dispatch_group_wait(group, dispatch_time(DISPATCH_TIME_NOW, 8 * NSEC_PER_SEC)) == 0;
    [fixture stop];
    BOOL allOK = finished && outcomes.count == 8 && fixture.requests.count == 8;
    for (NSNumber *n in outcomes) allOK = allOK && n.boolValue;
    Check(allOK, @"parallel: eight requests receive only their own response");
    @synchronized([RelayStreamHub shared]) {
        Check([RelayStreamHub shared].sinks.count == 0, @"parallel: no retained receiver after completion");
    }
}

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        NSArray *scenarios = argc > 1 ? @[[NSString stringWithUTF8String:argv[1]]] :
            @[@"normal", @"split", @"header-stall", @"body-stall", @"heartbeat",
              @"retry-grace", @"both-stall", @"late-normal", @"partial-idle", @"tool", @"tool-idle",
              @"missing-end", @"http-error", @"event-error", @"json", @"notice", @"completed-open"];
        for (NSString *scenario in scenarios) {
            NSDictionary *result = Run(scenario);
            printf("CASE %s attempts=%lu elapsed=%.3fs\n", scenario.UTF8String,
                (unsigned long)[result[@"attempts"] unsignedIntegerValue], [result[@"seconds"] doubleValue]);
        }
        if (argc == 1) RunParallel();
        printf("RESULT failures=%lu\n", (unsigned long)failures);
    }
    return failures ? 1 : 0;
}
