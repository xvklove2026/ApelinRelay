// SPDX-License-Identifier: MIT
// Build from tools/: clang -arch arm64 -fobjc-arc -Wall -Wextra
//   -Wno-unused-parameter -framework Foundation LatencyProbe.m -o .latency-probe
// Sign: ldid -STool.entitlements .latency-probe
// Run: API_KEY=TOKEN ./.latency-probe [pair|direct|relay|limit]
// pair runs concurrently; limit scans 18/20/21/24/30 user turns serially.
// Optional: UPSTREAM_URL, RELAY_URL, MODEL, TURNS (1..96).
// No response bodies or keys are logged.
#import <Foundation/Foundation.h>
#include <stdio.h>
#include <string.h>

static NSString *const kRelayFallbackKey =
    @"sk-46e396dad8763b5a6e460c7b1f3b9ebc02e8c8de6a930f53eaa0ca433ad4a481";

static double ProbeNow(void) { return NSProcessInfo.processInfo.systemUptime; }
static id ProbeTime(double value) { return value >= 0 ? @(value) : (id)NSNull.null; }

@interface ARLatencyProbe : NSObject <NSURLSessionDataDelegate>
@property(nonatomic, copy) NSString *label;
@property(nonatomic, strong) NSMutableData *buffer;
@property(nonatomic, strong) NSURLSession *session;
@property(nonatomic, strong) NSURLSessionDataTask *task;
@property(nonatomic, copy) void (^onFinish)(NSDictionary *);
@property(nonatomic) double start;
@property(nonatomic) double headers;
@property(nonatomic) double firstEvent;
@property(nonatomic) double firstContent;
@property(nonatomic) double terminal;
@property(nonatomic) NSInteger status;
@property(nonatomic) NSInteger requestedTurns;
@property(nonatomic) NSUInteger events;
@property(nonatomic) NSUInteger bytes;
@property(nonatomic) BOOL sawOK;
@property(nonatomic) BOOL failedEvent;
@property(nonatomic, strong) NSMutableString *text;
@property(nonatomic, strong) NSMutableDictionary *metrics;
- (void)startRequest:(NSURLRequest *)request label:(NSString *)label;
@end

@implementation ARLatencyProbe
- (void)startRequest:(NSURLRequest *)request label:(NSString *)label {
    self.label = label;
    self.buffer = [NSMutableData data];
    self.text = [NSMutableString string];
    self.metrics = [NSMutableDictionary dictionary];
    self.headers = self.firstEvent = self.firstContent = self.terminal = -1;
    NSURLSessionConfiguration *config = NSURLSessionConfiguration.ephemeralSessionConfiguration;
    config.timeoutIntervalForRequest = 80;
    config.timeoutIntervalForResource = 80;
    NSOperationQueue *queue = [[NSOperationQueue alloc] init];
    queue.maxConcurrentOperationCount = 1;
    self.session = [NSURLSession sessionWithConfiguration:config delegate:self delegateQueue:queue];
    self.start = ProbeNow();
    self.task = [self.session dataTaskWithRequest:request];
    [self.task resume];
}
- (void)URLSession:(NSURLSession *)session dataTask:(NSURLSessionDataTask *)dataTask
 didReceiveResponse:(NSURLResponse *)response
 completionHandler:(void (^)(NSURLSessionResponseDisposition))completionHandler {
    self.headers = ProbeNow() - self.start;
    if ([response isKindOfClass:NSHTTPURLResponse.class]) {
        self.status = ((NSHTTPURLResponse *)response).statusCode;
    }
    completionHandler(NSURLSessionResponseAllow);
}
- (void)consumeBlock:(NSData *)data {
    NSString *block = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
    if (!block) return;
    NSMutableArray *lines = [NSMutableArray array];
    NSString *name = @"";
    for (NSString *raw in [block componentsSeparatedByString:@"\n"]) {
        NSString *line = [raw hasSuffix:@"\r"] ? [raw substringToIndex:raw.length - 1] : raw;
        if ([line hasPrefix:@"event:"]) name = [[line substringFromIndex:6]
            stringByTrimmingCharactersInSet:NSCharacterSet.whitespaceCharacterSet];
        if ([line hasPrefix:@"data:"]) [lines addObject:[line substringFromIndex:5]];
    }
    if (!lines.count) return;
    NSString *json = [[lines componentsJoinedByString:@"\n"]
        stringByTrimmingCharactersInSet:NSCharacterSet.whitespaceAndNewlineCharacterSet];
    if ([json isEqualToString:@"[DONE]"]) return;
    id obj = [NSJSONSerialization JSONObjectWithData:[json dataUsingEncoding:NSUTF8StringEncoding]
        options:0 error:NULL];
    if (![obj isKindOfClass:NSDictionary.class]) return;
    NSDictionary *event = obj;
    if (!name.length && [event[@"type"] isKindOfClass:NSString.class]) name = event[@"type"];
    double elapsed = ProbeNow() - self.start;
    if (self.firstEvent < 0) self.firstEvent = elapsed;
    self.events++;
    id delta = event[@"delta"];
    NSString *text = nil;
    BOOL content = NO;
    if ([name isEqualToString:@"response.output_text.delta"] && [delta isKindOfClass:NSString.class]) {
        text = delta;
        content = text.length > 0;
    } else if ([name isEqualToString:@"response.function_call_arguments.delta"]) {
        content = [delta isKindOfClass:NSString.class] && [delta length] > 0;
    } else if ([name isEqualToString:@"content_block_delta"] && [delta isKindOfClass:NSDictionary.class]) {
        if ([delta[@"type"] isEqual:@"text_delta"] && [delta[@"text"] isKindOfClass:NSString.class]) {
            text = delta[@"text"];
            content = text.length > 0;
        } else if ([delta[@"type"] isEqual:@"input_json_delta"]) {
            content = [delta[@"partial_json"] isKindOfClass:NSString.class] && [delta[@"partial_json"] length] > 0;
        }
    }
    if (content && self.firstContent < 0) self.firstContent = elapsed;
    if (text.length && self.text.length < 512)
        [self.text appendString:[text substringToIndex:MIN(text.length, 512 - self.text.length)]];
    if ([self.text rangeOfString:@"OK" options:NSCaseInsensitiveSearch].location != NSNotFound) self.sawOK = YES;
    if ([name isEqualToString:@"error"] || [name isEqualToString:@"response.failed"] ||
        [name isEqualToString:@"response.incomplete"]) self.failedEvent = YES;
    if (self.terminal < 0 && ([name isEqualToString:@"response.completed"] ||
        [name isEqualToString:@"message_stop"] || [name isEqualToString:@"response.failed"] ||
        [name isEqualToString:@"response.incomplete"])) self.terminal = elapsed;
}
- (void)URLSession:(NSURLSession *)session dataTask:(NSURLSessionDataTask *)dataTask
 didReceiveData:(NSData *)data {
    self.bytes += data.length;
    if (self.bytes > 2 * 1024 * 1024) { [dataTask cancel]; return; }
    [self.buffer appendData:data];
    NSData *lf = [@"\n\n" dataUsingEncoding:NSUTF8StringEncoding];
    NSData *crlf = [@"\r\n\r\n" dataUsingEncoding:NSUTF8StringEncoding];
    for (;;) {
        NSRange a = [self.buffer rangeOfData:lf options:0 range:NSMakeRange(0, self.buffer.length)];
        NSRange b = [self.buffer rangeOfData:crlf options:0 range:NSMakeRange(0, self.buffer.length)];
        NSRange r = a.location <= b.location ? a : b;
        if (r.location == NSNotFound) break;
        NSData *block = [self.buffer subdataWithRange:NSMakeRange(0, r.location)];
        [self.buffer replaceBytesInRange:NSMakeRange(0, r.location + r.length) withBytes:NULL length:0];
        [self consumeBlock:block];
    }
}
- (void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task
 didFinishCollectingMetrics:(NSURLSessionTaskMetrics *)metrics {
    NSURLSessionTaskTransactionMetrics *tx = metrics.transactionMetrics.lastObject;
    if (!tx) return;
    if (tx.domainLookupStartDate && tx.domainLookupEndDate)
        self.metrics[@"dns_s"] = @([tx.domainLookupEndDate timeIntervalSinceDate:tx.domainLookupStartDate]);
    if (tx.connectStartDate && tx.connectEndDate)
        self.metrics[@"connect_s"] = @([tx.connectEndDate timeIntervalSinceDate:tx.connectStartDate]);
    if (tx.secureConnectionStartDate && tx.secureConnectionEndDate)
        self.metrics[@"tls_s"] = @([tx.secureConnectionEndDate timeIntervalSinceDate:tx.secureConnectionStartDate]);
    if (tx.requestEndDate && tx.responseStartDate)
        self.metrics[@"after_upload_to_headers_s"] = @([tx.responseStartDate timeIntervalSinceDate:tx.requestEndDate]);
    self.metrics[@"connection_reused"] = @(tx.reusedConnection);
    self.metrics[@"protocol"] = tx.networkProtocolName ?: @"unknown";
}
- (void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task
 didCompleteWithError:(NSError *)error {
    if (self.buffer.length) [self consumeBlock:self.buffer];
    double elapsed = ProbeNow() - self.start;
    NSString *text = self.text.lowercaseString;
    BOOL turnLimit = NO;
    for (NSString *notice in @[@"满 20 轮", @"满20轮", @"达到上限", @"已回复 105 轮",
                              @"reached 20 turns", @"reached 105", @"model turns", @"/compact", @"/new"]) {
        if ([text containsString:notice]) { turnLimit = YES; break; }
    }
    NSString *outcome = error ? @"network_error" : self.status != 200 ? @"http_error" :
        turnLimit ? @"turn_limit" : self.failedEvent ? @"stream_error" :
        self.terminal < 0 ? @"missing_terminal" : !self.sawOK ? @"unexpected_output" : @"accepted";
    NSDictionary *result = @{
        @"path": self.label, @"requested_turns": @(self.requestedTurns), @"http_status": @(self.status),
        @"headers_s": ProbeTime(self.headers), @"first_event_s": ProbeTime(self.firstEvent),
        @"first_content_s": ProbeTime(self.firstContent), @"terminal_s": ProbeTime(self.terminal),
        @"total_s": @(elapsed), @"tail_s": ProbeTime(self.terminal < 0 ? -1 : elapsed - self.terminal),
        @"events": @(self.events), @"bytes": @(self.bytes), @"contains_OK": @(self.sawOK),
        @"turn_limit_notice": @(turnLimit), @"accepted": @([outcome isEqualToString:@"accepted"]),
        @"outcome": outcome, @"error_event": @(self.failedEvent),
        @"network_error_code": @(error ? error.code : 0), @"network": self.metrics,
    };
    if (self.onFinish) self.onFinish(result);
    self.onFinish = nil;
    [session finishTasksAndInvalidate];
}
@end

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        NSString *mode = argc > 1 ? [NSString stringWithUTF8String:argv[1]] : @"pair";
        if (![@[@"pair", @"direct", @"relay", @"limit"] containsObject:mode]) {
            fputs("usage: .latency-probe [pair|direct|relay|limit]\n", stderr); return 2;
        }
        NSDictionary *env = NSProcessInfo.processInfo.environment;
        NSString *key = env[@"API_KEY"];
        if (!key.length || [key isEqualToString:@"TOKEN"]) key = kRelayFallbackKey;
        BOOL limitMode = [mode isEqualToString:@"limit"];
        BOOL includeDirect = ![mode isEqualToString:@"relay"];
        NSString *model = env[@"MODEL"] ?: @"gpt-6.1-sol";
        NSMutableArray *specs = [NSMutableArray array];
        if (limitMode) {
            NSArray *counts = @[@18, @20, @21, @24, @30];
            for (NSNumber *count in counts) {
                NSUInteger turns = count.unsignedIntegerValue;
                NSMutableArray *input = [NSMutableArray array];
                for (NSUInteger i = 0; i + 1 < turns; i++) {
                    [input addObject:@{@"role": @"user", @"content": [NSString stringWithFormat:@"Historical request %lu", (unsigned long)(i + 1)]}];
                    [input addObject:@{@"role": @"assistant", @"content": [NSString stringWithFormat:@"Historical answer %lu", (unsigned long)(i + 1)]}];
                }
                [input addObject:@{@"role": @"user", @"content": @"Reply with only OK."}];
                NSDictionary *body = @{@"model": model, @"max_output_tokens": @64,
                    @"stream": @YES, @"input": input};
                [specs addObject:@{@"label": @"direct-limit", @"turns": count, @"body": body,
                    @"url": env[@"UPSTREAM_URL"] ?: @"https://api.xingchiapi.com/v1/responses"}];
            }
        } else {
            NSInteger turns = env[@"TURNS"] ? [env[@"TURNS"] integerValue] : 1;
            if (turns < 1 || turns > 96) {
                fputs("TURNS must be between 1 and 96.\n", stderr); return 2;
            }
            NSMutableArray *messages = [NSMutableArray array];
            for (NSInteger i = 1; i < turns; i++) {
                [messages addObject:@{@"role": @"user", @"content":
                    [NSString stringWithFormat:@"Historical request %ld", (long)i]}];
                [messages addObject:@{@"role": @"assistant", @"content":
                    [NSString stringWithFormat:@"Historical answer %ld", (long)i]}];
            }
            [messages addObject:@{@"role": @"user", @"content": @"Reply with only OK."}];
            NSDictionary *direct = @{@"model": model, @"max_output_tokens": @64,
                @"stream": @YES, @"input": messages};
            NSDictionary *relay = @{@"model": model, @"max_tokens": @64,
                @"stream": @YES, @"messages": messages};
            if (includeDirect) [specs addObject:@{@"label": @"direct", @"body": direct, @"turns": @(turns),
                @"url": env[@"UPSTREAM_URL"] ?: @"https://api.xingchiapi.com/v1/responses"}];
            if (![mode isEqualToString:@"direct"]) [specs addObject:@{@"label": @"relay", @"body": relay, @"turns": @(turns),
                @"url": env[@"RELAY_URL"] ?: @"http://127.0.0.1:18791/v1/messages"}];
        }
        for (NSDictionary *spec in specs) {
            NSURL *url = [NSURL URLWithString:spec[@"url"]];
            if (!url.host || ![@[@"https", @"http"] containsObject:url.scheme] || url.user || url.password) {
                fputs("Invalid endpoint URL.\n", stderr); return 2;
            }
        }
        dispatch_group_t group = dispatch_group_create();
        NSMutableArray *probes = [NSMutableArray array];
        NSMutableArray *results = [NSMutableArray array];
        for (NSDictionary *spec in specs) {
            NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:[NSURL URLWithString:spec[@"url"]]];
            request.HTTPMethod = @"POST";
            request.timeoutInterval = 80;
            [request setValue:@"application/json" forHTTPHeaderField:@"Content-Type"];
            [request setValue:@"Minis/1.0" forHTTPHeaderField:@"User-Agent"];
            if (key.length) [request setValue:[@"Bearer " stringByAppendingString:key] forHTTPHeaderField:@"Authorization"];
            request.HTTPBody = [NSJSONSerialization dataWithJSONObject:spec[@"body"] options:0 error:NULL];
            ARLatencyProbe *probe = [[ARLatencyProbe alloc] init];
            probe.requestedTurns = spec[@"turns"] ? [spec[@"turns"] integerValue] : 1;
            dispatch_group_enter(group);
            probe.onFinish = ^(NSDictionary *item) {
                @synchronized(results) {
                    [results addObject:item];
                    NSData *json = [NSJSONSerialization dataWithJSONObject:item options:NSJSONWritingSortedKeys error:NULL];
                    if (json) { fwrite(json.bytes, 1, json.length, stdout); fputc('\n', stdout); fflush(stdout); }
                }
                dispatch_group_leave(group);
            };
            [probes addObject:probe];
            [probe startRequest:request label:spec[@"label"]];
            if (limitMode && dispatch_group_wait(group, dispatch_time(DISPATCH_TIME_NOW, 84 * NSEC_PER_SEC)) != 0) {
                [probe.task cancel];
                fputs("Probe exceeded the 84-second deadline.\n", stderr); return 1;
            }
        }
        if (!limitMode && dispatch_group_wait(group, dispatch_time(DISPATCH_TIME_NOW, 84 * NSEC_PER_SEC)) != 0) {
            for (ARLatencyProbe *probe in probes) [probe.task cancel];
            fputs("Probe exceeded the 84-second deadline.\n", stderr); return 1;
        }
        for (NSDictionary *result in results) {
            if (![result[@"accepted"] boolValue]) return 1;
        }
        return 0;
    }
}
