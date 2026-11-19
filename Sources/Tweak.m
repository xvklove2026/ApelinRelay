// SPDX-License-Identifier: MIT
//
// ApelinRelay — an in-process translation service inside SpringBoard.
//
// It listens on 127.0.0.1:18791 and speaks the OpenAI chat/completions API,
// forwarding to xingchiapi's /v1/responses (chat/completions is disabled
// upstream) and re-framing the answer as an SSE chat.completion.chunk stream.
//
// Everything here must be defensive: a throw that escapes into SpringBoard
// would take the UI down. Every entry point is wrapped in @try/@catch.

#import <Foundation/Foundation.h>
#import <UIKit/UIKit.h>
#import <os/log.h>
#import <substrate.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

static NSString *const kRelaySubsystem   = @"local.apelin.apelinrelay";
static NSString *const kUpstreamURL      = @"https://api.xingchiapi.com/v1/responses";
static NSString *const kFallbackKey      = @"sk-46e396dad8763b5a6e460c7b1f3b9ebc02e8c8de6a930f53eaa0ca433ad4a481";
static NSString *const kModelsBody       =
    @"{\"object\":\"list\",\"data\":[{\"id\":\"gpt-6.1-sol\",\"object\":\"model\",\"owned_by\":\"xingchiapi\"}]}";

static const int kRelayPort = 18791;

static os_log_t RelayLog(void) {
    static os_log_t log;
    static dispatch_once_t once;
    dispatch_once(&once, ^{
        log = os_log_create("local.apelin.apelinrelay", "relay");
    });
    return log;
}

#pragma mark - Socket helpers

static void RelayWriteAll(int fd, const void *bytes, size_t length) {
    const uint8_t *p = (const uint8_t *)bytes;
    size_t off = 0;
    while (off < length) {
        ssize_t n = send(fd, p + off, length - off, 0);
        if (n > 0) {
            off += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        break;  // peer went away; nothing useful left to do
    }
}

static void RelaySendResponse(int fd, int status, const char *statusText,
                              NSString *contentType, NSData *body) {
    NSString *header = [NSString stringWithFormat:
        @"HTTP/1.1 %d %s\r\n"
         "Content-Type: %@\r\n"
         "Content-Length: %lu\r\n"
         "Connection: close\r\n"
         "\r\n",
        status, statusText, contentType, (unsigned long)body.length];
    NSData *headerData = [header dataUsingEncoding:NSUTF8StringEncoding];
    RelayWriteAll(fd, headerData.bytes, headerData.length);
    if (body.length > 0) {
        RelayWriteAll(fd, body.bytes, body.length);
    }
}

static void RelaySendSSE(int fd, NSString *payload) {
    NSData *data = [payload dataUsingEncoding:NSUTF8StringEncoding];
    RelaySendResponse(fd, 200, "OK", @"text/event-stream", data);
}

/// Reads one full HTTP request (headers + Content-Length body) off the socket.
static NSData *RelayReadRequest(int fd) {
    NSMutableData *data = [NSMutableData data];
    NSData *terminator = [@"\r\n\r\n" dataUsingEncoding:NSUTF8StringEncoding];
    uint8_t buf[4096];

    // 1. Headers.
    while (YES) {
        NSRange r = [data rangeOfData:terminator options:0
                                range:NSMakeRange(0, data.length)];
        if (r.location != NSNotFound) {
            break;
        }
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return data.length > 0 ? data : nil;
        }
        [data appendBytes:buf length:(NSUInteger)n];
        if (data.length > 4 * 1024 * 1024) {
            return nil;
        }
    }

    NSRange hdrEnd = [data rangeOfData:terminator options:0
                                 range:NSMakeRange(0, data.length)];
    NSUInteger bodyStart = hdrEnd.location + hdrEnd.length;

    // 2. Content-Length.
    NSString *head = [[NSString alloc] initWithData:[data subdataWithRange:NSMakeRange(0, hdrEnd.location)]
                                           encoding:NSUTF8StringEncoding];
    long long contentLength = 0;
    for (NSString *line in [head componentsSeparatedByString:@"\r\n"]) {
        NSRange colon = [line rangeOfString:@":"];
        if (colon.location == NSNotFound) {
            continue;
        }
        NSString *key = [[line substringToIndex:colon.location]
                         stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
        if ([[key lowercaseString] isEqualToString:@"content-length"]) {
            NSString *value = [[line substringFromIndex:colon.location + 1]
                               stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
            contentLength = [value longLongValue];
        }
    }
    if (contentLength < 0 || contentLength > 8 * 1024 * 1024) {
        return nil;
    }

    // 3. Body, read until complete.
    NSUInteger total = bodyStart + (NSUInteger)contentLength;
    while (data.length < total) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            break;
        }
        [data appendBytes:buf length:(NSUInteger)n];
        if (data.length > total + 64 * 1024) {
            break;
        }
    }
    return data;
}

#pragma mark - Upstream call

/// Synchronous POST to the Responses API. Returns the decoded JSON, or nil with
/// a human readable reason in *outError.
static id RelayCallUpstream(NSString *apiKey, NSDictionary *requestBody,
                            NSString **outError) {
    NSData *sendBody = [NSJSONSerialization dataWithJSONObject:requestBody options:0 error:NULL];
    if (sendBody == nil) {
        if (outError) *outError = @"could not encode upstream request";
        return nil;
    }

    NSURL *url = [NSURL URLWithString:kUpstreamURL];
    NSMutableURLRequest *req = [NSMutableURLRequest requestWithURL:url];
    req.HTTPMethod = @"POST";
    req.timeoutInterval = 110.0;
    [req setValue:@"application/json" forHTTPHeaderField:@"Content-Type"];
    [req setValue:[NSString stringWithFormat:@"Bearer %@", apiKey]
        forHTTPHeaderField:@"Authorization"];
    [req setValue:@"Minis/1.0" forHTTPHeaderField:@"User-Agent"];
    req.HTTPBody = sendBody;

    __block NSData *responseData = nil;
    __block NSURLResponse *response = nil;
    __block NSError *netError = nil;

    dispatch_semaphore_t sem = dispatch_semaphore_create(0);
    NSURLSessionDataTask *task =
        [[NSURLSession sharedSession] dataTaskWithRequest:req
                                        completionHandler:^(NSData *d, NSURLResponse *r, NSError *e) {
            responseData = d;
            response = r;
            netError = e;
            dispatch_semaphore_signal(sem);
        }];
    [task resume];
    dispatch_semaphore_wait(sem,
        dispatch_time(DISPATCH_TIME_NOW, (int64_t)(115 * NSEC_PER_SEC)));

    if (netError != nil) {
        if (outError) *outError = netError.localizedDescription ?: @"upstream request failed";
        return nil;
    }
    if (responseData == nil) {
        if (outError) *outError = @"upstream returned no data";
        return nil;
    }

    id parsed = [NSJSONSerialization JSONObjectWithData:responseData options:0 error:NULL];
    NSInteger code = [(NSHTTPURLResponse *)response statusCode];

    if (code < 200 || code >= 300) {
        NSString *message = nil;
        if ([parsed isKindOfClass:[NSDictionary class]]) {
            id err = parsed[@"error"];
            if ([err isKindOfClass:[NSDictionary class]]) {
                message = err[@"message"];
            } else if ([err isKindOfClass:[NSString class]]) {
                message = err;
            }
        }
        if (![message isKindOfClass:[NSString class]] || message.length == 0) {
            NSString *raw = [[NSString alloc] initWithData:responseData encoding:NSUTF8StringEncoding];
            message = [NSString stringWithFormat:@"upstream HTTP %ld: %@",
                       (long)code, raw.length > 400 ? [raw substringToIndex:400] : (raw ?: @"")];
        }
        if (outError) *outError = message;
        return nil;
    }
    return parsed;
}

/// Picks the assistant text out of a Responses API payload.
static NSString *RelayTextFromResponse(id parsed) {
    if (![parsed isKindOfClass:[NSDictionary class]]) {
        return @"";
    }
    id output = ((NSDictionary *)parsed)[@"output"];
    if (![output isKindOfClass:[NSArray class]]) {
        return @"";
    }
    NSMutableString *text = [NSMutableString string];
    for (id item in (NSArray *)output) {
        if (![item isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        id type = ((NSDictionary *)item)[@"type"];
        if ([type isKindOfClass:[NSString class]] && ![type isEqualToString:@"message"]) {
            continue;
        }
        id content = ((NSDictionary *)item)[@"content"];
        if (![content isKindOfClass:[NSArray class]]) {
            continue;
        }
        for (id part in (NSArray *)content) {
            if (![part isKindOfClass:[NSDictionary class]]) {
                continue;
            }
            id partType = ((NSDictionary *)part)[@"type"];
            BOOL isText = [partType isKindOfClass:[NSString class]] &&
                          ([partType isEqualToString:@"output_text"] ||
                           [partType isEqualToString:@"text"]);
            if (!isText) {
                continue;
            }
            id t = ((NSDictionary *)part)[@"text"];
            if ([t isKindOfClass:[NSString class]]) {
                [text appendString:(NSString *)t];
            }
        }
    }
    return text;
}

#pragma mark - Request handling

static NSString *RelaySSEChunk(NSString *model, NSString *text) {
    NSDictionary *chunk = @{
        @"id": @"chatcmpl-local",
        @"object": @"chat.completion.chunk",
        @"created": @((long long)[[NSDate date] timeIntervalSince1970]),
        @"model": model ?: @"gpt-6.1-sol",
        @"choices": @[@{
            @"index": @0,
            @"delta": @{@"role": @"assistant", @"content": text ?: @""},
            @"finish_reason": @"stop",
        }],
    };
    NSData *json = [NSJSONSerialization dataWithJSONObject:chunk options:0 error:NULL];
    if (json == nil) {
        return @"data: {\"error\":{\"message\":\"could not encode chunk\"}}\n\ndata: [DONE]\n\n";
    }
    NSString *body = [[NSString alloc] initWithData:json encoding:NSUTF8StringEncoding];
    return [NSString stringWithFormat:@"data: %@\n\ndata: [DONE]\n\n", body];
}

static NSString *RelaySSEError(NSString *message) {
    NSString *safe = message.length > 0 ? message : @"unknown error";
    NSData *json = [NSJSONSerialization dataWithJSONObject:@{@"error": @{@"message": safe}}
                                                   options:0 error:NULL];
    NSString *body = json ? [[NSString alloc] initWithData:json encoding:NSUTF8StringEncoding]
                          : @"{\"error\":{\"message\":\"unknown error\"}}";
    return [NSString stringWithFormat:@"data: %@\n\ndata: [DONE]\n\n", body];
}

static void RelayHandleConnection(int fd) {
    NSData *raw = RelayReadRequest(fd);
    if (raw == nil || raw.length == 0) {
        return;
    }

    NSString *request = [[NSString alloc] initWithData:raw encoding:NSUTF8StringEncoding];
    if (request.length == 0) {
        return;
    }

    NSRange headerSplit = [request rangeOfString:@"\r\n\r\n"];
    NSString *head = headerSplit.location != NSNotFound
        ? [request substringToIndex:headerSplit.location] : request;
    NSString *bodyString = headerSplit.location != NSNotFound
        ? [request substringFromIndex:headerSplit.location + headerSplit.length] : @"";

    NSArray<NSString *> *lines = [head componentsSeparatedByString:@"\r\n"];
    if (lines.count == 0) {
        return;
    }
    NSArray<NSString *> *requestLine = [lines[0] componentsSeparatedByString:@" "];
    if (requestLine.count < 2) {
        return;
    }
    NSString *method = requestLine[0];
    NSString *path = requestLine[1];
    NSRange query = [path rangeOfString:@"?"];
    if (query.location != NSNotFound) {
        path = [path substringToIndex:query.location];
    }

    NSMutableDictionary<NSString *, NSString *> *headers = [NSMutableDictionary dictionary];
    for (NSUInteger i = 1; i < lines.count; i++) {
        NSString *line = lines[i];
        NSRange colon = [line rangeOfString:@":"];
        if (colon.location == NSNotFound) {
            continue;
        }
        NSString *key = [[line substringToIndex:colon.location] lowercaseString];
        NSString *value = [[line substringFromIndex:colon.location + 1]
                           stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
        headers[key] = value;
    }

    os_log(RelayLog(), "relay request %{public}s %{public}s", method.UTF8String, path.UTF8String);

    // GET /v1/models -> the single model Apelin should see.
    if ([path isEqualToString:@"/v1/models"]) {
        NSData *body = [kModelsBody dataUsingEncoding:NSUTF8StringEncoding];
        RelaySendResponse(fd, 200, "OK", @"application/json", body);
        return;
    }

    if (![path isEqualToString:@"/v1/chat/completions"]) {
        NSData *body = [@"{\"error\":{\"message\":\"not found\"}}"
                        dataUsingEncoding:NSUTF8StringEncoding];
        RelaySendResponse(fd, 404, "Not Found", @"application/json", body);
        return;
    }

    // Auth: caller's header if it came with one, otherwise the fallback key.
    NSString *apiKey = kFallbackKey;
    NSString *auth = headers[@"authorization"];
    if (auth.length > 0) {
        NSString *trimmed = [auth stringByTrimmingCharactersInSet:
                             [NSCharacterSet whitespaceCharacterSet]];
        if ([[trimmed lowercaseString] hasPrefix:@"bearer "]) {
            trimmed = [[trimmed substringFromIndex:7]
                       stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceCharacterSet]];
        }
        if (trimmed.length > 0) {
            apiKey = trimmed;
        }
    }

    id parsedBody = nil;
    NSData *bodyData = [bodyString dataUsingEncoding:NSUTF8StringEncoding];
    if (bodyData.length > 0) {
        parsedBody = [NSJSONSerialization JSONObjectWithData:bodyData options:0 error:NULL];
    }
    if (![parsedBody isKindOfClass:[NSDictionary class]]) {
        RelaySendSSE(fd, RelaySSEError(@"request body is not a JSON object"));
        return;
    }

    NSDictionary *incoming = (NSDictionary *)parsedBody;
    NSString *model = [incoming[@"model"] isKindOfClass:[NSString class]]
        ? incoming[@"model"] : @"gpt-6.1-sol";

    // messages -> input[{role, content}]
    NSMutableArray *input = [NSMutableArray array];
    id messages = incoming[@"messages"];
    if ([messages isKindOfClass:[NSArray class]]) {
        for (id message in (NSArray *)messages) {
            if (![message isKindOfClass:[NSDictionary class]]) {
                continue;
            }
            id role = ((NSDictionary *)message)[@"role"];
            id content = ((NSDictionary *)message)[@"content"];
            if (![role isKindOfClass:[NSString class]]) {
                role = @"user";
            }
            if (content == nil) {
                content = @"";
            }
            [input addObject:@{@"role": role, @"content": content}];
        }
    }
    if (input.count == 0) {
        [input addObject:@{@"role": @"user", @"content": @""}];
    }

    NSMutableDictionary *upstream = [NSMutableDictionary dictionary];
    upstream[@"model"] = model;
    upstream[@"input"] = input;

    id maxTokens = incoming[@"max_tokens"];
    if ([maxTokens isKindOfClass:[NSNumber class]]) {
        upstream[@"max_output_tokens"] = maxTokens;
    }
    // temperature is read from the request, but the Responses API is sent the
    // fields it accepts for this gateway; keep it out of the payload.

    NSString *error = nil;
    id response = RelayCallUpstream(apiKey, upstream, &error);

    if (response == nil) {
        os_log(RelayLog(), "relay upstream error: %{public}s", (error ?: @"unknown").UTF8String);
        RelaySendSSE(fd, RelaySSEError(error));
        return;
    }

    NSString *text = RelayTextFromResponse(response);
    os_log(RelayLog(), "relay ok, %{public}lu chars", (unsigned long)text.length);
    RelaySendSSE(fd, RelaySSEChunk(model, text));
}

#pragma mark - Listener

static void RelayStartListener(void) {
    dispatch_async(dispatch_get_global_queue(QOS_CLASS_UTILITY, 0), ^{
        @try {
            int fd = socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0) {
                os_log_error(RelayLog(), "relay socket() failed: %{public}d", errno);
                return;
            }

            int one = 1;
            setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

            struct sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_port = htons((uint16_t)kRelayPort);
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

            if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
                os_log_error(RelayLog(), "relay bind 127.0.0.1:%{public}d failed: %{public}d",
                             kRelayPort, errno);
                close(fd);
                return;
            }
            if (listen(fd, 32) != 0) {
                os_log_error(RelayLog(), "relay listen failed: %{public}d", errno);
                close(fd);
                return;
            }

            // Only hint logs once the socket is actually listening.
            os_log(RelayLog(), "relay listening on %{public}d", kRelayPort);

            for (;;) {
                int client = accept(fd, NULL, NULL);
                if (client < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    os_log_error(RelayLog(), "relay accept failed: %{public}d", errno);
                    break;
                }
                setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
                @autoreleasepool {
                    @try {
                        RelayHandleConnection(client);
                    } @catch (NSException *e) {
                        os_log_error(RelayLog(), "relay handler exception: %{public}s",
                                     e.reason.UTF8String ?: "?");
                    }
                }
                close(client);
            }
            close(fd);
        } @catch (NSException *e) {
            os_log_error(RelayLog(), "relay listener exception: %{public}s",
                         e.reason.UTF8String ?: "?");
        } @catch (...) {
            os_log_error(RelayLog(), "relay listener unknown exception");
        }
    });
}

__attribute__((constructor)) static void TweakInitialize(void) {
    // Apelin's confirm_loaded looks for this exact marker and build id under
    // the package id's subsystem. Keep the line, and keep it public.
    os_log(RelayLog(), "ApelinLoaded %{public}s in %{public}s", APELIN_BUILD_ID, getprogname());

    @try {
        static dispatch_once_t once;
        dispatch_once(&once, ^{
            RelayStartListener();
        });
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay start exception: %{public}s", e.reason.UTF8String ?: "?");
    }
}
