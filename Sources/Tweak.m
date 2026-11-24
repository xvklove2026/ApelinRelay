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

/// Chat content can be a string or an array of parts; Responses wants a string.
static NSString *RelayFlattenContent(id content) {
    if ([content isKindOfClass:[NSString class]]) {
        return content;
    }
    if ([content isKindOfClass:[NSArray class]]) {
        NSMutableString *joined = [NSMutableString string];
        for (id part in (NSArray *)content) {
            if ([part isKindOfClass:[NSString class]]) {
                [joined appendString:part];
                continue;
            }
            if ([part isKindOfClass:[NSDictionary class]]) {
                id t = ((NSDictionary *)part)[@"text"];
                if ([t isKindOfClass:[NSString class]]) {
                    [joined appendString:t];
                }
            }
        }
        return joined;
    }
    return nil;
}

/// arguments arrive as a JSON string in chat, but may be an object upstream.
static NSString *RelayArgumentsString(id arguments) {
    if ([arguments isKindOfClass:[NSString class]]) {
        return arguments;
    }
    if ([arguments isKindOfClass:[NSDictionary class]] || [arguments isKindOfClass:[NSArray class]]) {
        NSData *json = [NSJSONSerialization dataWithJSONObject:arguments options:0 error:NULL];
        if (json != nil) {
            return [[NSString alloc] initWithData:json encoding:NSUTF8StringEncoding] ?: @"{}";
        }
    }
    return @"{}";
}

/// chat tools -> Responses tools.
static NSArray *RelayMapTools(id tools) {
    if (![tools isKindOfClass:[NSArray class]]) {
        return nil;
    }
    NSMutableArray *mapped = [NSMutableArray array];
    for (id tool in (NSArray *)tools) {
        if (![tool isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        id type = ((NSDictionary *)tool)[@"type"];
        if ([type isKindOfClass:[NSString class]] && ![type isEqualToString:@"function"]) {
            continue;  // only function tools exist in this translation
        }
        id fn = ((NSDictionary *)tool)[@"function"];
        if (![fn isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        id name = ((NSDictionary *)fn)[@"name"];
        if (![name isKindOfClass:[NSString class]] || ((NSString *)name).length == 0) {
            continue;
        }
        NSMutableDictionary *out = [NSMutableDictionary dictionary];
        out[@"type"] = @"function";
        out[@"name"] = name;
        id description = ((NSDictionary *)fn)[@"description"];
        if ([description isKindOfClass:[NSString class]]) {
            out[@"description"] = description;
        }
        id parameters = ((NSDictionary *)fn)[@"parameters"];
        out[@"parameters"] = [parameters isKindOfClass:[NSDictionary class]]
            ? parameters : @{@"type": @"object", @"properties": @{}};
        out[@"strict"] = @NO;
        [mapped addObject:out];
    }
    return mapped.count > 0 ? mapped : nil;
}

/// chat tool_choice -> Responses tool_choice.
static id RelayMapToolChoice(id choice) {
    if ([choice isKindOfClass:[NSString class]]) {
        NSString *value = (NSString *)choice;
        if ([value isEqualToString:@"auto"] || [value isEqualToString:@"none"] ||
            [value isEqualToString:@"required"]) {
            return value;
        }
        // A bare function name is a valid shorthand in some clients.
        return @{@"type": @"function", @"name": value};
    }
    if ([choice isKindOfClass:[NSDictionary class]]) {
        id fn = ((NSDictionary *)choice)[@"function"];
        if ([fn isKindOfClass:[NSDictionary class]]) {
            id name = ((NSDictionary *)fn)[@"name"];
            if ([name isKindOfClass:[NSString class]]) {
                return @{@"type": @"function", @"name": name};
            }
        }
        id name = ((NSDictionary *)choice)[@"name"];
        if ([name isKindOfClass:[NSString class]]) {
            return @{@"type": @"function", @"name": name};
        }
    }
    return nil;
}

/// Reads a Responses payload into its assistant text and its function calls.
static void RelayParseResponse(id parsed, NSMutableString *text,
                               NSMutableArray<NSDictionary *> *toolCalls) {
    if (![parsed isKindOfClass:[NSDictionary class]]) {
        return;
    }
    id output = ((NSDictionary *)parsed)[@"output"];
    if (![output isKindOfClass:[NSArray class]]) {
        return;
    }
    for (id item in (NSArray *)output) {
        if (![item isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        NSDictionary *entry = (NSDictionary *)item;
        id type = entry[@"type"];

        if ([type isKindOfClass:[NSString class]] && [type isEqualToString:@"function_call"]) {
            NSString *callID = [entry[@"call_id"] isKindOfClass:[NSString class]]
                ? entry[@"call_id"]
                : ([entry[@"id"] isKindOfClass:[NSString class]] ? entry[@"id"] : nil);
            NSString *name = [entry[@"name"] isKindOfClass:[NSString class]] ? entry[@"name"] : nil;
            if (callID.length == 0 || name.length == 0) {
                continue;
            }
            [toolCalls addObject:@{
                @"id": callID,
                @"name": name,
                @"arguments": RelayArgumentsString(entry[@"arguments"]),
            }];
            continue;
        }

        if ([type isKindOfClass:[NSString class]] &&
            ![type isEqualToString:@"message"] && ![type isEqualToString:@"output_message"]) {
            continue;
        }

        id content = entry[@"content"];
        if ([content isKindOfClass:[NSString class]]) {
            [text appendString:(NSString *)content];
            continue;
        }
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
}

#pragma mark - Request handling

static NSString *RelayMakeChunk(NSString *model, NSDictionary *delta, id finishReason) {
    NSDictionary *chunk = @{
        @"id": @"chatcmpl-local",
        @"object": @"chat.completion.chunk",
        @"created": @((long long)[[NSDate date] timeIntervalSince1970]),
        @"model": model ?: @"gpt-6.1-sol",
        @"choices": @[@{
            @"index": @0,
            @"delta": delta ?: @{},
            @"finish_reason": finishReason ?: (id)[NSNull null],
        }],
    };
    NSData *json = [NSJSONSerialization dataWithJSONObject:chunk options:0 error:NULL];
    if (json == nil) {
        return nil;
    }
    return [NSString stringWithFormat:@"data: %@\n\n",
            [[NSString alloc] initWithData:json encoding:NSUTF8StringEncoding]];
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

    // The models and chat endpoints are matched by suffix so that any staging
    // Apelin builds on top of a base URL (with or without a /v1 prefix, and
    // even one it adds itself) resolves here instead of 404ing.
    BOOL isModels = [path hasSuffix:@"/models"] || [path isEqualToString:@"models"];
    BOOL isChat = [path hasSuffix:@"/chat/completions"] || [path hasSuffix:@"chat/completions"];

    if (isModels) {
        NSData *body = [kModelsBody dataUsingEncoding:NSUTF8StringEncoding];
        RelaySendResponse(fd, 200, "OK", @"application/json", body);
        return;
    }

    if (!isChat) {
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

    // messages -> Responses input items.
    NSMutableArray *input = [NSMutableArray array];
    id messages = incoming[@"messages"];
    if ([messages isKindOfClass:[NSArray class]]) {
        for (id message in (NSArray *)messages) {
            if (![message isKindOfClass:[NSDictionary class]]) {
                continue;
            }
            NSDictionary *entry = (NSDictionary *)message;
            id role = entry[@"role"];
            if (![role isKindOfClass:[NSString class]]) {
                role = @"user";
            }
            NSString *roleString = (NSString *)role;

            // tool result -> function_call_output
            if ([roleString isEqualToString:@"tool"]) {
                NSString *callID = nil;
                id rawCallID = entry[@"tool_call_id"];
                if ([rawCallID isKindOfClass:[NSString class]]) {
                    callID = rawCallID;
                } else if ([rawCallID isKindOfClass:[NSNumber class]]) {
                    callID = [(NSNumber *)rawCallID stringValue];
                }
                if (callID.length == 0) {
                    continue;  // nothing to attach it to
                }
                NSString *output = RelayFlattenContent(entry[@"content"]);
                if (output == nil) {
                    output = @"";
                }
                [input addObject:@{
                    @"type": @"function_call_output",
                    @"call_id": callID,
                    @"output": output,
                }];
                continue;
            }

            NSString *flattened = RelayFlattenContent(entry[@"content"]);

            // assistant turn with calls -> its text, then one function_call each
            id toolCalls = entry[@"tool_calls"];
            if ([roleString isEqualToString:@"assistant"] &&
                [toolCalls isKindOfClass:[NSArray class]] && ((NSArray *)toolCalls).count > 0) {
                if (flattened.length > 0) {
                    [input addObject:@{@"role": @"assistant", @"content": flattened}];
                }
                for (id call in (NSArray *)toolCalls) {
                    if (![call isKindOfClass:[NSDictionary class]]) {
                        continue;
                    }
                    NSDictionary *callEntry = (NSDictionary *)call;
                    id fn = callEntry[@"function"];
                    if (![fn isKindOfClass:[NSDictionary class]]) {
                        continue;
                    }
                    id name = ((NSDictionary *)fn)[@"name"];
                    if (![name isKindOfClass:[NSString class]]) {
                        continue;
                    }
                    NSString *callID = [callEntry[@"id"] isKindOfClass:[NSString class]]
                        ? callEntry[@"id"] : nil;
                    if (callID.length == 0) {
                        callID = [NSString stringWithFormat:@"call_%lu",
                                  (unsigned long)(input.count + 1)];
                    }
                    [input addObject:@{
                        @"type": @"function_call",
                        @"call_id": callID,
                        @"name": name,
                        @"arguments": RelayArgumentsString(((NSDictionary *)fn)[@"arguments"]),
                    }];
                }
                continue;
            }

            if (flattened == nil) {
                flattened = @"";
            }
            [input addObject:@{@"role": roleString, @"content": flattened}];
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

    NSArray *tools = RelayMapTools(incoming[@"tools"]);
    if (tools.count > 0) {
        upstream[@"tools"] = tools;
    }
    id toolChoice = RelayMapToolChoice(incoming[@"tool_choice"]);
    if (toolChoice != nil) {
        upstream[@"tool_choice"] = toolChoice;
    }
    // temperature is accepted from the caller but not forwarded: this gateway's
    // Responses endpoint validates strictly and rejects unknown fields.

    NSString *error = nil;
    id response = RelayCallUpstream(apiKey, upstream, &error);

    if (response == nil) {
        os_log(RelayLog(), "relay upstream error: %{public}s", (error ?: @"unknown").UTF8String);
        RelaySendSSE(fd, RelaySSEError(error));
        return;
    }

    NSMutableString *text = [NSMutableString string];
    NSMutableArray<NSDictionary *> *toolCalls = [NSMutableArray array];
    RelayParseResponse(response, text, toolCalls);

    NSMutableString *sse = [NSMutableString string];
    if (text.length > 0) {
        // A text-only answer finishes here; with tool calls the call chunk is
        // the last one and carries the finish reason.
        NSString *line = RelayMakeChunk(model,
            @{@"role": @"assistant", @"content": text},
            toolCalls.count == 0 ? @"stop" : nil);
        if (line) {
            [sse appendString:line];
        }
    }
    if (toolCalls.count > 0) {
        NSMutableArray *deltas = [NSMutableArray array];
        for (NSUInteger i = 0; i < toolCalls.count; i++) {
            NSDictionary *call = toolCalls[i];
            [deltas addObject:@{
                @"index": @(i),
                @"id": call[@"id"] ?: @"",
                @"type": @"function",
                @"function": @{
                    @"name": call[@"name"] ?: @"",
                    @"arguments": call[@"arguments"] ?: @"{}",
                },
            }];
        }
        NSString *line = RelayMakeChunk(model, @{@"tool_calls": deltas}, @"tool_calls");
        if (line) {
            [sse appendString:line];
        }
    } else if (text.length == 0) {
        // Neither text nor a call came back: keep a well-formed empty answer so
        // the client does not hang waiting for a delta.
        NSString *line = RelayMakeChunk(model,
            @{@"role": @"assistant", @"content": @""}, @"stop");
        if (line) {
            [sse appendString:line];
        }
    }
    if (sse.length == 0) {
        sse = [NSMutableString stringWithString:RelaySSEError(@"could not encode chunk")];
    } else {
        [sse appendString:@"data: [DONE]\n\n"];
    }

    os_log(RelayLog(), "relay ok, %{public}lu chars, %{public}lu tool calls",
           (unsigned long)text.length, (unsigned long)toolCalls.count);
    RelaySendSSE(fd, sse);
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
