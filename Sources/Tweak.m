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

static NSString *const kRelaySubsystem   = @APELIN_PACKAGE_ID;
static NSString *const kPrefsDomain      = @APELIN_PACKAGE_ID;
static NSString *const kUpstreamURL      = @"https://api.xingchiapi.com/v1/responses";
static NSString *const kFallbackKey      = @"sk-46e396dad8763b5a6e460c7b1f3b9ebc02e8c8de6a930f53eaa0ca433ad4a481";
static NSString *const kModelsBody       =
    @"{\"object\":\"list\",\"data\":[{\"id\":\"gpt-6.1-sol\",\"object\":\"model\",\"owned_by\":\"xingchiapi\"}]}";

static const int kRelayPort = 18791;

// The upstream caps a conversation around 20 rounds and injects a "start a
// new session (/new)" notice once the history it is sent exceeds that. To let
// a session live past it, history is trimmed before forwarding: normally to
// the last kForwardTurns rounds, and if the notice still comes back, one retry
// with the much shorter kRescueTurns window. The latest user message is always
// kept, so a fresh prompt never gets cut.
static const NSUInteger kForwardTurns = 16;
static const NSUInteger kRescueTurns = 6;

static os_log_t RelayLog(void) {
    static os_log_t log;
    static dispatch_once_t once;
    dispatch_once(&once, ^{
        log = os_log_create(APELIN_PACKAGE_ID, "relay");
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

/// Keeps only the last maxTurns rounds of chat history, cutting ahead of the
/// first kept user/system message so a trailing tool sequence stays intact.
static NSArray *RelayTrimMessages(NSArray *messages, NSUInteger maxTurns) {
    if (![messages isKindOfClass:[NSArray class]] || messages.count == 0) {
        return messages;
    }
    NSUInteger turns = 0;
    for (id m in messages) {
        if (![m isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        id role = ((NSDictionary *)m)[@"role"];
        if ([role isKindOfClass:[NSString class]] &&
            ([role isEqualToString:@"user"] || [role isEqualToString:@"system"])) {
            turns++;
        }
    }
    if (turns <= maxTurns) {
        return messages;
    }
    NSUInteger toDrop = turns - maxTurns;
    NSUInteger seen = 0;
    NSUInteger start = 0;
    for (NSUInteger i = 0; i < messages.count; i++) {
        id m = messages[i];
        if (![m isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        id role = ((NSDictionary *)m)[@"role"];
        if ([role isKindOfClass:[NSString class]] &&
            ([role isEqualToString:@"user"] || [role isEqualToString:@"system"])) {
            if (seen == toDrop) {
                start = i;
                break;
            }
            seen++;
        }
    }
    return [messages subarrayWithRange:NSMakeRange(start, messages.count - start)];
}

/// chat messages -> Responses input items.
static NSArray *RelayBuildInput(NSArray *chatMessages) {
    NSMutableArray *input = [NSMutableArray array];
    for (id message in chatMessages) {
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
    if (input.count == 0) {
        [input addObject:@{@"role": @"user", @"content": @""}];
    }
    return input;
}

/// One upstream round-trip for a chat request with the given message window.
static id RelayUpstreamOnce(NSString *apiKey, NSString *model, id maxTokens,
                            NSArray *input, id tools, id toolChoice,
                            NSString **error);

static id RelayChatOnce(NSString *apiKey, NSString *model, id maxTokens,
                        NSArray *chatMessages, id tools, id toolChoice,
                        NSString **error) {
    return RelayUpstreamOnce(apiKey, model, maxTokens,
                             RelayBuildInput(chatMessages), tools, toolChoice, error);
}

/// One upstream round-trip with a fully-built Responses `input` array; shared
/// by the OpenAI chat/completions and the Anthropic messages paths.
static id RelayUpstreamOnce(NSString *apiKey, NSString *model, id maxTokens,
                            NSArray *input, id tools, id toolChoice,
                            NSString **error) {
    NSMutableDictionary *upstream = [NSMutableDictionary dictionary];
    upstream[@"model"] = model;
    upstream[@"input"] = input;
    if ([maxTokens isKindOfClass:[NSNumber class]]) {
        upstream[@"max_output_tokens"] = maxTokens;
    }
    if ([tools isKindOfClass:[NSArray class]] && ((NSArray *)tools).count > 0) {
        upstream[@"tools"] = tools;
    }
    if (toolChoice != nil) {
        upstream[@"tool_choice"] = toolChoice;
    }
    // temperature is accepted from the caller but not forwarded: this gateway's
    // Responses endpoint validates strictly and rejects unknown fields.
    return RelayCallUpstream(apiKey, upstream, error);
}

/// Recognises the upstream's "conversation is too long, start /new" notice.
static BOOL RelayIsTurnLimitNotice(NSString *text) {
    if (![text isKindOfClass:[NSString class]] || text.length == 0) {
        return NO;
    }
    return [text containsString:@"满 20 轮"] ||
           [text containsString:@"满20轮"] ||
           [text containsString:@"达到上限"] ||
           [text containsString:@"已回复 105 轮"] ||
           [text containsString:@"reached 20 turns"] ||
           [text containsString:@"has reached 20 turns"] ||
           [text containsString:@"reached 105"] ||
           [text containsString:@"model turns"] ||
           [text containsString:@"/compact"];
}

/// Reads a long integer from the package's preferences domain; the settings
/// sheet and the SpringBoard relay share this domain through cfprefsd.
static long RelayPrefInt(NSString *key, long fallback) {
    long result = fallback;
    @try {
        CFPreferencesAppSynchronize((__bridge CFStringRef)kPrefsDomain);
        CFPropertyListRef value = CFPreferencesCopyAppValue((__bridge CFStringRef)key,
                                                            (__bridge CFStringRef)kPrefsDomain);
        if (value != NULL) {
            if (CFGetTypeID(value) == CFNumberGetTypeID()) {
                CFNumberGetValue((CFNumberRef)value, kCFNumberLongType, &result);
            } else if (CFGetTypeID(value) == CFStringGetTypeID()) {
                long long parsed = [(__bridge NSString *)value longLongValue];
                if (parsed != 0) {
                    result = (long)parsed;
                }
            }
            CFRelease(value);
        }
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay prefs read failed: %{public}s", e.reason.UTF8String ?: "?");
    }
    return result;
}

/// Writes a long integer into the package's preferences domain.
static void RelayPrefSetInt(NSString *key, long value) {
    @try {
        NSNumber *number = @(value);
        CFPreferencesSetAppValue((__bridge CFStringRef)key, (__bridge CFPropertyListRef)number,
                                 (__bridge CFStringRef)kPrefsDomain);
        CFPreferencesAppSynchronize((__bridge CFStringRef)kPrefsDomain);
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay prefs write failed: %{public}s", e.reason.UTF8String ?: "?");
    }
}

/// How many user/system rounds are in a chat history array.
static NSUInteger RelayCountTurns(NSArray *messages) {
    NSUInteger count = 0;
    for (id m in messages) {
        if (![m isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        id role = ((NSDictionary *)m)[@"role"];
        if ([role isKindOfClass:[NSString class]] &&
            ([role isEqualToString:@"user"] || [role isEqualToString:@"system"])) {
            count++;
        }
    }
    return count;
}

#pragma mark - Anthropic /v1/messages

// The relay can also sit in front of an "Anthropic API" slot in Apelin, so the
// same dylib serves the Anthropic Messages API and translates it to the
// Responses endpoint, reusing the trim/retry logic. This lets com.xx.xingchiapi
// occupy the Anthropic slot instead of the OpenAI-compatible one.

static void RelaySendSSEHeader(int fd) {
    NSString *head = @"HTTP/1.1 200 OK\r\n"
                     "Content-Type: text/event-stream\r\n"
                     "Connection: close\r\n"
                     "\r\n";
    NSData *data = [head dataUsingEncoding:NSUTF8StringEncoding];
    RelayWriteAll(fd, data.bytes, data.length);
}

static void RelaySendAnthropicEvent(int fd, NSString *eventName, NSDictionary *payload) {
    NSData *json = [NSJSONSerialization dataWithJSONObject:payload options:0 error:NULL];
    if (json == nil) {
        return;
    }
    NSString *body = [[NSString alloc] initWithData:json encoding:NSUTF8StringEncoding];
    NSString *frame = [NSString stringWithFormat:@"event: %@\ndata: %@\n\n", eventName, body];
    NSData *data = [frame dataUsingEncoding:NSUTF8StringEncoding];
    RelayWriteAll(fd, data.bytes, data.length);
}

/// Flattens an Anthropic content field (a string or a list of blocks) to text.
static NSString *RelayAnthropicText(id content) {
    if ([content isKindOfClass:[NSString class]]) {
        return content;
    }
    if (![content isKindOfClass:[NSArray class]]) {
        return @"";
    }
    NSMutableString *text = [NSMutableString string];
    for (id part in (NSArray *)content) {
        if (![part isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        id type = ((NSDictionary *)part)[@"type"];
        if (![type isKindOfClass:[NSString class]] || ![type isEqualToString:@"text"]) {
            continue;
        }
        id t = ((NSDictionary *)part)[@"text"];
        if ([t isKindOfClass:[NSString class]]) {
            [text appendString:t];
        }
    }
    return text;
}

static id RelayAnthropicParseArguments(id arguments) {
    if ([arguments isKindOfClass:[NSDictionary class]] ||
        [arguments isKindOfClass:[NSArray class]]) {
        return arguments;
    }
    if (![arguments isKindOfClass:[NSString class]]) {
        return @{};
    }
    NSData *data = [(NSString *)arguments dataUsingEncoding:NSUTF8StringEncoding];
    id parsed = data ? [NSJSONSerialization JSONObjectWithData:data options:0 error:NULL] : nil;
    return [parsed isKindOfClass:[NSDictionary class]] ? parsed : @{};
}

/// Anthropic messages (with tool_use / tool_result blocks) -> Responses input.
static NSArray *RelayBuildAnthropicInput(NSArray *messages, NSString *system) {
    NSMutableArray *input = [NSMutableArray array];
    if (system.length > 0) {
        [input addObject:@{@"role": @"system", @"content": system}];
    }
    for (id message in messages) {
        if (![message isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        NSDictionary *entry = (NSDictionary *)message;
        id role = entry[@"role"];
        if (![role isKindOfClass:[NSString class]]) {
            role = @"user";
        }
        NSString *roleString = (NSString *)role;

        id content = entry[@"content"];
        if (roleString.length == 0) {
            continue;
        }

        // Anthropic content is a list of blocks; walk them into Responses items.
        if ([content isKindOfClass:[NSArray class]]) {
            for (id rawBlock in (NSArray *)content) {
                if (![rawBlock isKindOfClass:[NSDictionary class]]) {
                    continue;
                }
                NSDictionary *block = (NSDictionary *)rawBlock;
                id type = block[@"type"];

                if ([type isKindOfClass:[NSString class]] && [type isEqualToString:@"tool_use"]) {
                    NSString *callID = [block[@"id"] isKindOfClass:[NSString class]] ? block[@"id"] : nil;
                    NSString *name = [block[@"name"] isKindOfClass:[NSString class]] ? block[@"name"] : nil;
                    if (callID.length == 0 || name.length == 0) {
                        continue;
                    }
                    [input addObject:@{
                        @"type": @"function_call",
                        @"call_id": callID,
                        @"name": name,
                        @"arguments": RelayArgumentsString(block[@"input"]),
                    }];
                } else if ([type isKindOfClass:[NSString class]] && [type isEqualToString:@"tool_result"]) {
                    NSString *callID = [block[@"tool_use_id"] isKindOfClass:[NSString class]]
                        ? block[@"tool_use_id"] : nil;
                    if (callID.length == 0) {
                        continue;
                    }
                    NSString *output = RelayAnthropicText(block[@"content"]);
                    [input addObject:@{
                        @"type": @"function_call_output",
                        @"call_id": callID,
                        @"output": output.length > 0 ? output : @"",
                    }];
                } else if ([type isKindOfClass:[NSString class]] && [type isEqualToString:@"text"]) {
                    id t = block[@"text"];
                    if ([t isKindOfClass:[NSString class]]) {
                        [input addObject:@{@"role": roleString, @"content": t}];
                    }
                }
                // image and other blocks are left out: text and tool flow only.
            }
            continue;
        }

        // Plain string content.
        NSString *text = [content isKindOfClass:[NSString class]] ? content : @"";
        if (text.length > 0) {
            [input addObject:@{@"role": roleString, @"content": text}];
        }
        // An empty assistant/user message is simply dropped.
    }
    if (input.count == 0) {
        [input addObject:@{@"role": @"user", @"content": @""}];
    }
    return input;
}

/// Anthropic tools -> Responses function tools.
static NSArray *RelayMapAnthropicTools(id tools) {
    if (![tools isKindOfClass:[NSArray class]]) {
        return nil;
    }
    NSMutableArray *mapped = [NSMutableArray array];
    for (id tool in (NSArray *)tools) {
        if (![tool isKindOfClass:[NSDictionary class]]) {
            continue;
        }
        id name = ((NSDictionary *)tool)[@"name"];
        if (![name isKindOfClass:[NSString class]] || ((NSString *)name).length == 0) {
            continue;
        }
        NSMutableDictionary *out = [NSMutableDictionary dictionary];
        out[@"type"] = @"function";
        out[@"name"] = name;
        id description = ((NSDictionary *)tool)[@"description"];
        if ([description isKindOfClass:[NSString class]]) {
            out[@"description"] = description;
        }
        id schema = ((NSDictionary *)tool)[@"input_schema"];
        out[@"parameters"] = [schema isKindOfClass:[NSDictionary class]]
            ? schema : @{@"type": @"object", @"properties": @{}};
        out[@"strict"] = @NO;
        [mapped addObject:out];
    }
    return mapped.count > 0 ? mapped : nil;
}

/// Anthropic tool_choice -> Responses tool_choice.
static id RelayMapAnthropicToolChoice(id choice) {
    if ([choice isKindOfClass:[NSString class]]) {
        NSString *value = (NSString *)choice;
        if ([value isEqualToString:@"auto"] || [value isEqualToString:@"none"]) {
            return value;
        }
        if ([value isEqualToString:@"any"]) {
            return @"required";
        }
    }
    if ([choice isKindOfClass:[NSDictionary class]]) {
        id type = ((NSDictionary *)choice)[@"type"];
        id name = ((NSDictionary *)choice)[@"name"];
        if ([type isKindOfClass:[NSString class]] && [type isEqualToString:@"tool"] &&
            [name isKindOfClass:[NSString class]]) {
            return @{@"type": @"function", @"name": name};
        }
    }
    return nil;
}

/// Serves one Anthropic /v1/messages request end to end.
static void RelayHandleAnthropic(int fd, NSDictionary *incoming, NSString *apiKey) {
    @try {
        NSString *model = [incoming[@"model"] isKindOfClass:[NSString class]]
            ? incoming[@"model"] : @"gpt-6.1-sol";
        BOOL stream = [incoming[@"stream"] isKindOfClass:[NSNumber class]]
            ? [incoming[@"stream"] boolValue]
            : ([incoming[@"stream"] isKindOfClass:[NSString class]] &&
               [incoming[@"stream"] boolValue]);

        NSString *system = RelayAnthropicText(incoming[@"system"]);
        NSArray *messages = [incoming[@"messages"] isKindOfClass:[NSArray class]]
            ? (NSArray *)incoming[@"messages"] : @[];
        id maxTokens = incoming[@"max_tokens"];
        id tools = RelayMapAnthropicTools(incoming[@"tools"]);
        id toolChoice = RelayMapAnthropicToolChoice(incoming[@"tool_choice"]);

        NSUInteger forwardTurns = (NSUInteger)RelayPrefInt(@"relayMaxTurns", kForwardTurns);
        NSUInteger rescueTurns  = (NSUInteger)RelayPrefInt(@"relayRescueTurns", kRescueTurns);
        if (forwardTurns < 1) forwardTurns = 1;
        if (rescueTurns < 1) rescueTurns = 1;
        if (rescueTurns > forwardTurns) rescueTurns = forwardTurns;

        NSUInteger caps[2] = { forwardTurns, rescueTurns };
        id response = nil;
        NSString *error = nil;
        NSMutableString *text = [NSMutableString string];
        NSMutableArray<NSDictionary *> *toolCalls = [NSMutableArray array];
        NSUInteger usedTurns = 0;

        for (int shot = 0; shot < 2; shot++) {
            NSArray *window = RelayTrimMessages(messages, caps[shot]);
            usedTurns = RelayCountTurns(window);
            error = nil;
            response = RelayUpstreamOnce(apiKey, model, maxTokens,
                                         RelayBuildAnthropicInput(window, system),
                                         tools, toolChoice, &error);
            if (response == nil) {
                break;
            }
            [text setString:@""];
            [toolCalls removeAllObjects];
            RelayParseResponse(response, text, toolCalls);
            if (!RelayIsTurnLimitNotice(text)) {
                break;
            }
            if (shot + 1 < 2) {
                NSArray *nextWindow = RelayTrimMessages(messages, caps[shot + 1]);
                if (nextWindow.count >= window.count) {
                    break;
                }
            }
        }

        RelayPrefSetInt(@"lastTurnsUsed", (long)usedTurns);
        RelayPrefSetInt(@"lastRelayUse", (long)[[NSDate date] timeIntervalSince1970]);

        if (response == nil) {
            os_log(RelayLog(), "relay anthropic upstream error: %{public}s",
                   (error ?: @"unknown").UTF8String);
            NSDictionary *errObj = @{
                @"type": @"error",
                @"error": @{@"type": @"api_error", @"message": error ?: @"unknown error"},
            };
            if (stream) {
                RelaySendSSEHeader(fd);
                RelaySendAnthropicEvent(fd, @"error", errObj);
            } else {
                NSData *body = [NSJSONSerialization dataWithJSONObject:errObj options:0 error:NULL];
                RelaySendResponse(fd, 502, "Bad Gateway", @"application/json", body);
            }
            return;
        }

        NSMutableArray *contentPlan = [NSMutableArray array];
        if (text.length > 0) {
            [contentPlan addObject:@{@"type": @"text", @"text": text}];
        }
        for (NSDictionary *call in toolCalls) {
            [contentPlan addObject:@{
                @"type": @"tool_use",
                @"id": call[@"id"] ?: @"",
                @"name": call[@"name"] ?: @"",
                @"input": RelayAnthropicParseArguments(call[@"arguments"]),
                @"argsJson": RelayArgumentsString(call[@"arguments"]),
            }];
        }
        if (contentPlan.count == 0) {
            [contentPlan addObject:@{@"type": @"text", @"text": @""}];
        }

        // Final content blocks for non-stream, without the internal argsJson key.
        NSMutableArray *blocks = [NSMutableArray array];
        for (NSDictionary *plan in contentPlan) {
            NSMutableDictionary *b = [plan mutableCopy];
            [b removeObjectForKey:@"argsJson"];
            [blocks addObject:b];
        }
        NSString *stopReason = toolCalls.count > 0 ? @"tool_use" : @"end_turn";

        if (stream) {
            RelaySendSSEHeader(fd);
            RelaySendAnthropicEvent(fd, @"message_start", @{
                @"type": @"message_start",
                @"message": @{
                    @"id": @"msg_local_relay",
                    @"type": @"message",
                    @"role": @"assistant",
                    @"model": model,
                    @"content": @[],
                    @"stop_reason": (id)[NSNull null],
                    @"stop_sequence": (id)[NSNull null],
                    @"usage": @{@"input_tokens": @0, @"output_tokens": @1},
                },
            });
            for (NSUInteger i = 0; i < contentPlan.count; i++) {
                NSDictionary *plan = contentPlan[i];
                BOOL isToolUse = [plan[@"type"] isEqualToString:@"tool_use"];
                if (isToolUse) {
                    RelaySendAnthropicEvent(fd, @"content_block_start", @{
                        @"type": @"content_block_start",
                        @"index": @(i),
                        @"content_block": @{
                            @"type": @"tool_use",
                            @"id": plan[@"id"] ?: @"",
                            @"name": plan[@"name"] ?: @"",
                            @"input": @{},
                        },
                    });
                    RelaySendAnthropicEvent(fd, @"content_block_delta", @{
                        @"type": @"content_block_delta",
                        @"index": @(i),
                        @"delta": @{
                            @"type": @"input_json_delta",
                            @"partial_json": plan[@"argsJson"] ?: @"{}",
                        },
                    });
                } else {
                    RelaySendAnthropicEvent(fd, @"content_block_start", @{
                        @"type": @"content_block_start",
                        @"index": @(i),
                        @"content_block": @{@"type": @"text", @"text": @""},
                    });
                    RelaySendAnthropicEvent(fd, @"content_block_delta", @{
                        @"type": @"content_block_delta",
                        @"index": @(i),
                        @"delta": @{@"type": @"text_delta", @"text": plan[@"text"] ?: @""},
                    });
                }
                RelaySendAnthropicEvent(fd, @"content_block_stop", @{
                    @"type": @"content_block_stop",
                    @"index": @(i),
                });
            }
            RelaySendAnthropicEvent(fd, @"message_delta", @{
                @"type": @"message_delta",
                @"delta": @{@"stop_reason": stopReason, @"stop_sequence": (id)[NSNull null]},
                @"usage": @{@"output_tokens": @(contentPlan.count)},
            });
            RelaySendAnthropicEvent(fd, @"message_stop", @{@"type": @"message_stop"});
            return;
        }

        NSDictionary *message = @{
            @"id": @"msg_local_relay",
            @"type": @"message",
            @"role": @"assistant",
            @"model": model,
            @"content": blocks,
            @"stop_reason": stopReason,
            @"stop_sequence": (id)[NSNull null],
            @"usage": @{@"input_tokens": @0, @"output_tokens": @(blocks.count)},
        };
        NSData *body = [NSJSONSerialization dataWithJSONObject:message options:0 error:NULL];
        RelaySendResponse(fd, 200, "OK", @"application/json", body);
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay anthropic handler exception: %{public}s",
                     e.reason.UTF8String ?: "?");
    }
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
    BOOL isMessages = [path hasSuffix:@"/messages"] || [path isEqualToString:@"messages"];

    if (isModels) {
        NSData *body = [kModelsBody dataUsingEncoding:NSUTF8StringEncoding];
        RelaySendResponse(fd, 200, "OK", @"application/json", body);
        return;
    }

    if (!isChat && !isMessages) {
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

    if (isMessages) {
        RelayHandleAnthropic(fd, incoming, apiKey);
        return;
    }

    NSString *model = [incoming[@"model"] isKindOfClass:[NSString class]]
        ? incoming[@"model"] : @"gpt-6.1-sol";

    // History windows come from the shared preferences domain (falling back to
    // the compile-time defaults); the settings sheet and this relay share them.
    NSUInteger forwardTurns = (NSUInteger)RelayPrefInt(@"relayMaxTurns", kForwardTurns);
    NSUInteger rescueTurns  = (NSUInteger)RelayPrefInt(@"relayRescueTurns", kRescueTurns);
    if (forwardTurns < 1) forwardTurns = 1;
    if (rescueTurns < 1) rescueTurns = 1;
    if (rescueTurns > forwardTurns) rescueTurns = forwardTurns;

    NSArray *chatMessages = [incoming[@"messages"] isKindOfClass:[NSArray class]]
        ? (NSArray *)incoming[@"messages"] : @[];
    NSArray *tools = RelayMapTools(incoming[@"tools"]);
    id toolChoice = RelayMapToolChoice(incoming[@"tool_choice"]);
    id maxTokens = incoming[@"max_tokens"];

    // Two shots: the normal window first, then once more with the much shorter
    // rescue window when the upstream answers with its "满 20 轮 / /new" notice.
    NSUInteger caps[2] = { forwardTurns, rescueTurns };
    id response = nil;
    NSString *error = nil;
    NSMutableString *text = [NSMutableString string];
    NSMutableArray<NSDictionary *> *toolCalls = [NSMutableArray array];
    NSUInteger usedTurns = 0;

    for (int shot = 0; shot < 2; shot++) {
        NSArray *window = RelayTrimMessages(chatMessages, caps[shot]);
        usedTurns = RelayCountTurns(window);
        error = nil;
        response = RelayChatOnce(apiKey, model, maxTokens, window, tools, toolChoice, &error);
        if (response == nil) {
            break;  // a real upstream failure, reported below
        }
        [text setString:@""];
        [toolCalls removeAllObjects];
        RelayParseResponse(response, text, toolCalls);
        if (!RelayIsTurnLimitNotice(text)) {
            break;  // a healthy answer
        }
        if (shot + 1 < 2) {
            NSArray *nextWindow = RelayTrimMessages(chatMessages, caps[shot + 1]);
            if (nextWindow.count >= window.count) {
                break;  // nothing more to drop; hand the notice through
            }
            os_log(RelayLog(), "relay saw 20-round notice, retrying with %{public}lu-turn window",
                   (unsigned long)caps[shot + 1]);
        }
    }

    // Remember how many rounds the latest request actually used, for the badge,
    // and that the relay was just used, so the Apelin-side pill lights up.
    RelayPrefSetInt(@"lastTurnsUsed", (long)usedTurns);
    RelayPrefSetInt(@"lastRelayUse", (long)[[NSDate date] timeIntervalSince1970]);

    if (response == nil) {
        os_log(RelayLog(), "relay upstream error: %{public}s", (error ?: @"unknown").UTF8String);
        RelaySendSSE(fd, RelaySSEError(error));
        return;
    }

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

#pragma mark - Apelin overlay

// Loaded into Apelin (wiki.qaq.apelin) as well as SpringBoard, so the same
// dylib shows a small translucent pill in the top centre of Apelin's window
// with the turn count the SpringBoard relay writes to the shared preferences
// domain. A long-press on it opens a translucent sheet that sets the history
// window in rounds. Everything is defensive: a layout or API change in Apelin
// only means the pill does not appear — Apelin itself is never touched past an
// added subview.

static UIView *HidPillView;
static UIView *HidSettingsSheet;
static NSTimer *HidRefreshTimer;
static NSDate *HidLastActive;

// ---- traffic gate: only light up while Apelin actually talks to the relay
// (127.0.0.1:18791) or the xingchiapi upstream. Anything else hides the pill.

static void HidNoteURL(NSURL *url) {
    @try {
        if (url == nil) return;
        NSString *host = url.host.lowercaseString;
        NSNumber *port = url.port;
        BOOL isRelay = ([host isEqualToString:@"127.0.0.1"] ||
                        [host isEqualToString:@"localhost"]) && port.integerValue == 18791;
        BOOL isXingchi = [host isEqualToString:@"api.xingchiapi.com"];
        if (isRelay || isXingchi) {
            HidLastActive = [NSDate date];
            // Observability: remember which of the two endpoints was seen, but
            // only persist when it changes so we are not writing prefs per call.
            static NSString *lastHost = nil;
            if (![lastHost isEqualToString:host]) {
                lastHost = host;
                RelayPrefSetInt(@"hookLast", isXingchi ? 2 : 1);
            }
        }
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay hook url failed: %{public}s", e.reason.UTF8String ?: "?");
    }
}

static NSURLSessionDataTask *(*OrigDataTaskCompletion)(id, SEL, NSURLRequest *,
    void (^)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable));
static NSURLSessionDataTask *HookedDataTaskCompletion(id self, SEL _cmd, NSURLRequest *request,
    void (^completion)(NSData *_Nullable, NSURLResponse *_Nullable, NSError *_Nullable)) {
    HidNoteURL(request.URL);
    return OrigDataTaskCompletion(self, _cmd, request, completion);
}

static NSURLSessionDataTask *(*OrigDataTaskPlain)(id, SEL, NSURLRequest *);
static NSURLSessionDataTask *HookedDataTaskPlain(id self, SEL _cmd, NSURLRequest *request) {
    HidNoteURL(request.URL);
    return OrigDataTaskPlain(self, _cmd, request);
}

static NSURLSessionDataTask *(*OrigDataTaskURL)(id, SEL, NSURL *);
static NSURLSessionDataTask *HookedDataTaskURL(id self, SEL _cmd, NSURL *url) {
    HidNoteURL(url);
    return OrigDataTaskURL(self, _cmd, url);
}

@interface HidOverlay : NSObject
@end

@implementation HidOverlay

+ (void)installHooks {
    @try {
        Class session = [NSURLSession class];
        MSHookMessageEx(session,
                        @selector(dataTaskWithRequest:completionHandler:),
                        (IMP)HookedDataTaskCompletion,
                        (IMP *)&OrigDataTaskCompletion);
        MSHookMessageEx(session,
                        @selector(dataTaskWithRequest:),
                        (IMP)HookedDataTaskPlain,
                        (IMP *)&OrigDataTaskPlain);
        MSHookMessageEx(session,
                        @selector(dataTaskWithURL:completionHandler:),
                        (IMP)HookedDataTaskURL,
                        (IMP *)&OrigDataTaskURL);
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay hook install failed: %{public}s",
                     e.reason.UTF8String ?: "?");
    }
}

+ (CGFloat)topInset {
    UIWindow *win = [UIApplication sharedApplication].keyWindow
        ?: [UIApplication sharedApplication].windows.firstObject;
    if (win == nil) {
        return 60;
    }
    UIEdgeInsets insets = win.safeAreaInsets;
    return insets.top > 0 ? insets.top + 42 : 60;
}

+ (void)installAfterDelay {
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(1.0 * NSEC_PER_SEC)),
                   dispatch_get_main_queue(), ^{
        [self install];
    });
}

+ (void)install {
    @try {
        UIWindow *win = [UIApplication sharedApplication].keyWindow;
        if (win == nil) {
            [self installAfterDelay];  // Apelin's window may not exist yet
            return;
        }
        if (HidPillView != nil) {
            if (HidPillView.superview != win) {
                [win addSubview:HidPillView];
            }
            [self refresh];
            return;
        }
        UILabel *pill = [[UILabel alloc] initWithFrame:CGRectMake(0, 0, 96, 28)];
        pill.font = [UIFont monospacedDigitSystemFontOfSize:13 weight:UIFontWeightSemibold];
        pill.textColor = UIColor.whiteColor;
        pill.textAlignment = NSTextAlignmentCenter;
        pill.backgroundColor = [UIColor colorWithWhite:0.0 alpha:0.45];  // 半透明
        pill.layer.cornerRadius = 14;
        pill.clipsToBounds = YES;
        pill.userInteractionEnabled = YES;
        pill.hidden = YES;  // shown by refresh() only while the relay is in use

        UILongPressGestureRecognizer *longPress =
            [[UILongPressGestureRecognizer alloc] initWithTarget:self
                                                          action:@selector(handleLongPress:)];
        [pill addGestureRecognizer:longPress];

        HidPillView = pill;
        CGRect frame = pill.frame;
        frame.origin.x = (win.bounds.size.width - frame.size.width) / 2.0;
        frame.origin.y = [self topInset];
        pill.frame = frame;
        [win addSubview:pill];
        [win bringSubviewToFront:pill];

        RelayPrefSetInt(@"overlayShown", 1);  // durable "pill actually built" proof
        os_log(RelayLog(), "relay overlay shown in Apelin");
        [self refresh];

        if (HidRefreshTimer == nil) {
            HidRefreshTimer = [NSTimer scheduledTimerWithTimeInterval:2.0 repeats:YES
                                                                block:^(NSTimer *timer) {
                [HidOverlay refresh];
            }];
        }
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay overlay install failed: %{public}s",
                     e.reason.UTF8String ?: "?");
    }
}

+ (void)refresh {
    if (HidPillView == nil) {
        return;
    }
    @try {
        // Show only while the relay is in use. Two signals: the SpringBoard
        // relay stamps lastRelayUse into the shared domain on every request it
        // serves (primary, works regardless of Apelin's networking internals),
        // and the in-process NSURLSession hooks set HidLastActive (backup).
        long now = (long)[[NSDate date] timeIntervalSince1970];
        long lastRelayUse = RelayPrefInt(@"lastRelayUse", 0);
        BOOL relayRecent = (lastRelayUse != 0) && (now - lastRelayUse) < 30;
        BOOL sessionRecent = (HidLastActive != nil) && [HidLastActive timeIntervalSinceNow] > -30.0;
        BOOL show = relayRecent || sessionRecent;
        HidPillView.hidden = !show;
        if (!show) {
            return;
        }
        NSInteger used = RelayPrefInt(@"lastTurnsUsed", 0);
        NSInteger cap = RelayPrefInt(@"relayMaxTurns", (long)kForwardTurns);
        ((UILabel *)HidPillView).text = [NSString stringWithFormat:@"%ld 轮 / %ld",
                                         (long)used, (long)cap];
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay overlay refresh failed: %{public}s",
                     e.reason.UTF8String ?: "?");
    }
}

+ (void)handleLongPress:(UILongPressGestureRecognizer *)gesture {
    if (gesture.state != UIGestureRecognizerStateBegan) {
        return;
    }
    [self showSettings];
}

+ (void)showSettings {
    @try {
        UIWindow *win = [UIApplication sharedApplication].keyWindow;
        if (win == nil) {
            return;
        }
        [self removeSettings];

        CGFloat width = 300, height = 224;
        UIView *sheet = [[UIView alloc]
            initWithFrame:CGRectMake((win.bounds.size.width - width) / 2.0,
                                     (win.bounds.size.height - height) / 2.0,
                                     width, height)];
        sheet.backgroundColor = [UIColor colorWithWhite:0.22 alpha:0.82];  // 半透明
        sheet.layer.cornerRadius = 16;
        sheet.clipsToBounds = YES;

        UILabel *title = [UILabel new];
        title.text = @"转发历史轮数上限";
        title.textColor = UIColor.whiteColor;
        title.font = [UIFont systemFontOfSize:15 weight:UIFontWeightSemibold];
        title.textAlignment = NSTextAlignmentCenter;
        title.frame = CGRectMake(16, 14, width - 32, 22);
        [sheet addSubview:title];

        UILabel *hint = [UILabel new];
        hint.text = @"范围 10–200,可拖动滑杆或直接输入";
        hint.textColor = [UIColor colorWithWhite:0.9 alpha:0.85];
        hint.font = [UIFont systemFontOfSize:12];
        hint.textAlignment = NSTextAlignmentCenter;
        hint.frame = CGRectMake(12, 40, width - 24, 16);
        [sheet addSubview:hint];

        UISlider *slider = [UISlider new];
        slider.minimumValue = 10;
        slider.maximumValue = 200;
        long current = RelayPrefInt(@"relayMaxTurns", (long)kForwardTurns);
        if (current < 10) current = 10;
        if (current > 200) current = 200;
        slider.value = (float)current;
        slider.frame = CGRectMake(20, 62, width - 40, 30);
        [slider addTarget:self action:@selector(sliderChanged:)
           forControlEvents:UIControlEventValueChanged];
        [sheet addSubview:slider];

        UITextField *field = [UITextField new];
        field.frame = CGRectMake((width - 120) / 2.0, 100, 120, 34);
        field.keyboardType = UIKeyboardTypeNumberPad;
        field.text = [NSString stringWithFormat:@"%ld", current];
        field.textColor = UIColor.whiteColor;
        field.textAlignment = NSTextAlignmentCenter;
        field.font = [UIFont monospacedDigitSystemFontOfSize:16 weight:UIFontWeightSemibold];
        field.backgroundColor = [UIColor colorWithWhite:0.0 alpha:0.35];
        field.layer.cornerRadius = 8;
        field.clipsToBounds = YES;
        field.tag = 0x5843;
        [field addTarget:self action:@selector(fieldChanged:)
           forControlEvents:UIControlEventEditingChanged];
        [sheet addSubview:field];

        UIButton *done = [UIButton buttonWithType:UIButtonTypeCustom];
        done.frame = CGRectMake(0, height - 46, width, 46);
        [done setTitle:@"完成" forState:UIControlStateNormal];
        [done setTitleColor:UIColor.whiteColor forState:UIControlStateNormal];
        done.titleLabel.font = [UIFont systemFontOfSize:15 weight:UIFontWeightSemibold];
        [done addTarget:self action:@selector(doneTapped:) forControlEvents:UIControlEventTouchUpInside];
        [sheet addSubview:done];

        [win addSubview:sheet];
        [win bringSubviewToFront:sheet];
        HidSettingsSheet = sheet;
        [field becomeFirstResponder];
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay overlay settings failed: %{public}s",
                     e.reason.UTF8String ?: "?");
    }
}

+ (long)clamp:(long)value low:(long)low high:(long)high {
    if (value < low) return low;
    if (value > high) return high;
    return value;
}

+ (UITextField *)settingsFieldIn:(UIView *)view {
    UITextField *field = [view viewWithTag:0x5843];
    return [field isKindOfClass:[UITextField class]] ? field : nil;
}

+ (void)syncFieldFromSlider:(UISlider *)slider {
    if (slider.superview == nil) {
        return;
    }
    UITextField *field = [self settingsFieldIn:slider.superview];
    if (field != nil) {
        field.text = [NSString stringWithFormat:@"%ld", (long)lround(slider.value)];
    }
}

+ (void)sliderChanged:(UISlider *)slider {
    @try {
        long value = [self clamp:(long)lround(slider.value) low:10 high:200];
        RelayPrefSetInt(@"relayMaxTurns", value);
        [self syncFieldFromSlider:slider];
        [self refresh];
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay overlay slider failed: %{public}s",
                     e.reason.UTF8String ?: "?");
    }
}

+ (void)fieldChanged:(UITextField *)field {
    @try {
        long value = [field.text longLongValue];
        if (value >= 10 && value <= 200) {
            RelayPrefSetInt(@"relayMaxTurns", value);
        }
        if (field.superview != nil) {
            UISlider *slider = [self sliderIn:field.superview];
            if (slider != nil) {
                slider.value = (float)[self clamp:value low:10 high:200];
            }
        }
        [self refresh];
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay overlay field failed: %{public}s",
                     e.reason.UTF8String ?: "?");
    }
}

+ (UISlider *)sliderIn:(UIView *)view {
    for (UIView *sub in view.subviews) {
        if ([sub isKindOfClass:[UISlider class]]) {
            return (UISlider *)sub;
        }
    }
    return nil;
}

+ (void)doneTapped:(id)sender {
    @try {
        UIView *sheet = HidSettingsSheet;
        if (sheet != nil) {
            UITextField *field = [self settingsFieldIn:sheet];
            if (field != nil) {
                long value = [self clamp:[field.text longLongValue] low:10 high:200];
                RelayPrefSetInt(@"relayMaxTurns", value);
            }
            [field resignFirstResponder];
        }
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay overlay done failed: %{public}s",
                     e.reason.UTF8String ?: "?");
    }
    [self removeSettings];
    [self refresh];
}

+ (void)removeSettings {
    if (HidSettingsSheet != nil) {
        [HidSettingsSheet removeFromSuperview];
        HidSettingsSheet = nil;
    }
}

@end

__attribute__((constructor)) static void TweakInitialize(void) {
    // Apelin's confirm_loaded looks for this exact marker and build id under
    // the package id's subsystem. Keep the line, and keep it public.
    os_log(RelayLog(), "ApelinLoaded %{public}s in %{public}s", APELIN_BUILD_ID, getprogname());

    @try {
        static dispatch_once_t once;
        dispatch_once(&once, ^{
            // Same filter, two roles: SpringBoard hosts the relay socket;
            // Apelin hosts the translucent turn-count pill.
            NSString *bundleID = [NSBundle mainBundle].bundleIdentifier ?: @"";
            if ([bundleID isEqualToString:@"com.apple.springboard"] ||
                [bundleID isEqualToString:@"SpringBoard"]) {
                RelayStartListener();
            } else if ([bundleID isEqualToString:@"wiki.qaq.apelin"]) {
                // Durable "we actually injected into Apelin" proof for this chat
                // to read back without depending on log timing.
                RelayPrefSetInt(@"apelinLoaded", 1);
                [HidOverlay installHooks];
                [HidOverlay installAfterDelay];
            }
        });
    } @catch (NSException *e) {
        os_log_error(RelayLog(), "relay start exception: %{public}s", e.reason.UTF8String ?: "?");
    }
}
