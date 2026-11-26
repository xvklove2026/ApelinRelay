// varianceprobe.m — N direct upstream calls, reporting when response headers
// arrived, when the first text delta arrived, and the total. Used to pick a
// sensible "give up and retry" deadline for the relay.
//   varianceprobe <count> <timeoutSec> [low]
#import <Foundation/Foundation.h>

static NSString *const kKey =
    @"sk-46e396dad8763b5a6e460c7b1f3b9ebc02e8c8de6a930f53eaa0ca433ad4a481";

@interface V : NSObject <NSURLSessionDataDelegate>
@property (nonatomic) double t0;
@property (nonatomic) long status;
@property (nonatomic) double tStatus, tText;
@property (nonatomic) BOOL gotStatus, gotText;
@property (nonatomic) int bytes;
@property (nonatomic, strong) dispatch_semaphore_t done;
@end

@implementation V
- (instancetype)init { self = [super init]; _tStatus = -1; _tText = -1; return self; }
- (void)URLSession:(NSURLSession *)s dataTask:(NSURLSessionDataTask *)d
    didReceiveResponse:(NSURLResponse *)r
     completionHandler:(void (^)(NSURLSessionResponseDisposition))c {
    if ([r isKindOfClass:[NSHTTPURLResponse class]]) self.status = ((NSHTTPURLResponse *)r).statusCode;
    if (!self.gotStatus) { self.gotStatus = YES; self.tStatus = CFAbsoluteTimeGetCurrent() - self.t0; }
    c(NSURLSessionResponseAllow);
}
- (void)URLSession:(NSURLSession *)s dataTask:(NSURLSessionDataTask *)d didReceiveData:(NSData *)data {
    self.bytes += (int)data.length;
    if (self.gotText) return;
    NSString *c = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
    if (c && [c containsString:@"output_text.delta"]) {
        self.gotText = YES; self.tText = CFAbsoluteTimeGetCurrent() - self.t0;
    }
}
- (void)URLSession:(NSURLSession *)s task:(NSURLSessionTask *)t didCompleteWithError:(NSError *)e {
    if (self.done) dispatch_semaphore_signal(self.done);
}
@end

int main(int argc, char **argv) {
    @autoreleasepool {
        int count = argc > 1 ? atoi(argv[1]) : 4;
        double timeout = argc > 2 ? atof(argv[2]) : 45.0;
        NSString *variant = argc > 3 ? [NSString stringWithUTF8String:argv[3]] : @"baseline";
        NSInteger turns = argc > 4 ? atoi(argv[4]) : 0;
        NSMutableArray *history = [NSMutableArray array];
        for (NSInteger i = 1; i <= turns; i++) {
            [history addObject:@{@"role": @"user", @"content":
                [NSString stringWithFormat:@"Historical request %ld about a small topic", (long)i]}];
            [history addObject:@{@"role": @"assistant", @"content":
                [NSString stringWithFormat:@"Historical answer %ld, a couple of sentences of padding.", (long)i]}];
        }
        [history addObject:@{@"role": @"user", @"content":
            @"Write a Swift function that reverses a string. Output code only."}];
        NSString *prompt = @"Write a Swift function that reverses a string. Output code only.";

        int n = count > 64 ? 64 : count;
        for (int i = 0; i < n; i++) {
            NSMutableDictionary *body = [@{
                @"model": @"gpt-6.1-sol",
                @"input": turns > 0 ? history : @[ @{@"role": @"user", @"content": prompt} ],
                @"stream": @YES,
                @"max_output_tokens": @1024,
            } mutableCopy];
            if ([variant isEqualToString:@"store"]) body[@"store"] = @NO;
            else if ([variant isEqualToString:@"key"]) body[@"prompt_cache_key"] = @"relay-session";
            else if ([variant isEqualToString:@"low"]) body[@"reasoning"] = @{@"effort": @"low"};
            else if ([variant isEqualToString:@"minimal"]) body[@"reasoning"] = @{@"effort": @"minimal"};
            NSData *json = [NSJSONSerialization dataWithJSONObject:body options:0 error:NULL];

            NSMutableURLRequest *req = [NSMutableURLRequest requestWithURL:
                [NSURL URLWithString:@"https://api.xingchiapi.com/v1/responses"]];
            req.HTTPMethod = @"POST";
            req.timeoutInterval = timeout;
            [req setValue:@"application/json" forHTTPHeaderField:@"Content-Type"];
            [req setValue:[NSString stringWithFormat:@"Bearer %@", kKey] forHTTPHeaderField:@"Authorization"];
            [req setValue:@"Minis/1.0" forHTTPHeaderField:@"User-Agent"];
            req.HTTPBody = json;

            V *v = [V new];
            v.t0 = CFAbsoluteTimeGetCurrent();
            v.done = dispatch_semaphore_create(0);
            NSOperationQueue *q = [NSOperationQueue new]; q.maxConcurrentOperationCount = 1;
            NSURLSessionConfiguration *cfg = [NSURLSessionConfiguration ephemeralSessionConfiguration];
            cfg.timeoutIntervalForRequest = timeout; cfg.timeoutIntervalForResource = timeout;
            NSURLSession *session = [NSURLSession sessionWithConfiguration:cfg delegate:v delegateQueue:q];
            NSURLSessionDataTask *task = [session dataTaskWithRequest:req];
            [task resume];

            long timedOut = dispatch_semaphore_wait(v.done,
                dispatch_time(DISPATCH_TIME_NOW, (int64_t)((timeout + 10) * NSEC_PER_SEC)));
            if (timedOut != 0) [task cancel];
            double total = CFAbsoluteTimeGetCurrent() - v.t0;
            printf("#%d status=%3ld  statusAt=%6.1fs  textAt=%6.1fs  total=%6.1fs  bytes=%6d%s\n",
                   i + 1, v.status,
                   v.gotStatus ? v.tStatus : -1,
                   v.gotText ? v.tText : -1,
                   total, v.bytes,
                   timedOut != 0 ? "  (HUNG)" : "");
            fflush(stdout);
            [session finishTasksAndInvalidate];
        }
    }
    return 0;
}