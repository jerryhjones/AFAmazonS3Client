//
// AFAmazonS3Client.m
//
// Copyright (c) 2012 Mattt Thompson (http://mattt.me/)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#import "AFAmazonS3Client.h"
#import "AFXMLRequestOperation.h"
#import <CommonCrypto/CommonDigest.h>
#import <CommonCrypto/CommonHMAC.h>
#import <CommonCrypto/CommonCryptor.h>

NSString * const kAFAmazonS3BaseURLString = @"http://s3.amazonaws.com";
NSString * const kAFAmazonS3BucketBaseURLFormatString = @"http://%@.s3.amazonaws.com";

static NSString * AFBase64EncodedStringFromData(NSData *data)
{
	static uint8_t const kAFBase64EncodingTable[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	
    NSMutableString *result;
    unsigned char   *raw;
    unsigned long   length;
    short           i, nCharsToWrite;
    long            cursor;
    unsigned char   inbytes[3], outbytes[4];
	
    length = [data length];
	
    if (length < 1) {
        return @"";
    }
	
    result = [NSMutableString stringWithCapacity:length];
    raw    = (unsigned char *)[data bytes];
    // Take 3 chars at a time, and encode to 4
    for (cursor = 0; cursor < length; cursor += 3) {
        for (i = 0; i < 3; i++) {
            if (cursor + i < length) {
                inbytes[i] = raw[cursor + i];
            }
            else{
                inbytes[i] = 0;
            }
        }
		
        outbytes[0] = (inbytes[0] & 0xFC) >> 2;
        outbytes[1] = ((inbytes[0] & 0x03) << 4) | ((inbytes[1] & 0xF0) >> 4);
        outbytes[2] = ((inbytes[1] & 0x0F) << 2) | ((inbytes[2] & 0xC0) >> 6);
        outbytes[3] = inbytes[2] & 0x3F;
		
        nCharsToWrite = 4;
		
        switch (length - cursor) {
			case 1:
				nCharsToWrite = 2;
				break;
				
			case 2:
				nCharsToWrite = 3;
				break;
        }
        for (i = 0; i < nCharsToWrite; i++) {
            [result appendFormat:@"%c", kAFBase64EncodingTable[outbytes[i]]];
        }
        for (i = nCharsToWrite; i < 4; i++) {
            [result appendString:@"="];
        }
    }
	
    return result; // convert to immutable string
}

#pragma mark -

@interface AFAmazonS3Client ()
@property (readwrite, nonatomic, copy) NSString *accessKey;
@property (readwrite, nonatomic, copy) NSString *secret;

- (void)setObjectWithMethod:(NSString *)method
                       file:(NSString *)path
                 parameters:(NSDictionary *)parameters
                   progress:(void (^)(NSInteger bytesWritten, long long totalBytesWritten, long long totalBytesExpectedToWrite))progressBlock
                    success:(void (^)(id responseObject))success
                    failure:(void (^)(NSError *error))failure;
- (void)signRequest:(NSMutableURLRequest *)request forObjectNamed:(NSString *)objectName;

@end

@implementation AFAmazonS3Client
@synthesize baseURL = _s3_baseURL;
@synthesize bucket = _bucket;
@synthesize accessKey = _accessKey;
@synthesize secret = _secret;
@synthesize dateFormatter = _dateFormatter;

- (id)initWithBaseURL:(NSURL *)url {
    self = [super initWithBaseURL:url];
    if (!self) {
        return nil;
    }

    [self registerHTTPOperationClass:[AFXMLRequestOperation class]];

    return self;
}

- (id)initWithAccessKeyID:(NSString *)accessKey
                   secret:(NSString *)secret
{
    self = [self initWithBaseURL:[NSURL URLWithString:kAFAmazonS3BaseURLString]];
    if (!self) {
        return nil;
    }

    self.accessKey = accessKey;
    self.secret = secret;

    return self;
}

- (NSURL *)baseURL {
    if (_s3_baseURL && self.bucket) {
		NSString *urlString = [NSString stringWithFormat:kAFAmazonS3BucketBaseURLFormatString, self.bucket];
		NSURL *url = [NSURL URLWithString:urlString];
        return url;
    }

    return _s3_baseURL;
}

- (void)setBucket:(NSString *)bucket {
	NSString *lowercaseBucket = [bucket lowercaseString];
	if ([lowercaseBucket isEqualToString:_bucket]) {
		return;
	}
	
	[_bucket release];
	
    [self willChangeValueForKey:@"baseURL"];
    [self willChangeValueForKey:@"bucket"];
    _bucket = [lowercaseBucket retain];
    [self didChangeValueForKey:@"bucket"];
    [self didChangeValueForKey:@"baseURL"];
}

- (NSDateFormatter *)dateFormatter
{
	if (nil != _dateFormatter) {
		return _dateFormatter;
	}
	
	_dateFormatter = [[NSDateFormatter alloc] init];
    [_dateFormatter setDateFormat:@"EEE, dd MMM yyyy HH:mm:ss z"];
	NSLocale *local =[[[NSLocale alloc] initWithLocaleIdentifier:@"en_US"] autorelease];
    [_dateFormatter setLocale:local];

	return _dateFormatter;
}

#pragma mark -

- (void)enqueueS3RequestOperationWithMethod:(NSString *)method
                                       path:(NSString *)path
                                 parameters:(NSDictionary *)parameters
                                    success:(void (^)(id responseObject))success
                                    failure:(void (^)(NSError *error))failure
{
    NSURLRequest *request = [self requestWithMethod:method path:path parameters:parameters];
	
    AFHTTPRequestOperation *requestOperation = [self HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, id responseObject) {
        if (success) {
            success(responseObject);
        }
    } failure:^(AFHTTPRequestOperation *operation, NSError *error) {
        if (failure) {
            failure(error);
        }
    }];

    [self enqueueHTTPRequestOperation:requestOperation];
}

#pragma mark - Service Operations

- (void)getServiceWithSuccess:(void (^)(id responseObject))success
                      failure:(void (^)(NSError *error))failure
{
    [self enqueueS3RequestOperationWithMethod:@"GET" path:@"/" parameters:nil success:success failure:failure];
}

#pragma mark - Bucket Operations

- (void)getBucket:(NSString *)bucket
          success:(void (^)(id responseObject))success
          failure:(void (^)(NSError *error))failure
{
    [self enqueueS3RequestOperationWithMethod:@"GET" path:bucket parameters:nil success:success failure:failure];
}

- (void)putBucket:(NSString *)bucket
       parameters:(NSDictionary *)parameters
          success:(void (^)(id responseObject))success
          failure:(void (^)(NSError *error))failure
{
    [self enqueueS3RequestOperationWithMethod:@"PUT" path:bucket parameters:parameters success:success failure:failure];

}

- (void)deleteBucket:(NSString *)bucket
             success:(void (^)(id responseObject))success
             failure:(void (^)(NSError *error))failure
{
    [self enqueueS3RequestOperationWithMethod:@"DELETE" path:bucket parameters:nil success:success failure:failure];
}

#pragma mark - Object Operations

- (void)headObjectWithPath:(NSString *)path
                   success:(void (^)(id responseObject))success
                   failure:(void (^)(NSError *error))failure
{
    [self enqueueS3RequestOperationWithMethod:@"HEAD" path:path parameters:nil success:success failure:failure];
}

- (void)getObjectWithPath:(NSString *)path
                 progress:(void (^)(NSInteger bytesRead, long long totalBytesRead, long long totalBytesExpectedToRead))progress
                  success:(void (^)(id responseObject, NSData *responseData))success
                  failure:(void (^)(NSError *error))failure
{
    NSURLRequest *request = [self requestWithMethod:@"GET" path:path parameters:nil];
    AFHTTPRequestOperation *requestOperation = [self HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, id responseObject) {
        if (success) {
            success(responseObject, operation.responseData);
        }
    } failure:^(AFHTTPRequestOperation *operation, NSError *error) {
        if (failure) {
            failure(error);
        }
    }];

    [requestOperation setUploadProgressBlock:progress];

    [self enqueueHTTPRequestOperation:requestOperation];
}

- (void)getObjectWithPath:(NSString *)path
             outputStream:(NSOutputStream *)outputStream
                 progress:(void (^)(NSInteger bytesRead, long long totalBytesRead, long long totalBytesExpectedToRead))progress
                  success:(void (^)(id responseObject))success
                  failure:(void (^)(NSError *error))failure
{
    NSURLRequest *request = [self requestWithMethod:@"GET" path:path parameters:nil];
	NSURLRequest *signedRequest = [self signedRequestWithRequest:request];
    AFHTTPRequestOperation *requestOperation = [self HTTPRequestOperationWithRequest:signedRequest
																			 success:^(AFHTTPRequestOperation *operation, id responseObject) {
																				 if (success) {
																					 success(responseObject);
																				 }
																			 } failure:^(AFHTTPRequestOperation *operation, NSError *error) {
																				 if (failure) {
																					 failure(error);
																				 }
																			 }];

    [requestOperation setUploadProgressBlock:progress];
    [requestOperation setOutputStream:outputStream];

    [self enqueueHTTPRequestOperation:requestOperation];
}

- (void)postObjectWithFile:(NSString *)path
                parameters:(NSDictionary *)parameters
                  progress:(void (^)(NSInteger bytesWritten, long long totalBytesWritten, long long totalBytesExpectedToWrite))progress
                   success:(void (^)(id responseObject))success
                   failure:(void (^)(NSError *error))failure
{
    [self setObjectWithMethod:@"POST" file:path parameters:parameters progress:progress success:success failure:failure];
}

- (void)putObjectWithFile:(NSString *)path
               parameters:(NSDictionary *)parameters
                 progress:(void (^)(NSInteger bytesWritten, long long totalBytesWritten, long long totalBytesExpectedToWrite))progress
                  success:(void (^)(id responseObject))success
                  failure:(void (^)(NSError *error))failure
{
    [self setObjectWithMethod:@"PUT" file:path parameters:parameters progress:progress success:success failure:failure];
}

- (void)putObjectNamed:(NSString *)objectName
				  data:(NSData *)data
			parameters:(NSDictionary *)parameters
			  progress:(void (^)(NSInteger bytesWritten, long long totalBytesWritten, long long totalBytesExpectedToWrite))progress
			   success:(void (^)(id responseObject))success
			   failure:(void (^)(NSError *error))failure
{
	if (nil == data) {
		if (failure) {
			NSError *error = nil;
			failure(error);
		}
		
		return;
	}
	
	NSMutableURLRequest *request = [self requestWithMethod:@"PUT" path:objectName parameters:parameters];
	[request setHTTPBody:data];
	
	[self signRequest:request forObjectNamed:objectName];
	
	AFHTTPRequestOperation *requestOperation = [self HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, id responseObject) {
		if (success) {
			success(responseObject);
		}
	} failure:^(AFHTTPRequestOperation *operation, NSError *error) {
		if (failure) {
			failure(error);
		}
	}];
	
	[requestOperation setUploadProgressBlock:progress];
	
	[self enqueueHTTPRequestOperation:requestOperation];
}

- (void)deleteObjectWithPath:(NSString *)path
                     success:(void (^)(id responseObject))success
                     failure:(void (^)(NSError *error))failure
{
    [self enqueueS3RequestOperationWithMethod:@"DELETE" path:path parameters:nil success:success failure:failure];
}

- (void)setObjectWithMethod:(NSString *)method
                       file:(NSString *)path
        parameters:(NSDictionary *)parameters
                   progress:(void (^)(NSInteger bytesWritten, long long totalBytesWritten, long long totalBytesExpectedToWrite))progress
                    success:(void (^)(id responseObject))success
                    failure:(void (^)(NSError *error))failure
{
    NSMutableURLRequest *fileRequest = [NSMutableURLRequest requestWithURL:[NSURL fileURLWithPath:path]];
    [fileRequest setCachePolicy:NSURLCacheStorageNotAllowed];

    NSURLResponse *response = nil;
    NSError *error = nil;
    NSData *data = [NSURLConnection sendSynchronousRequest:fileRequest returningResponse:&response error:&error];

    if (data && response) {
        NSMutableURLRequest *request = [self multipartFormRequestWithMethod:method path:path parameters:parameters constructingBodyWithBlock:^(id<AFMultipartFormData> formData) {
            [formData appendPartWithFileData:data name:@"file" fileName:[path lastPathComponent] mimeType:[response MIMEType]];
        }];

        AFHTTPRequestOperation *requestOperation = [self HTTPRequestOperationWithRequest:request success:^(AFHTTPRequestOperation *operation, id responseObject) {
            if (success) {
                success(responseObject);
            }
        } failure:^(AFHTTPRequestOperation *operation, NSError *error) {
            if (failure) {
                failure(error);
            }
        }];

        [requestOperation setUploadProgressBlock:progress];

        [self enqueueHTTPRequestOperation:requestOperation];
    }
}

- (NSMutableURLRequest *)signedRequestWithRequest:(NSURLRequest *)request
{
	NSMutableURLRequest *mutableRequest = [[request mutableCopy] autorelease];
	[self signRequest:mutableRequest forObjectNamed:nil];
	return mutableRequest;
}

- (void)signRequest:(NSMutableURLRequest *)request forObjectNamed:(NSString *)objectName
{
	// Set expected date header with proper format
	NSString *dateString = [self.dateFormatter stringFromDate:[NSDate date]];
	
	[request setValue:dateString forHTTPHeaderField:@"Date"];
	
	NSString *contentMd5  = [request valueForHTTPHeaderField:@"Content-MD5"];
	NSString *contentType = [request valueForHTTPHeaderField:@"Content-Type"];
	NSString *timestamp   = [request valueForHTTPHeaderField:@"Date"];
	
	if (nil == contentMd5) {
		contentMd5 = @"";
	}
	if (nil == contentType) {
		contentType = @"";
	}
	
	NSMutableString *canonicalizedAmzHeaders = [NSMutableString stringWithFormat:@""];
	NSString *path = objectName;
	if (nil == path) {
		path = [[request URL] path];
		path = [path stringByReplacingOccurrencesOfString:@"/" withString:@"" options:0 range:NSMakeRange(0, 1)];
	}
	
	NSString *canonicalizedResource = [NSString stringWithFormat:@"/%@/%@", self.bucket, path];
	NSString *query = request.URL.query;
	
	if (query != nil && [query length] > 0) {
		canonicalizedResource = [canonicalizedResource stringByAppendingFormat:@"?%@", [query stringByReplacingPercentEscapesUsingEncoding:NSUTF8StringEncoding]];
	}
	
	NSString *stringToSign = [NSString stringWithFormat:@"%@\n%@\n%@\n%@\n%@%@", [request HTTPMethod], contentMd5, contentType, timestamp, canonicalizedAmzHeaders, canonicalizedResource];
	
	NSData *stringData = [stringToSign dataUsingEncoding:NSASCIIStringEncoding];
	CCHmacContext context;
    const char *secretCString = [self.secret cStringUsingEncoding:NSASCIIStringEncoding];
	
    CCHmacInit(&context, kCCHmacAlgSHA1, secretCString, strlen(secretCString));
    CCHmacUpdate(&context, [stringData bytes], [stringData length]);
	
    // Both SHA1 and SHA256 will fit in here
    unsigned char digestRaw[CC_SHA256_DIGEST_LENGTH];
	
    CCHmacFinal(&context, digestRaw);
	
    NSData *digestData = [NSData dataWithBytes:digestRaw length:CC_SHA1_DIGEST_LENGTH];
	NSString *base64SignatureString = AFBase64EncodedStringFromData(digestData);
	
	[request setValue:[NSString stringWithFormat:@"AWS %@:%@", self.accessKey, base64SignatureString] forHTTPHeaderField:@"Authorization"];
}

@end
