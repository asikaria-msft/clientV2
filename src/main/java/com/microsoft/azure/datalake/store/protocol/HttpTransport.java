package com.microsoft.azure.datalake.store.protocol;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.datalake.store.AzureDataLakeStorageClient;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.UUID;


/**
 *
 * The core class that does the actual network communication. All the REST methods
 * use this class to make HTTP calls.
 * <P>
 *     There are two calls in this class:
 *     makeSingleCall - this makes an HTTP request.
 *     makeCall - wraps retries around makeSingleCall
 * </P>
 */
class HttpTransport {

    private static final String API_VERSION = "2015-10-01-preview"; // API version used in REST requests

    /**
     * calls {@link #makeSingleCall(AzureDataLakeStorageClient, Operation, String, QueryParams, byte[], int, int, RequestOptions, OperationResponse) makeSingleCall}
     * in a retry loop. The retry policies are dictated by the {@link com.microsoft.azure.datalake.store.retrypolicies.RetryPolicy RetryPolicy} passed in.
     *
     * @param client the the {@link AzureDataLakeStorageClient}
     * @param op the WebHDFS operation tp perform
     * @param path the path to operate on
     * @param queryParams query parameter names and values to include on the URL of the request
     * @param requestBody the body of the request, if applicable. can be {@code null}
     * @param offsetWithinContentsArray offset within the byte array passed in {@code requestBody}. Bytes starting
     *                                  at this offset will be sent to server
     * @param length number of bytes from {@code requestBody} to be sent
     * @param opts options to change the behavior of the call
     * @param resp response from the call, and any error info generated by the call
     */
    public static void makeCall (AzureDataLakeStorageClient client,
                                       Operation op,
                                       String path,
                                       QueryParams queryParams,
                                       byte[] requestBody,
                                       int offsetWithinContentsArray,
                                       int length,
                                       RequestOptions opts,
                                       OperationResponse resp
                                       )
    {
        if (opts.retryPolicy == null) {
            opts.retryPolicy = new NoRetryPolicy();
        }
        if (opts.requestid == null) {
            opts.requestid = UUID.randomUUID().toString();
        }

        int retryCount = 0;
        do {
            long start = System.nanoTime();
            makeSingleCall(client, op, path, queryParams, requestBody, offsetWithinContentsArray, length, opts, resp);
            resp.lastCallLatency = System.nanoTime() - start;
            resp.numRetries = retryCount;
            if (isSuccessfulResponse(resp, op)) {
                resp.successful = true;
                LatencyTracker.addLatency(opts.requestid, retryCount, resp.lastCallLatency, op.name, length + resp.responseContentLength);
                return;
            } else {
                resp.successful = false;
                String error;
                if (resp.ex!=null) {
                    error = resp.ex.getClass().getName();
                } else {
                    error = "HTTP" + resp.httpResponseCode;
                }
                LatencyTracker.addError(opts.requestid, retryCount, error, op.name, length);
                retryCount++;
            }
        } while (opts.retryPolicy.shouldRetry(resp.httpResponseCode, resp.ex));
    }

    private static boolean isSuccessfulResponse(OperationResponse resp, Operation op) {
        if (resp.ex != null) return false;
        if (resp.httpResponseCode >=100 && resp.httpResponseCode < 300) return true; // 1xx and 2xx return codes
        if  (    (op == Operation.OPEN)
              && (resp.httpResponseCode == 403 || resp.httpResponseCode == 416)      // EOF for OPEN
            ) {
            return true;
        }
        return false;         //anything else
    }

    /**
     * Does the actual HTTP call to server. All REST API calls use this method to make their HTTP calls.
     * <P>
     * This is a static, stateless, thread-safe method.
     * </P>
     *
     * @param client the the {@link AzureDataLakeStorageClient}
     * @param op the WebHDFS operation tp perform
     * @param path the path to operate on
     * @param queryParams query parameter names and values to include on the URL of the request
     * @param requestBody the body of the request, if applicable. can be {@code null}
     * @param offsetWithinContentsArray offset within the byte array passed in {@code requestBody}. Bytes starting
     *                                  at this offset will be sent to server
     * @param length number of bytes from {@code requestBody} to be sent
     * @param opts options to change the behavior of the call
     * @param resp response from the call, and any error info generated by the call
     */
    private static void makeSingleCall(AzureDataLakeStorageClient client,
                                            Operation op,
                                            String path,
                                            QueryParams queryParams,
                                            byte[] requestBody,
                                            int offsetWithinContentsArray,
                                            int length,
                                            RequestOptions opts,
                                            OperationResponse resp) {
        if (client == null || client.getAccountName().equals("") || client.getAccessToken().equals("") ) {
            // TODO: log this
            return;
        }

        if (op == null) {
            // TODO: Log this
            return;
        }

        if (path == null || path.trim().equals("")) {
            // TODO: log this
            return;
        }

        if (    requestBody != null ) {
            if (    offsetWithinContentsArray < 0 ||
                    length < 0 ||
                    offsetWithinContentsArray + length < 0 || // integer overflow
                    offsetWithinContentsArray >= requestBody.length ||
                    offsetWithinContentsArray + length > requestBody.length) {
                throw new IndexOutOfBoundsException();
            }
        } else {
            if (offsetWithinContentsArray != 0 || length != 0) {
                throw new IndexOutOfBoundsException();
            }
        }

        if (path.charAt(0) != '/') path = "/" + path;

        if (queryParams == null) queryParams = new QueryParams();

        // Build URL
        StringBuilder urlString = new StringBuilder();
        urlString.append("https://");
        urlString.append(client.getAccountName());
        if (op.isExt) {
            urlString.append("/WebHdfsExt");
        } else {
            urlString.append("/webhdfs/v1");
        }

        try {
            // URL encode, but keep the "/" characters
            urlString.append(URLEncoder.encode(path, "UTF-8").replace("%2F", "/").replace("%2f", "/"));
        } catch (UnsupportedEncodingException ex) {}
        urlString.append('?');

        queryParams.setOp(op);
        queryParams.setApiVersion(API_VERSION);
        urlString.append(queryParams.serialize());
        try {
            // Setup Http Request (method and headers)
            URL url = new URL(urlString.toString());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Authorization", client.getAccessToken());
            // TODO: It is wasteful to recompute User-Agent on every call. Move it to adlclient and reuse same string from there
            conn.setRequestProperty("User-Agent", getUserAgent(client.getUserAgentSuffix()));
            conn.setRequestProperty("x-ms-client-request-id", opts.requestid);
            String latencyHeader = LatencyTracker.get();
            if (latencyHeader!=null) conn.setRequestProperty("x-ms-adl-client-latency", latencyHeader);
            conn.setConnectTimeout(opts.timeout);
            conn.setReadTimeout(opts.timeout);
            conn.setUseCaches(false);
            conn.setRequestMethod(op.method);
            conn.setDoInput(true);

            // populate request body if applicable
             if (!op.method.equals("GET")) {
                conn.setDoOutput(true);
                conn.setRequestMethod(op.method);
                OutputStream s = conn.getOutputStream();
                if (op.requiresBody && requestBody != null) {
                    s.write(requestBody, offsetWithinContentsArray, length);
                    s.close();
                } else {
                    // server *requires* a Content-Length header, and doesnt take absence of header as 0 (bad behavior)
                    // The only way to force java to send "Content-Length:0" is to do this.
                    // Setting Content-Length header to 0 using setRequestProprty doesnt work (bad behavior)
                    byte[] b = new byte[]{};  // zero-length byte-array
                    s.write(b);
                    s.close();
                }
            }

            // get Response Stream if applicable
            resp.httpResponseCode = conn.getResponseCode();
            resp.httpResponseMessage = conn.getResponseMessage();
            resp.requestId = conn.getHeaderField("x-ms-request-id");
            resp.responseContentLength = conn.getHeaderFieldLong("Content-Length", 0);
            String chunked = conn.getHeaderField("Transfer-Encoding");
            if (chunked != null && chunked.equals("chunked")) resp.responseChunked = true;

            // if request failed, then the body of an HTTP 4xx or 5xx response contains erro info as JSon
            if (resp.httpResponseCode >= 400) {
                if (resp.responseContentLength > 0 && conn.getErrorStream() != null) {
                    getCodesFromJSon(conn.getErrorStream(), resp);
                    return;
                }
            }

            // read response body if applicable
            if (op.returnsBody) {
                resp.responseStream = conn.getInputStream();
            }
        } catch (MalformedURLException ex) {
            resp.ex = ex;
        } catch (IOException ex) {
            resp.ex = ex;
        }
    }

    private static void getCodesFromJSon(InputStream s, OperationResponse resp) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(resp.responseStream);

            JsonNode remoteExceptionNode = rootNode.path("RemoteException");
            resp.remoteExceptionName = remoteExceptionNode.path("exception").asText();
            resp.remoteExceptionMessage = remoteExceptionNode.path("message").asText();
            resp.remoteExceptionJavaClassName = remoteExceptionNode.path("javaClassName").asText();
        } catch (IOException ex) {}
    }


    private static String userAgent =
            String.format("%s-%s/%s-%s/%s/%s-%s",
                    "ADLSJavaSDK",
                    HttpTransport.class.getPackage().getImplementationVersion(), // SDK Version
                    System.getProperty("os.name").replaceAll(" ", ""),
                    System.getProperty("os.version"),
                    System.getProperty("os.arch"),
                    System.getProperty("java.vendor").replaceAll(" ", ""),
                    System.getProperty("java.version")
            );

    private static String getUserAgent(String customSuffix) {

        if (customSuffix != null && !customSuffix.trim().equals("")) {
            return userAgent + " / " + customSuffix;
        } else {
            return userAgent;
        }
    }
}
