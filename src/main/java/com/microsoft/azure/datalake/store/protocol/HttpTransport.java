package com.microsoft.azure.datalake.store.protocol;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.datalake.store.AzureDataLakeStorageClient;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.UUID;

// TODO: Look again at the timeout implementation -- seems inadequate
// TODO: See if returning InputStream of a response does not lead to resource leak, esp of HttpUrlConnection objects

class HttpTransport {

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
            makeSingleCall(client, op, path, queryParams, requestBody, offsetWithinContentsArray, length, opts, resp);  // THE REAL CALL
            resp.lastCallLatency = System.nanoTime() - start;
            resp.numRetries = retryCount;
            if (    (resp.ex == null)
                 && (resp.httpResponseCode >=100 && resp.httpResponseCode < 300) ) {   // 1xx and 2xx return codes
                resp.successful = true;
                return;
            } else {
                resp.successful = false;
                retryCount++;
            }
        } while (opts.retryPolicy.shouldRetry(resp.httpResponseCode, resp.ex));
    }

    private static void makeSingleCall(AzureDataLakeStorageClient client,
                                            Operation op,
                                            String path,
                                            QueryParams queryParams,
                                            byte[] requestBody,
                                            int offsetWithinContentsArray,
                                            int length,
                                            RequestOptions opts,
                                            OperationResponse resp) {
        if (client == null || client.getStorageAccountName().equals("") || client.getAccessToken().equals("") ) {
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

        if (    requestBody != null )
            if ( offsetWithinContentsArray < 0 ||
                 length < 0 ||
                 offsetWithinContentsArray + length < 0 || // integer overflow
                 offsetWithinContentsArray >= requestBody.length ||
                 offsetWithinContentsArray + length > requestBody.length)
                {
                    throw new IndexOutOfBoundsException();
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
        urlString.append(client.getStorageAccountName());
        if (op.isExt) {
            urlString.append("/WebHdfsExt");
        } else {
            urlString.append("/webhdfs/v1");
        }

        urlString.append(path);
        urlString.append('?');

        queryParams.setOp(op);
        queryParams.add("api-version", "2015-10-01-preview");
        urlString.append(queryParams.serialize());
        //InputStream responseStream = null;
        try {
            // Setup Http Request (method and headers)
            URL url = new URL(urlString.toString());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(op.method);
            conn.setRequestProperty("Authorization", client.getAccessToken());
            // TODO: It is wasteful to recompute User-Agent on every call. Move it to adlclient and reuse same string from there
            conn.setRequestProperty("User-Agent", getUserAgent(client.getUserAgentSuffix()));
            conn.setRequestProperty("x-ms-client-request-id", opts.requestid);
            conn.setConnectTimeout(opts.timeout);
            conn.setUseCaches(false);

            // populate request body if applicable
            if (op.requiresBody && requestBody != null) {
                conn.setDoOutput(true);
                OutputStream s = conn.getOutputStream();
                s.write(requestBody, offsetWithinContentsArray, length);
                s.close();
            } else {
                // This doesnt work, so use work-around below. Ugly.
                //conn.setDoOutput(false);
                //conn.setRequestProperty("Content-Length", "0");
                conn.setDoOutput(true);
                byte[] b = new byte[] {};  // zero-length byte-array, to force content-length of zero
                OutputStream s = conn.getOutputStream();
                s.write(b);
                s.close();
            }

            // get Response Stream if applicable
            // This is where the HTTP call happens
            resp.httpResponseCode = conn.getResponseCode();
            resp.httpResponseMessage = conn.getResponseMessage();
            resp.requestId = conn.getHeaderField("x-ms-request-id");

            if (resp.httpResponseCode >= 400) {
                if (conn.getHeaderFieldLong("Content-Length", 0) > 0 && conn.getInputStream() != null) {
                    getCodesFromJSon(conn.getInputStream(), resp);
                    return;
                }
            }

            // read response body if applicable
            if (op.returnsBody) {
                resp.responseStream = conn.getInputStream();
                resp.responseContentLength = conn.getHeaderFieldLong("Content-Length", 0);
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
