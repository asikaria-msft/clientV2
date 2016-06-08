package com.microsoft.azure.datalake.store.protocol;

import java.io.InputStream;

public class OperationResponse {
    /**
     * whether the request was successful. Callers should always check for success before using any return value from
     * any of the calls.
     */
    public boolean successful = false;

    /**
     * the HTTP response code from the call
     */
    public int httpResponseCode;

    /**
     * the message that came with the HTTP response
     */
    public String httpResponseMessage;

    /**
     * for methods that return data from server, this field contains the
     * {@link com.microsoft.azure.datalake.store.ADLFileInputStream ADLFileInputStream}. {@code null} for methods that
     * return no data in the HTTP body.
     *
     */
    public InputStream responseStream = null;

    /**
     * the server request ID.
     */
    public String requestId = null;

    /**
     * the number of retries attempted before returning from the call
     */
    public int numRetries;

    /**
     * the latency of the <I>last</I> try
     */
    public long lastCallLatency = 0;

    /**
     * Content-Length of the returned HTTP body (if return was not chunked). Callers should look at both this and
     * {@link #responseChunked} values to determine whether any data was returned by server.
     */
    public long responseContentLength = 0;


    /**
     * indicates whether HTTP body used chunked for {@code Transfer-Encoding} of the response
     */
    public boolean responseChunked = false;

    /**
     * the exception name as reported by the server, if the call failed on server
     */
    public String remoteExceptionName = null;

    /**
     * the exception message as reported by the server, if the call failed on server
     */
    public String remoteExceptionMessage = null;

    /**
     * the exception's Java Class Name as reported by the server, if the call failed on server
     * This is there for WebHDFS compatibility.
     */
    public String remoteExceptionJavaClassName = null;

    /**
     * exceptions encountered when processing the resquest or response
     */
    public Exception ex = null;

    /**
     * error message, used for errors that originate within the SDK
     */
    public String message;
}

