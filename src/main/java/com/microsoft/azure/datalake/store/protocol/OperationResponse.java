package com.microsoft.azure.datalake.store.protocol;

import java.io.InputStream;

public class OperationResponse {
    public boolean successful = false;
    public int httpResponseCode;
    public String httpResponseMessage;

    public InputStream responseStream = null;
    public String requestId = null;

    public int numRetries;
    public long lastCallLatency = 0;
    public long responseContentLength = 0;

    // These come back on non-successful http responses
    public String remoteExceptionName = null;
    public String remoteExceptionMessage = null;
    public String remoteExceptionJavaClassName = null;

    public Exception ex = null;
    public String message; // for errors generated internally by the SDK
}

