package com.microsoft.azure.datalake.store.protocol;

import com.microsoft.azure.datalake.store.retrypolicies.RetryPolicy;
import java.util.Map;

public class RequestOptions {
    /**
     * the timeout to use for the request.
     * TODO: This is currently only implemented as connection timeout
     */
    public int timeout = 0;

    /**
     * the client request ID. the SDK generates a UUID if a request ID is not specified.
     */
    public String requestid = null;

    /**
     * the {@link RetryPolicy} to use for the request
     */
    public RetryPolicy retryPolicy = null;
}
