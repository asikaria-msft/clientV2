package com.microsoft.azure.datalake.store.protocol;

import com.microsoft.azure.datalake.store.retrypolicies.RetryPolicy;
import java.util.Map;

public class RequestOptions {
    public int timeout = 0;
    public String requestid = null;
    public RetryPolicy retryPolicy = null;
    public Map<String, String> AdditionalQueryParams = null;
    public Map<String, String> AdditionalHeaders = null;
}
