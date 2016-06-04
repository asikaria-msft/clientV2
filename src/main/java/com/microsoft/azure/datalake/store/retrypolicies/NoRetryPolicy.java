package com.microsoft.azure.datalake.store.retrypolicies;

public class NoRetryPolicy implements RetryPolicy {

    public boolean shouldRetry(int httpResponseCode, Exception lastException) {
        return false;
    }
}
