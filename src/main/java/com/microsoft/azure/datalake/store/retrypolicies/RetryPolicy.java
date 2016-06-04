package com.microsoft.azure.datalake.store.retrypolicies;

public interface RetryPolicy {
    boolean shouldRetry(int httpResponseCode, Exception lastException);
}
