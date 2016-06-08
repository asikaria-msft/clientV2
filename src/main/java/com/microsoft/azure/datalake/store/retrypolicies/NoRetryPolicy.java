package com.microsoft.azure.datalake.store.retrypolicies;

/**
 * No retry ever. Always returns false, indicating that erequest should not be retried.
 *
 * This should be used when retrying is not safe, and user wants at-most-once semantics with the call. This is
 * useful for non-idempotent methods, where the error retruned by the last call does not conclusively indicate
 * success or failure of the call. for example, if an append times out but succeeds ont he back-end , then
 * retrying it may append the data twice to the file.
 *
 */
public class NoRetryPolicy implements RetryPolicy {

    public boolean shouldRetry(int httpResponseCode, Exception lastException) {
        return false;
    }
}
