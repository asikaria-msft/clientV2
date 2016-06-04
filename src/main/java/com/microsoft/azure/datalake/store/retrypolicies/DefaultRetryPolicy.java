package com.microsoft.azure.datalake.store.retrypolicies;

public class DefaultRetryPolicy implements RetryPolicy {

    private int retryCount = 0;
    private int maxRetries = 2;
    private int linearRetryInterval = 1000;
    private int exponentialRetryInterval = 1000;

    public boolean shouldRetry(int httpResponseCode, Exception lastException) {

        // exponential backoff if throttled
        if (httpResponseCode == 429 || httpResponseCode == 503) { // throttled, backoff exponentially
           if (retryCount < maxRetries) {
               wait(exponentialRetryInterval);
               exponentialRetryInterval *= 2;
               retryCount++;
               return true;
           }
        }

        // Non-retryable error
        if (     (httpResponseCode >= 300 && httpResponseCode < 500 && httpResponseCode != 408)
               || httpResponseCode == 501 // Not Implemented
               || httpResponseCode == 505 // Version Not Supported
               ) {
            return false;
        }

        // Retryable error, retry with linear backoff
        if ( lastException!=null || httpResponseCode >=500) {
            if (retryCount < maxRetries) {
                wait(linearRetryInterval);
                retryCount++;
                return true;
            }
        }

        // these are not errors - this method should never have been called with this
        if (httpResponseCode >= 100 && httpResponseCode <300)
        {
            // TODO: log this
            return false;
        }

        // Dont know what happened - we should never get here
        // TODO: log this
        return false;
    }

    private void wait(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) { }
    }
}
