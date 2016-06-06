package com.microsoft.azure.datalake.store.protocol;

import java.util.concurrent.ArrayBlockingQueue;


class LatencyTracker {
    /*
     Schema:
     Single entry, comma separated:
      1. Client Request ID
      2. Retry Number
      3. latency in milliseconds (if integer)
      4. error code (if request failed)
      5. Operation
      6. Request+response body Size (if available, zero otherwise)

     Multiple entries can be on a single request. Entries will be separated by semicolons
     Limit max entires on a single request to three, to limit increase in HTTP request size.
    */

    private static final ArrayBlockingQueue<String> Q = new ArrayBlockingQueue<String>(256);
    private static final int MAXPERLINE = 3;
    private static boolean disabled = false;

    private LatencyTracker() {} // Prevent instantiation - static methods only

    public static synchronized void disable() {
        // using synchronized causes update to "disabled" to be published, so other threads will see updated value.
        // Deadlocks:
        //    Since this is the only method that acquires lock on the class object, deadlock qith Q's lock is not an
        //        issue (i.e., lock order is deterministic throughout the class: LatencyTracker.class, then Q)
        disabled = true;
        Q.clear();
        // The clear does not guarantee that Q will be empty afterwards - e.g., if another thread was in the middle
        // of add. However, the clear is not critical. Also, disable is one-way, so a little bit of crud leftover
        // doesnt matter. If in the future we offer re-enable, then the enable would have to clear the Q, to prevent
        // very old entries from being sent.
    }

    static void addLatency(String clientRequestId, int retryNum, long latency, String operation, long size) {
        if (disabled) return;
        String line = String.format("%s,%s,%s,,%s,%s", clientRequestId, Integer.toString(retryNum), Long.toString(latency), operation, Long.toString(size));
        Q.offer(line); // non-blocking append. If queue is full then silently discard
    }

    static void addError(String clientRequestId, int retryNum, String error, String operation, long size) {
        if (disabled) return;
        String line = String.format("%s,%s,,%s,%s,%s", clientRequestId, Integer.toString(retryNum), error, operation, Long.toString(size));
        Q.offer(line); // non-blocking append. If queue is full then silently discard
    }

    static String get() {
        if (disabled) return null;
        int count = 0;
        String separator = "";
        StringBuilder line = new StringBuilder(MAXPERLINE * 2);
        String entry = Q.poll();
        if (entry == null) return null;
        while (entry != null && count < MAXPERLINE) {
            line.append(separator);
            line.append(entry);
            separator = ";";
            count++;
            entry = Q.poll();
        }
        return line.toString();
    }
}
