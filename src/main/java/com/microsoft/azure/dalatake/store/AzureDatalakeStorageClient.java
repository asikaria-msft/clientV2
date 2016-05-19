package com.microsoft.azure.dalatake.store;


public class AzureDatalakeStorageClient {

    public static AzureDatalakeStorageClient Create(String accountFQDN, AzureADToken token) {
        return null;
    }

    public static AzureDatalakeStorageClient Create(String accountFQDN, String accessToken) {
        return null;
    }

    public ADLFile GetFileReference(String filename) {
        return null;
    }

    public ADLFile GetFileReferenceFromServer(String filename) {
        return null;
    }

    public ADLDirectory GetDirectoryReference(String directoryname) {
        return null;
    }

    public ADLDirectory GetDirectoryReferenceFromServer(String directoryname) {
        return null;
    }

    /**
     * Registers a callback to be called at the end of every server call in any method from this client.
     * This allows interested clients to track things like latency, request tracing ID, etc.
     * Keeping the callback separate allows the main interface to be clean, while enabling
     * the few use cases where customer might be interested in gathering stats for success cases.
     *
     * This is also a potential place for custom logging from customers.
     *
     * @param callback
     */
    public void RegisterCallback(ICallback callback) { }


    /**
     * Tokens expire in 1 hour. This call allows user to update token on existing clients.
     * This is useful if the client is expected to be used over long time.
     *
     * @param token
     */
    public void UpdateToken(AzureADToken token) {

    }

    public void UpdateToken(String accessToken) {

    }


    /*
       Static Methods for convenience
    */

    public static boolean CheckDirectoryExists(String fileName) {
        return true;
    }

    public static boolean CreateDirectory(String directoryName) {
        return true;
    }

    public static boolean CheckFileExists(String fileName) {
        return true;
    }
}
