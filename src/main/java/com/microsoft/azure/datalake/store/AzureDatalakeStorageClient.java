package com.microsoft.azure.datalake.store;


public class AzureDatalakeStorageClient {


    private String accountFQDN;
    private String accessToken;

    private AzureDatalakeStorageClient(String accountFQDN, String accessToken) {
        this.accountFQDN = accountFQDN;
        this.accessToken = accessToken;
        utils = new Utils(this);
    }

    public static AzureDatalakeStorageClient createClient(String accountFQDN, AzureADToken token) {
        return null;
    }

    public static AzureDatalakeStorageClient createClient(String accountFQDN, String accessToken) {
        return null;
    }

    public ADLFileInfo getFileInfo(String filename) {
        return null;
    }

    public ADLDirectoryInfo getDirectoryInfo(String directoryname) {
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
       Utils class holds the convenience methods
    */

    public final Utils utils;



}
