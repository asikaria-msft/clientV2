package com.microsoft.azure.datalake.store;


import java.net.URI;


public class AzureDataLakeStorageClient {


    private final String accountFQDN;
    private String accessToken;
    private String userAgentSuffix;


    private AzureDataLakeStorageClient(String accountFQDN, String accessToken) {
        this.accountFQDN = accountFQDN;
        this.accessToken = "Bearer " + accessToken;
        utils = new Utils(this);
    }

    public static AzureDataLakeStorageClient createClient(String accountFQDN, AzureADToken token) {
        return new AzureDataLakeStorageClient(accountFQDN, token.accessToken);
    }

    public static AzureDataLakeStorageClient createClient(String accountFQDN, String accessToken) {
        return new AzureDataLakeStorageClient(accountFQDN, accessToken);
    }

    public ADLFileInfo getFileInfo(String filename) {
        return new ADLFileInfo(this, filename);
    }
    public ADLFileInfo getFileInfo(URI fileUri) {
        if (!fileUri.getAuthority().equals(accountFQDN))
            throw new IllegalArgumentException("account name in URI doesnt match the account of the client");
        return new ADLFileInfo(this, fileUri.getPath());
    }

    public ADLDirectoryInfo getDirectoryInfo(String directoryName) {
        return new ADLDirectoryInfo(this, directoryName);
    }

    /**
     * AAD Tokens expire in 1 hour. This call allows user to update token on existing clients.
     * This is useful if the client is expected to be used over long time.
     *
     * @param token The OAuth2 Token string
     */
    public void updateToken(AzureADToken token) {
        this.accessToken = "Bearer " + token.accessToken;
    }

    public void updateToken(String accessToken) {
        this.accessToken = "Bearer " + accessToken;
    }

    public String getStorageAccountName() {
        return accountFQDN;
    }

    public String getAccessToken() {
        return accessToken;
    }

    // All the calls made from this client will use this user agent suffix
    public String getUserAgentSuffix() {
        return userAgentSuffix;
    }

    public void setUserAgentSuffix(String userAgentSuffix) {
        this.userAgentSuffix = userAgentSuffix;
    }


    /*
       Utils class holds the convenience methods
    */
    public final Utils utils;



}
