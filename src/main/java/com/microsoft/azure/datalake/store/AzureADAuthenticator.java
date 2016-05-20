package com.microsoft.azure.datalake.store;

/**
 * provides convenience methods over AAD client
 */
public class AzureADAuthenticator {

    public static AzureADToken getTokenUsingClientCreds(String authEndpoint, String clientId, String clientSecret) {
        return null;
    }

    public static AzureADToken getTokenUsingClientCert(String authEndpoint, String clientId, java.security.cert.X509Certificate cert){
        return null;
    }

    public static AzureADToken getTokenUsingRefreshToken(String authEndpoint, String refreshToken){
        return null;
    }

    public static AzureADToken getTokenUsingUserCreds(String authEndpoint, String userId, String password){
        return null;
    }

    public static AzureADToken getTokenUsingInteractiveLogin(){
        return null;
    }

    public static AzureADToken getTokenUsingAuthCode(){
        return null;
    }
}


