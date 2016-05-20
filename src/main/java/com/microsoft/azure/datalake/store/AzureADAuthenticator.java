package com.microsoft.azure.datalake.store;

/**
 * provides convenience methods over AAD client
 */
public class AzureADAuthenticator {

    public static AzureADToken GetTokenUsingClientCreds(String authEndpoint, String clientId, String ClientSecret) {
        return null;
    }

    public static AzureADToken GetTokenUsingClientCert(String authEndpoint, String clientId, java.security.cert.X509Certificate cert){
        return null;
    }

    public static AzureADToken GetTokenUsingRefreshToken(String authEndpoint, String refreshToken){
        return null;
    }

    public static AzureADToken GetTokenUsingUserCreds(String authEndpoint, String userId, String password){
        return null;
    }

    public static AzureADToken GetTokenUsingInteractiveLogin(){
        return null;
    }

    public static AzureADToken GetTokenUsingAuthCode(){
        return null;
    }
}


