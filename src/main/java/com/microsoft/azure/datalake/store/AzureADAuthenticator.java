package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.QueryParams;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Calendar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * provides convenience methods over AAD client
 */
public class AzureADAuthenticator {

    public static AzureADToken getTokenUsingClientCreds(String authEndpoint, String clientId, String clientSecret)
            throws IOException
    {
        String resource = "https://management.core.windows.net/";
        AzureADToken token = new AzureADToken();

        URL url = new URL(authEndpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");

        QueryParams qp = new QueryParams();
        qp.add("grant_type","client_credentials");
        qp.add("resource", resource);
        qp.add("client_id", clientId);
        qp.add("client_secret", clientSecret);

        conn.setDoOutput(true);
        conn.getOutputStream().write(qp.serialize().getBytes("UTF-8"));

        int httpResponseCode = conn.getResponseCode();
        if (httpResponseCode == 200) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(conn.getInputStream());

            token.accessToken = rootNode.path("access_token").asText();
            int expiryPeriod = rootNode.path("expires_in").asInt(0);
            Calendar c = Calendar.getInstance();
            c.add(Calendar.SECOND, expiryPeriod);
            token.expiry = c;
            token.refreshToken = null;
        } else {
            throw new IOException("Failed to acquire token from AzureAD. Http response: " + httpResponseCode + " " + conn.getResponseMessage());
        }
        return token;
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


