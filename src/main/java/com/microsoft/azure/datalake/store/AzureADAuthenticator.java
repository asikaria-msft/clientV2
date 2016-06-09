package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.QueryParams;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Calendar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/**
 * This class provides convenience methods to obtain AAD tokens.
 *
 */
public class AzureADAuthenticator {

    /**
     * gets AAD tokens using the user ID and password of a service principal.
     * <P>
     * AAD allows users to set up a web app as a service principal. Users can optionally obtain service principal
     * keys from AAD. This method gets a token using a service principal's client ID and keys. In addition, it
     * needs the token endpoint associated with the user's directory.
     * </P>
     *
     *
     * @param authEndpoint the OAuth 2.0 token endpoint associated with the user's directory
     * @param clientId the client ID (GUID) of the client web app
     * @param clientSecret the secret key of the client web app
     * @return {@link AzureADToken} obtained using the creds
     * @throws IOException throws IOException if there is a failure in connecting to Azure AD
     */
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
        throw new NotImplementedException();
    }

    public static AzureADToken getTokenUsingRefreshToken(String authEndpoint, String refreshToken){
        return null;
    }

    public static AzureADToken getTokenUsingUserCreds(String authEndpoint, String userId, String password){
        return null;
    }
}


