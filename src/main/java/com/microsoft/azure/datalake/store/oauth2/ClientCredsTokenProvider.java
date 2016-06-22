/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.oauth2;

import java.io.IOException;

/**
 * Provides tokens based on client credentials
 */
public class ClientCredsTokenProvider extends AccessTokenProvider {
    private final String authEndpoint, clientId, clientSecret;

    /**
     * constructs a token provider based on supplied credentials.
     *
     * @param authEndpoint the OAuth 2.0 token endpoint associated with the user's directory
     *                     (obtain from Active Directory configuration)
     * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
     * @param clientSecret the secret key of the client web app
     */
    public ClientCredsTokenProvider(String authEndpoint, String clientId, String clientSecret) {
        this.authEndpoint = authEndpoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    @Override
    protected AzureADToken refreshToken() throws IOException {
        return AzureADAuthenticator.getTokenUsingClientCreds(authEndpoint, clientId, clientSecret);
    }
}
