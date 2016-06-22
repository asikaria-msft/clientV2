/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.oauth2;

import java.io.IOException;
import java.util.Calendar;

/**
 * Returns an Azure Active Directory token when requested. The provider can cache the token if it has already
 * retrieved one. If it does, then the provider is responsible for checking expiry and refreshing as needed.
 * <P>
 * In other words, this is is a token cache that fetches tokens when requested, if the cached token has expired.
 * </P>
 */
public abstract class AccessTokenProvider {

    protected AzureADToken token;

    /**
     * returns the {@link AzureADToken} cached (or retrieved) by this instance.
     *
     * @return {@link AzureADToken} containing the access token
     * @throws IOException if there is an error fetching the token
     */
    public synchronized AzureADToken getToken() throws IOException {
        if (isTokenAboutToExpire()) {
            token = refreshToken();
        }
        return token;
    }

    /**
     * the method to fetch the access token. Derived classes should override this method to
     * actually get the token from Azure Active Directory.
     * <P>
     * This method will be called initially, and then once when the token is about to expire.
     * </P>
     *
     *
     * @return {@link AzureADToken} containing the access token
     * @throws IOException if there is an error fetching the token
     */
    protected abstract AzureADToken refreshToken() throws IOException;

    /**
     * Checks if the token is about to expire in the next 5 minutes. The 5 minute allowance is to
     * allow for clock skew and also to allow for token to be refreshed in that much time.
     *
     *
     * @return true if the token is expiring in next 5 minutes
     */
    private boolean isTokenAboutToExpire() {
        if (token==null) return true;   // no token should have same response as expired token
        boolean expiring = false;
        Calendar approximatelyNow = Calendar.getInstance(); // get current time
        approximatelyNow.add(Calendar.MINUTE, 5);  // allow 5 minutes for clock skew
        if (token.expiry.before(approximatelyNow)) expiring = true;
        return expiring;
    }
}
