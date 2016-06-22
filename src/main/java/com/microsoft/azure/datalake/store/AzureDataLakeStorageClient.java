/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;


import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;


/**
 * {@code AzureDataLakeStorageClient} class represents a client to Azure Data Lake. It can be used to obtain references
 * to files and directories, which can be operated upon using the returned {@link ADLFileInfo} objects.
 * <P>
 * {@code AzureDataLakeStorageClient} class also has a {@link Utils utils} member that can be used to perform many operations
 * as simple one-liners. The same operations can also be performed using the {@link ADLFileInfo} object returned from
 * {@link #getFileInfo(String) getFileInfo} calls.
 * </P>
 *
 */
public class AzureDataLakeStorageClient {

    private final String accountFQDN;
    private String accessToken;
    private final AccessTokenProvider tokenProvider;
    private String userAgentSuffix;
    private String userAgentString;
    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store"); // package-default logging policy
    private static final AtomicLong clientIdCounter = new AtomicLong(0);
    private final long clientId;
    private String proto = "https";

    private static String userAgent =
            String.format("%s.%s/%s-%s/%s/%s-%s",
                    "ADLSJavaSDK",
                    AzureDataLakeStorageClient.class.getPackage().getImplementationVersion(), // SDK Version
                    System.getProperty("os.name").replaceAll(" ", ""),
                    System.getProperty("os.version"),
                    System.getProperty("os.arch"),
                    System.getProperty("java.vendor").replaceAll(" ", ""),
                    System.getProperty("java.version")
            );


    /**
     * {@link Utils utils} member that can be used to perform many operations
     * as simple one-liners. The same operations can also be performed using the {@link ADLFileInfo} objects
     * returned from {@link #getFileInfo(String) getFileInfo} calls.
     */
    public final Utils utils;


    // private constructor, references should be obtained using the createClient factory method
    private AzureDataLakeStorageClient(String accountFQDN, String accessToken, long clientId, AccessTokenProvider tokenProvider) {
        this.accountFQDN = accountFQDN;
        this.accessToken = "Bearer " + accessToken;
        this.tokenProvider = tokenProvider;
        this.clientId = clientId;
        this.utils = new Utils(this);
        this.userAgentString = userAgent;
    }

    /**
     * gets an {@code AzureDataLakeStorageClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    e.g., contoso.azuredatalakestore.net
     * @param token {@link AzureADToken} object that contains the AAD token to use
     * @return the client object
     */
    public static AzureDataLakeStorageClient createClient(String accountFQDN, AzureADToken token) {
        if (accountFQDN == null || accountFQDN.trim().equals("")) {
            throw new IllegalArgumentException("account name is required");
        }
        if (token == null || token.accessToken == null || token.accessToken.equals("")) {
            throw new IllegalArgumentException("token is required");
        }
        long clientId =  clientIdCounter.incrementAndGet();
        log.debug("AzureDatalakeStorageClient {} created for {}", clientId, accountFQDN);
        return new AzureDataLakeStorageClient(accountFQDN, token.accessToken, clientId, null);
    }

    /**
     * gets an {@code AzureDataLakeStorageClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    e.g., contoso.azuredatalakestore.net
     * @param accessToken String containing the AAD access token to be used
     * @return the client object
     */
    public static AzureDataLakeStorageClient createClient(String accountFQDN, String accessToken) {
        if (accountFQDN == null || accountFQDN.trim().equals("")) {
            throw new IllegalArgumentException("account name is required");
        }

        if (accessToken == null || accessToken.equals("")) {
            throw new IllegalArgumentException("token is required");
        }
        long clientId =  clientIdCounter.incrementAndGet();
        log.debug("AzureDatalakeStorageClient {} created for {}", clientId, accountFQDN);
        return new AzureDataLakeStorageClient(accountFQDN, accessToken, clientId, null);
    }

    /**
     * gets an {@code AzureDataLakeStorageClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    For example, contoso.azuredatalakestore.net
     * @param tokenProvider {@link AccessTokenProvider} that can provide the AAD token
     * @return the client object
     */
    public static AzureDataLakeStorageClient createClient(String accountFQDN, AccessTokenProvider tokenProvider) {
        if (accountFQDN == null || accountFQDN.trim().equals("")) {
            throw new IllegalArgumentException("account name is required");
        }

        if (tokenProvider == null) {
            throw new IllegalArgumentException("token provider is required");
        }
        long clientId =  clientIdCounter.incrementAndGet();
        log.debug("AzureDatalakeStorageClient {} created for {}", clientId, accountFQDN);
        return new AzureDataLakeStorageClient(accountFQDN, null, clientId, tokenProvider);
    }

    /**
     * gets an {@link ADLFileInfo} object that can be used to manipulate or read a file.
     * <P>
     * Note that creating this object does not contact the server, it only creates a local object that can be used
     * to operate on file objects on the server.
     * </P>
     * @param filename String containing the filename
     * @return {@link ADLFileInfo} object that can be used to manipulate or read a file
     */
    public ADLFileInfo getFileInfo(String filename) {
        log.debug("FileInfo created for client {} for {}", this.clientId, filename);
        return new ADLFileInfo(this, filename);
    }

    /**
     * gets an {@link ADLFileInfo} object that can be used to manipulate or read a file.
     * <P>
     * Note that creating this object does not contact the server, it only creates a local object that can be used
     * to operate on the corresponding file on the server.
     * </P>
     * @param fileUri URI containing the file to be referenced
     * @return {@link ADLFileInfo} object that can be used to manipulate or read a file
     *
     */
    public ADLFileInfo getFileInfo(URI fileUri) {
        if (!fileUri.getAuthority().equals(accountFQDN))
            throw new IllegalArgumentException("account name in URI doesnt match the account of the client");
        return new ADLFileInfo(this, fileUri.getPath());
    }

    /**
     * update token on existing client.
     * This is useful if the client is expected to be used over long time, and token has expired.
     *
     * @param token The OAuth2 Token
     */
    public synchronized void updateToken(AzureADToken token) {
        log.debug("AAD Token Updated for client client {} for account {}", clientId, accountFQDN);
        this.accessToken = "Bearer " + token.accessToken;
    }

    /**
     * update token on existing client.
     * This is useful if the client is expected to be used over long time, and token has expired.
     *
     * @param accessToken The AAD Token string
     */
    public synchronized void updateToken(String accessToken) {
        log.debug("AAD Token Updated for client client {} for account {}", clientId, accountFQDN);
        this.accessToken = "Bearer " + accessToken;
    }

    /**
     * gets the Azure Data Lake Store account name associated with this client
     * @return the account name
     */
    public String getAccountName() {
        return accountFQDN;
    }

    /**
     * gets the AAD access token associated with this client
     * @return String containing the AAD Access token
     */
    public String getAccessToken() throws IOException {
        if (tokenProvider != null ) {
            return "Bearer " + tokenProvider.getToken().accessToken;
        } else {
            return accessToken;
        }
    }

    /**
     * returns the user agent suffix to be added to the User-Agent header in all HTTP requests made to the server.
     * This suffix is appended to the end of the User-Agent string constructed by the SDK.
     * @return the suffix
     */
    public String getUserAgentSuffix() {
        return userAgentSuffix;
    }

    /**
     * Gets a unique long associated with this instance of {@code AzureDataLakeStorageClient}
     *
     * @return a unique long associated with this instance of {@code AzureDataLakeStorageClient}
     */
    public long getClientId() {
        return this.clientId;
    }

    /**
     * sets the user agent suffix to be added to the User-Agent header in all HTTP requests made to the server.
     * This suffix is appended to the end of the User-Agent string constructed by the SDK.
     *
     * @param userAgentSuffix the suffix
     */
    public synchronized void setUserAgentSuffix(String userAgentSuffix) {
        if (userAgentSuffix != null && !userAgentSuffix.trim().equals("")) {
            this.userAgentString = userAgent + "/" + userAgentSuffix;
        }
    }

    /**
     * Gets the HTTP User-Agent string that will be used for requests made from this client.
     * @return User-Agent string
     */
    public String getUserAgent() {
        return userAgentString;
    }

    /**
     * Use http as transport for back-end calls, instead of https. This is to allow unit
     * testing using mock or fake web servers.
     * <P>
     * <B>Warning: Do not</B> use this for talking to real Azure Data Lake service,
     * since https is the only supported protocol on the server.
     * </P>
     */
    public void setInsecureTransport() {
        proto = "http";
    }

    /**
     * get the http prefix ({@code http} or {@code https}) that will be used for
     * connections used by thei client.
     * @return Sytring containing the HTTP protocol used ({@code http} or {@code https})
     */
    public String getHttpPrefix() {
        return proto;
    }


}
