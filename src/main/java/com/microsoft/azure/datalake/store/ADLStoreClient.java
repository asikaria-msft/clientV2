/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;


import com.microsoft.azure.datalake.store.acl.AclEntry;
import com.microsoft.azure.datalake.store.acl.AclStatus;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;
import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialOnThrottlePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * {@code ADLStoreClient} class represents a client to Azure Data Lake. It can be used to perform operations on files
 * and directories.
 * <P>
 * {@code ADLStoreClient} class also has a {@link Utils utils} member that can be used to perform many operations
 * as simple one-liners.
 * </P>
 *
 */
public class ADLStoreClient {

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
                    ADLStoreClient.class.getPackage().getImplementationVersion(), // SDK Version
                    System.getProperty("os.name").replaceAll(" ", ""),
                    System.getProperty("os.version"),
                    System.getProperty("os.arch"),
                    System.getProperty("java.vendor").replaceAll(" ", ""),
                    System.getProperty("java.version")
            );


    /**
     * {@link Utils utils} member that can be used to perform many operations
     * as simple one-liners.
     */
    public final Utils utils;


    // private constructor, references should be obtained using the createClient factory method
    private ADLStoreClient(String accountFQDN, String accessToken, long clientId, AccessTokenProvider tokenProvider) {
        this.accountFQDN = accountFQDN;
        this.accessToken = "Bearer " + accessToken;
        this.tokenProvider = tokenProvider;
        this.clientId = clientId;
        this.utils = new Utils(this);
        this.userAgentString = userAgent;
    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    e.g., contoso.azuredatalakestore.net
     * @param token {@link AzureADToken} object that contains the AAD token to use
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, AzureADToken token) {
        if (accountFQDN == null || accountFQDN.trim().equals("")) {
            throw new IllegalArgumentException("account name is required");
        }
        if (token == null || token.accessToken == null || token.accessToken.equals("")) {
            throw new IllegalArgumentException("token is required");
        }
        long clientId =  clientIdCounter.incrementAndGet();
        log.debug("AzureDatalakeStorageClient {} created for {}", clientId, accountFQDN);
        return new ADLStoreClient(accountFQDN, token.accessToken, clientId, null);
    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    e.g., contoso.azuredatalakestore.net
     * @param accessToken String containing the AAD access token to be used
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, String accessToken) {
        if (accountFQDN == null || accountFQDN.trim().equals("")) {
            throw new IllegalArgumentException("account name is required");
        }

        if (accessToken == null || accessToken.equals("")) {
            throw new IllegalArgumentException("token is required");
        }
        long clientId =  clientIdCounter.incrementAndGet();
        log.debug("AzureDatalakeStorageClient {} created for {}", clientId, accountFQDN);
        return new ADLStoreClient(accountFQDN, accessToken, clientId, null);
    }

    /**
     * gets an {@code ADLStoreClient} object.
     *
     * @param accountFQDN string containing the fully qualified domain name of the account.
     *                    For example, contoso.azuredatalakestore.net
     * @param tokenProvider {@link AccessTokenProvider} that can provide the AAD token
     * @return the client object
     */
    public static ADLStoreClient createClient(String accountFQDN, AccessTokenProvider tokenProvider) {
        if (accountFQDN == null || accountFQDN.trim().equals("")) {
            throw new IllegalArgumentException("account name is required");
        }

        if (tokenProvider == null) {
            throw new IllegalArgumentException("token provider is required");
        }
        long clientId =  clientIdCounter.incrementAndGet();
        log.debug("AzureDatalakeStorageClient {} created for {}", clientId, accountFQDN);
        return new ADLStoreClient(accountFQDN, null, clientId, tokenProvider);
    }

    /* ----------------------------------------------------------------------------------------------------------*/

    /*
    *
    * Methods that apply to Files only
    *
    */

    /**
     * create a file. If {@code overwriteIfExists} is false and the file already exists,
     * then an exceptionis thrown.
     * The call returns an {@link ADLFileOutputStream} that can then be written to.
     *
     *
     * @param path full pathname of file to create
     * @param mode {@link IfExists} {@code enum} specifying whether to overwite or throw
     *                             an exception if the file already exists
     * @return  {@link ADLFileOutputStream} to write to
     */
    public ADLFileOutputStream createFromStream(String path, IfExists mode) {
        boolean overwriteIfExists = (mode == IfExists.OVERWRITE);
        return new ADLFileOutputStream(path, this, true, overwriteIfExists);
    }

    /**
     * Opens a file for read and returns an {@link ADLFileInputStream} to read the file
     * contents from.
     *
     * @param path full pathname of file to read
     * @return {@link ADLFileInputStream} to read the file contents from.
     */
    public ADLFileInputStream getReadStream(String path) {
        return new ADLFileInputStream(path, this);
    }

    /**
     * appends to an existing file.
     *
     * @param path full pathname of file to append to
     * @return {@link ADLFileOutputStream} to write to. The contents written to this stream
     *         will be appended to the file.
     */
    public ADLFileOutputStream getAppendStream(String path) {
        return new ADLFileOutputStream(path, this, false, false);
    }

    /**
     * Concatenate the specified list of files into this file. The target should not exist.
     * The source files will be deleted if the concatenate succeeds.
     *
     *
     * @param path full pathname of the destination to concatenate files into
     * @param fileList {@link List} of strings containing full pathnames of the files to concatenate.
     *                Cannot be null or empty.
     * @return returns true if the call succeeds
     * @throws ADLException {@link ADLException} is thrown if there is an error in concatenating files
     */
    public boolean concatenateFiles(String path, List<String> fileList) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.concat(path, fileList, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error concatenating files into " + path);
        }
        return true;
    }

    /*
    *
    * Methods that apply to Directories only
    *
    */

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     * @param path full pathname of directory to enumerate
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, int maxEntriesToRetrieve) throws ADLException  {
        return enumerateDirectory(path, maxEntriesToRetrieve, null, null);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     *
     * @param path full pathname of directory to enumerate
     * @param startAfter the filename after which to begin enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, String startAfter) throws ADLException  {
        return enumerateDirectory(path, 0, startAfter, null);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     *
     * @param path full pathname of directory to enumerate
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @param startAfter the filename after which to begin enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, int maxEntriesToRetrieve, String startAfter) throws ADLException  {
        return enumerateDirectory(path, maxEntriesToRetrieve, startAfter, null);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     *
     * @param path full pathname of directory to enumerate
     * @param startAfter the filename after which to begin enumeration
     * @param endBefore the filename before which to end the enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, String startAfter, String endBefore) throws ADLException  {
        return enumerateDirectory(path, 0, startAfter, endBefore);
    }

    /**
     * Enumerates the contents of a directory, returning a {@link List} of {@link DirectoryEntry} objects,
     * one per file or directory in the specified directory.
     * <P>
     * To avoid overwhelming the client or the server, the call may return a partial list, in which case
     * the caller should make the call again with the last entry from the returned list specified as the
     * {@code startAfter} parameter of the next call.
     * </P>
     *
     * @param path full pathname of directory to enumerate
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @param startAfter the filename after which to begin enumeration
     * @param endBefore the filename before which to end the enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String path, int maxEntriesToRetrieve, String startAfter, String endBefore) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        List<DirectoryEntry> dirEnt  = Core.listStatus(path, startAfter, endBefore, maxEntriesToRetrieve, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error enumerating directory " + path);
        }
        return dirEnt;
    }

    /**
     * creates a directory, and all it's parent directories if they dont already exist.
     *
     * @param path full pathname of directory to create
     * @return returns {@code true} if the call succeeded
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean createDirectory(String path) throws ADLException  {
        return createDirectory(path, null);
    }

    /**
     * creates a directory, and all it's parent directories if they dont already exist.
     *
     * @param path full pathname of directory to create
     * @param octalPermission permissions for the directory, as octal digits (for example, {@code "755"}). Can be null.
     * @return returns {@code true} if the call succeeded
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean createDirectory(String path, String octalPermission) throws ADLException  {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.mkdirs(path, octalPermission, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error creating directory " + path);
        }
        return succeeded;
    }

    /**
     * deletes a directory and all it's child directories and files recursively.
     *
     * @param path full pathname of directory to delete
     * @return returns {@code true} if the call succeeded
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean deleteRecursive(String path) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.delete(path, true, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error deleting directory tree " + path);
        }
        return succeeded;
    }

    /**
     * removes all default acl entries from a directory. The access ACLs for the directory itself are
     * not modified.
     *
     * @param path full pathname of directory to remove default ACLs from
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void removeDefaultAcls(String path) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeDefaultAcl(path, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing default ACLs for directory " + path);
        }
    }

    /*
    *
    * Methods that apply to both Files and Directoties
    *
    */

    /**
     * rename a file or directory.
     *
     * @param path full pathname of file or directory to rename
     * @param newName the new name of the file
     *
     * @return returns {@code true} if the call succeeded
     *
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean rename(String path, String newName) throws ADLException {
        return rename(path, newName, false);
    }

    /**
     * rename a file or directory.
     *
     * @param path full pathname of file or directory to rename
     * @param newName the new name of the file/directory
     * @param overwrite overwrite destination if it already exists. If the
     *                  destination is a non-empty directory, then the call
     *                  fails rather than overwrite the directory.
     *
     * @return returns {@code true} if the call succeeded
     *
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean rename(String path, String newName, boolean overwrite) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.rename(path, newName, overwrite, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error renaming file " + path);
        }
        return succeeded;
    }

    /**
     * delete the file or directory.
     *
     * @param path full pathname of file or directory to delete
     * @return returns {@code true} if the call succeeded
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean delete(String path) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.delete(path, false, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error deleting directory " + path);
        }
        return succeeded;
    }

    /**
     * Gets the directory metadata about this file or directory.
     *
     * @param path full pathname of file or directory to get directory entry for
     * @return {@link DirectoryEntry} containing the metadata for the file/directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public DirectoryEntry getDirectoryEntry(String path) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        DirectoryEntry dirEnt  = Core.getFileStatus(path, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error getting info for file " + path);
        }
        return dirEnt;
    }

    /**
     * sets the owning user and group of the file. If the user or group are {@code null}, then they are not changed.
     * It is illegal to pass both user and owner as {@code null}.
     *
     * @param path full pathname of file or directory to set owner/group for
     * @param owner the ID of the user, or {@code null}
     * @param group the ID of the group, or {@code null}
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void setOwner(String path, String owner, String group) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setOwner(path, owner, group, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting owner for file " + path);
        }
    }

    /**
     * sets one or both of the times (Modified and Access time) of the file or directory
     *
     * @param path full pathname of file or directory to set times for
     * @param atime Access time as a long
     * @param mtime Modified time as a long
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void setTimes(String path, Date atime, Date mtime) throws ADLException {
        long atimeLong = (atime == null)? -1 : atime.getTime();
        long mtimeLong = (mtime == null)? -1 : mtime.getTime();

        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setTimes(path, atimeLong, mtimeLong, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting times for file " + path);
        }
    }

    /**
     * Sets the permissions of the specified file ro directory. This sets the traditional unix read/write/execute
     * permissions for the file/directory. To set Acl's use the
     * {@link  #setAcl(String, List) setAcl} call.
     *
     * @param path full pathname of file or directory to set permissions for
     * @param octalPermissions the permissions to set, in unix octal form. For example, '644'.
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void setPermission(String path, String octalPermissions) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setPermission(path, octalPermissions, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting times for " + path);
        }
    }

    /**
     * checks whether the calling user has the required permissions for the file/directory . The permissions
     * to check should be specified in the rwx parameter, as a unix permission string
     * (for example, {@code "r-x"}).
     *
     * @param path full pathname of file or directory to check access for
     * @param rwx the permission to check for, in rwx string form. The call returns true if the caller has
     *            all the requested permissions. For example, specifying {@code "r-x"} succeeds if the caller has
     *            read and execute permissions.
     * @return true if the caller has the requested permissions, false otherwise
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean checkAccess(String path, String rwx) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.checkAccess(path, rwx, this, opts, resp);
        if (!resp.successful) {
            if (resp.httpResponseCode == 401 || resp.httpResponseCode == 403) return false;
            throw Core.getExceptionFromResp(resp, "Error checking access for " + path);
        }
        return true;
    }

    /**
     * checks whether the calling user has read permissions for the file/directory
     *
     * @param path full pathname of file or directory to check access for
     * @return true if the caller has read permissions, false otherwise
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean canRead(String path) throws ADLException {
        return checkAccess(path, "r--");
    }

    /**
     * checks whether the calling user has write permissions for the file/directory
     *
     * @param path full pathname of file or directory to check access for
     * @return true if the caller has write permissions, false otherwise
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean canWrite(String path) throws ADLException  {
        return checkAccess(path, "-w-");
    }

    /**
     * checks whether the calling user has execute permissions for the file/directory
     *
     * @param path full pathname of file or directory to check access for
     * @return true if the caller has execute permissions, false otherwise
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean canExecute(String path) throws ADLException  {
        return checkAccess(path, "--x");
    }

    /**
     * Modify the acl entries for a file or directory. This call merges the supplied list with
     * existing ACLs. If an entry with the same scope, type and user already exists, then the permissions
     * are replaced. If not, than an new ACL entry if added.
     *
     * @param path full pathname of file or directory to change ACLs for
     * @param aclSpec {@link List} of {@link AclEntry}s, containing the entries to add or modify
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void modifyAclEntries(String path, List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.modifyAclEntries(path, aclSpec, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error modifying ACLs for " + path);
        }
    }

    /**
     * Sets the ACLs for a file or directory. If the file or directory already has any ACLs
     * associated with it, then all the existing ACLs are removed before adding the specified
     * ACLs.
     *
     * @param path full pathname of file or directory to set ACLs for
     * @param aclSpec {@link List} of {@link AclEntry}s, containing the entries to set
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void setAcl(String path, List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setAcl(path, aclSpec, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting ACLs for " + path);
        }
    }

    /**
     * Removes the specified ACL entries from a file or directory.
     *
     * @param path full pathname of file or directory to remove ACLs for
     * @param aclSpec {@link List} of {@link AclEntry}s to remove
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void removeAclEntries(String path, List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeAclEntries(path, aclSpec, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing ACLs for " + path);
        }
    }

    /**
     * Removes all acl entries from a file or directory.
     *
     * @param path full pathname of file or directory to remove ACLs for
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void removeAllAcls(String path) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeAcl(path, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing all ACLs for file " + path);
        }
    }

    /**
     * Queries the ACLs and permissions for a file or directory.
     *
     * @param path full pathname of file or directory to query
     * @return {@link AclStatus} object containing the ACL and permission info
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public AclStatus getAclStatus(String path) throws ADLException {
        AclStatus status = null;
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        status = Core.getAclStatus(path, this, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error getting  ACL Status for " + path);
        }
        return status;
    }


    /* ----------------------------------------------------------------------------------------------------------*/

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
     * gets the AAD access token associated with this client
     * @return String containing the AAD Access token
     * @throws IOException thrown if a token provider is being used and the token provider has problem getting token
     */
    public synchronized String getAccessToken() throws IOException {
        if (tokenProvider != null ) {
            return "Bearer " + tokenProvider.getToken().accessToken;
        } else {
            return accessToken;
        }
    }


    /**
     * gets the Azure Data Lake Store account name associated with this client
     * @return the account name
     */
    public String getAccountName() {
        return accountFQDN;
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
     * returns the user agent suffix to be added to the User-Agent header in all HTTP requests made to the server.
     * This suffix is appended to the end of the User-Agent string constructed by the SDK.
     * @return the suffix
     */
    public String getUserAgentSuffix() {
        return userAgentSuffix;
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


    /**
     * Gets a unique long associated with this instance of {@code ADLStoreClient}
     *
     * @return a unique long associated with this instance of {@code ADLStoreClient}
     */
    public long getClientId() {
        return this.clientId;
    }





}
