/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.acl.AclEntry;
import com.microsoft.azure.datalake.store.acl.AclStatus;
import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialOnThrottlePolicy;

import java.util.Date;
import java.util.List;


/**
 * A local Java object that represents a file on the server.
 * <P>
 * Methods on this class enable manipulation of the file's
 * contents and attributes.
 * </P>
 * <B>Not Thread Safe:</B> methods in this class are not thread-safe.
 */
public class ADLFileInfo {

    private final AzureDataLakeStorageClient client;
    private String filename = null;  // cannot be marked final - rename can change it

    // package-private constructor - use factory method in AzureDataLakeStorageClient
    ADLFileInfo(AzureDataLakeStorageClient client, String filename) {
        this.client = client;
        this.filename = filename;
    }

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
     *
     * @param overwriteIfExists whether to overwite or throw an exception if the
     *                          file already exists
     * @return  {@link ADLFileOutputStream} to write to
     */
    public ADLFileOutputStream createFromStream(boolean overwriteIfExists) {
        return new ADLFileOutputStream(filename, client, true, overwriteIfExists);
    }

    /**
     * Opens a file for read and returns an {@link ADLFileInputStream} to read the file
     * contents from.
     *
     * @return {@link ADLFileInputStream} to read the file contents from.
     */
    public ADLFileInputStream getReadStream() {
        return new ADLFileInputStream(filename, client);
    }

    /**
     * appends to an existing file.
     *
     * @return {@link ADLFileOutputStream} to write to. The contents written to this stream
     *         will be appended to the file.
     */
    public ADLFileOutputStream getAppendStream() {
        return new ADLFileOutputStream(filename, client, false, false);
    }

    /**
     * Concatenate the specified list of files into this file. The target should not exist.
     * The source files will be deleted if the concatenate succeeds.
     *
     *
     * @param fileList {@link List} of strings containing full pathnames of the files to concatenate.
     *                Cannot be null or empty.
     * @return returns true if the call succeeds
     * @throws ADLException {@link ADLException} is thrown if there is an error in concatenating files
     */
    public boolean concatenateFiles(List<String> fileList) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.concat(filename, fileList, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error concatenating files into " + filename);
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
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(int maxEntriesToRetrieve) throws ADLException  {
        return enumerateDirectory(maxEntriesToRetrieve, null, null);
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
     * @param startAfter the filename after which to begin enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String startAfter) throws ADLException  {
        return enumerateDirectory(0, startAfter, null);
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
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @param startAfter the filename after which to begin enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(int maxEntriesToRetrieve, String startAfter) throws ADLException  {
        return enumerateDirectory(maxEntriesToRetrieve, startAfter, null);
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
     * @param startAfter the filename after which to begin enumeration
     * @param endBefore the filename before which to end the enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(String startAfter, String endBefore) throws ADLException  {
        return enumerateDirectory(0, startAfter, endBefore);
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
     * @param maxEntriesToRetrieve maximum number of entries to retrieve. Note that server can limit the
     *                             number of entries retrieved to a number smaller than the number specified.
     * @param startAfter the filename after which to begin enumeration
     * @param endBefore the filename before which to end the enumeration
     * @return {@link List}&lt;{@link DirectoryEntry}&gt; containing the contents of the directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public List<DirectoryEntry> enumerateDirectory(int maxEntriesToRetrieve, String startAfter, String endBefore) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        List<DirectoryEntry> dirEnt  = Core.listStatus(filename, startAfter, endBefore, maxEntriesToRetrieve, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error enumerating directory " + filename);
        }
        return dirEnt;
    }

    /**
     * creates a directory, and all it's parent directories if they dont already exist.
     *
     * @return returns {@code true} if the call succeeded
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean createDirectory() throws ADLException  {
        return createDirectory(null);
    }

    /**
     * creates a directory, and all it's parent directories if they dont already exist.
     *
     * @param octalPermission permissions for the directory, as octal digits (for example, {@code "755"}). Can be null.
     * @return returns {@code true} if the call succeeded
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean createDirectory(String octalPermission) throws ADLException  {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.mkdirs(filename, octalPermission, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error creating directory " + filename);
        }
        return succeeded;
    }

    /**
     * deletes a directory and all it's child directories and files recursively.
     *
     * @return returns {@code true} if the call succeeded
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean deleteDirectoryTree() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.delete(filename, true, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error deleting directory tree " + filename);
        }
        return succeeded;
    }

    /**
     * removes all default acl entries from a directory. The access ACLs for the directory itself are
     * not modified.
     *
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void removeDefaultAcls() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeDefaultAcl(filename, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing default ACLs for directory " + filename);
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
     * @param newName the new name of the file
     *
     * @return returns {@code true} if the call succeeded
     *
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean rename(String newName) throws ADLException {
        return rename(newName, false);
    }

    /**
     * rename a file or directory.
     *
     * @param newName the new name of the file/directory
     * @param overwrite overwrite destination if it already exists. If the
     *                  destination is a non-empty directory, then the call
     *                  fails rather than overwrite the directory.
     *
     * @return returns {@code true} if the call succeeded
     *
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean rename(String newName, boolean overwrite) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.rename(filename, newName, overwrite, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error renaming file " + filename);
        }
        filename = newName;
        return succeeded;
    }

    /**
     * delete the file or directory. If called on a directory and the directory is not empty, then an
     * ADLException is thrown.
     *
     * @return returns {@code true} if the call succeeded
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean delete() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.delete(filename, false, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error deleting directory " + filename);
        }
        return succeeded;
    }

    /**
     * Gets the directory metadata about this file or directory.
     *
     * @return {@link DirectoryEntry} containing the metadata for the file/directory
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public DirectoryEntry getDirectoryEntry() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        DirectoryEntry dirEnt  = Core.getFileStatus(filename, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error getting info for file " + filename);
        }
        return dirEnt;
    }

    /**
     * sets the owning user and group of the file. If the user or group are {@code null}, then they are not changed.
     * It is illegal to pass both user and owner as {@code null}.
     *
     * @param owner the ID of the user, or {@code null}
     * @param group the ID of the group, or {@code null}
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void setOwner(String owner, String group) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setOwner(filename, owner, group, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting owner for file " + filename);
        }
    }

    /**
     * sets one or both of the times (Modified and Access time) of the file or directory
     *
     * @param atime Access time as a long
     * @param mtime Modified time as a long
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void setTimes(Date atime, Date mtime) throws ADLException {
        long atimeLong = (atime == null)? -1 : atime.getTime();
        long mtimeLong = (mtime == null)? -1 : mtime.getTime();

        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setTimes(filename, atimeLong, mtimeLong, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting times for file " + filename);
        }
    }

    /**
     * Sets the permissions of the specified file ro directory. This sets the traditional unix read/write/execute
     * permissions for the file/directory. To set Acl's use the
     *
     * {@link  #setAcl(List) setAcl} call.
     * @param octalPermissions the permissions to set, in unix octal form. For example, '644'.
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void setPermission(String octalPermissions) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setPermission(filename, octalPermissions, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting times for " + filename);
        }
    }

    /**
     * checks whether the calling user has the required permissions for the file/directory . The permissions
     * to check should be specified in the rwx parameter, as a unix permission string
     * (for example, {@code "r-x"}).
     *
     * @param rwx the permission to check for, in rwx string form. The call returns true if the caller has
     *            all the requested permissions. For example, specifying {@code "r-x"} succeeds if the caller has
     *            read and execute permissions.
     * @return true if the caller has the requested permissions, false otherwise
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean checkAccess(String rwx) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.checkAccess(filename, rwx, client, opts, resp);
        if (!resp.successful) {
            if (resp.httpResponseCode == 401 || resp.httpResponseCode == 403) return false;
            throw Core.getExceptionFromResp(resp, "Error checking access for " + filename);
        }
        return true;
    }

    /**
     * checks whether the calling user has read permissions for the file/directory
     *
     * @return true if the caller has read permissions, false otherwise
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean canRead() throws ADLException {
        return checkAccess("r--");
    }

    /**
     * checks whether the calling user has write permissions for the file/directory
     *
     * @return true if the caller has write permissions, false otherwise
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean canWrite() throws ADLException  {
        return checkAccess("-w-");
    }

    /**
     * checks whether the calling user has execute permissions for the file/directory
     * @return true if the caller has execute permissions, false otherwise
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public boolean canExecute() throws ADLException  {
        return checkAccess("--x");
    }

    /**
     * Modify the acl entries for a file or directory. This call merges the supplied list with
     * existing ACLs. If an entry with the same scope, type and user already exists, then the permissions
     * are replaced. If not, than an new ACL entry if added.
     *
     * @param aclSpec {@link List} of {@link AclEntry}s, containing the entries to add or modify
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void modifyAclEntries(List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.modifyAclEntries(filename, aclSpec, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error modifying ACLs for " + filename);
        }
    }

    /**
     * Sets the ACLs for a file or directory. If the file or directory already has any ACLs
     * associated with it, then all the existing ACLs are removed before adding the specified
     * ACLs.
     *
     * @param aclSpec {@link List} of {@link AclEntry}s, containing the entries to set
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void setAcl(List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setAcl(filename, aclSpec, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting ACLs for " + filename);
        }
    }

    /**
     * Removes the specified ACL entries from a file or directory.
     *
     * @param aclSpec {@link List} of {@link AclEntry}s to remove
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void removeAclEntries(List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeAclEntries(filename, aclSpec, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing ACLs for " + filename);
        }
    }

    /**
     * Removes all acl entries from a file or directory.
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public void removeAllAcls() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeAcl(filename, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing all ACLs for file " + filename);
        }
    }

    /**
     * Queries the ACLs and permissions for a file or directory.
     *
     * @return {@link AclStatus} object containing the ACL and permission info
     * @throws ADLException {@link ADLException} is thrown if there is an error
     */
    public AclStatus getAclStatus() throws ADLException {
        AclStatus status = null;
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        status = Core.getAclStatus(filename, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error getting  ACL Status for " + filename);
        }
        return status;
    }
}
