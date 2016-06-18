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

    public ADLFileOutputStream createFromStream(boolean overwriteIfExists) {
        return new ADLFileOutputStream(filename, client, true, overwriteIfExists);
    }

    public ADLFileInputStream getReadStream() {
        return new ADLFileInputStream(filename, client);
    }

    public ADLFileOutputStream getAppendStream() {
        return new ADLFileOutputStream(filename, client, false, false);
    }

    public boolean concatenateFiles(List<String> fileList, boolean deleteSourceDirectory) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.concat(filename, fileList, deleteSourceDirectory, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error concatenating files into " + filename);
        }
        return true;
    }

    public void rename(String newName) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.rename(filename, newName, client, opts, resp);
        if (!resp.successful || !succeeded) {
            throw Core.getExceptionFromResp(resp, "Error renaming file " + filename);
        }
        filename = newName;
    }

    public void delete() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.delete(filename, false, client, opts, resp);
        if (!resp.successful || !succeeded) {
            throw Core.getExceptionFromResp(resp, "Error deleting directory " + filename);
        }
    }

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

    public void setOwner(String owner, String group) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setOwner(filename, owner, group, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting owner for file " + filename);
        }
    }

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

    public void setPermission(String octalPermissions) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setPermission(filename, octalPermissions, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting times for file " + filename);
        }
    }

    public boolean checkAccess(String rwx) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.checkAccess(filename, rwx, client, opts, resp);
        if (!resp.successful) {
            if (resp.httpResponseCode == 401 || resp.httpResponseCode == 403) return false;
            throw Core.getExceptionFromResp(resp, "Error checking access for file " + filename);
        }
        return true;
    }

    public boolean canRead() throws ADLException {
        return checkAccess("r--");
    }

    public boolean canWrite() throws ADLException  {
        return checkAccess("-w-");
    }

    public boolean canExecute() throws ADLException  {
        return checkAccess("--x");
    }

    public void modifyAclEntries(List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.modifyAclEntries(filename, aclSpec, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error modifying ACLs for file " + filename);
        }
    }

    public void removeAclEntries(List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeAclEntries(filename, aclSpec, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing ACLs for file " + filename);
        }
    }

    public void removeAllAcls() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeAcl(filename, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing all ACLs for file " + filename);
        }
    }

    public void replaceAllAcls(List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setAcl(filename, aclSpec, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error replacing ACLs for file " + filename);
        }
    }

    public AclStatus getAclStatus() throws ADLException {
        AclStatus status = null;
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        status = Core.getAclStatus(filename, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error getting  ACL Status for file " + filename);
        }
        return status;
    }
}
