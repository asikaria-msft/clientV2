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
 * A local Java object that represents a directory on the server.
 * <P>
 * Methods on this class enable manipulation of the directory's
 * contents and attributes.
 * </P>
 */
public class ADLDirectoryInfo {

    private final AzureDataLakeStorageClient client;
    private String dirname = null;  // cannot be marked final - rename can change it

    // package-private constructor - use factory method in AzureDataLakeStorageClient
    ADLDirectoryInfo(AzureDataLakeStorageClient client, String dirname) {
        this.client = client;
        this.dirname = dirname;
    }

    public List<DirectoryEntry> enumerate(int maxEntriesToRetrieve) throws ADLException  {
        return enumerate(maxEntriesToRetrieve, null, null);
    }

    public List<DirectoryEntry> enumerate(String startAfter) throws ADLException  {
        return enumerate(0, startAfter, null);
    }

    public List<DirectoryEntry> enumerate(int maxEntriesToRetrieve, String startAfter) throws ADLException  {
        return enumerate(maxEntriesToRetrieve, startAfter, null);
    }

    public List<DirectoryEntry> enumerate(String startAfter, String endBefore) throws ADLException  {
        return enumerate(0, startAfter, endBefore);
    }

    public List<DirectoryEntry> enumerate(int maxEntriesToRetrieve, String startAfter, String endBefore) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        List<DirectoryEntry> dirEnt  = Core.listStatus(dirname, startAfter, endBefore, maxEntriesToRetrieve, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error enumerating directory " + dirname);
        }
        return dirEnt;
    }

    public void create() throws ADLException  {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.mkdirs(dirname, client, opts, resp);
        if (!resp.successful || !succeeded) {
            throw Core.getExceptionFromResp(resp, "Error creating directory " + dirname);
        }
    }

    public void delete(boolean recursive) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.delete(dirname, recursive, client, opts, resp);
        if (!resp.successful || !succeeded) {
            throw Core.getExceptionFromResp(resp, "Error deleting directory " + dirname);
        }
    }

    public void removeDefaultAcls() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeDefaultAcl(dirname, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing default ACLs for directory " + dirname);
        }
    }

    public boolean rename(String newName) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        boolean succeeded = Core.rename(dirname, newName, client, opts, resp);
        if (!resp.successful || !succeeded) {
            throw Core.getExceptionFromResp(resp, "Error renaming directory " + dirname);
        }
        dirname = newName;
        return true;
    }

    public DirectoryEntry getDirectoryEntry() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        DirectoryEntry dirEnt  = Core.getFileStatus(dirname, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error getting info for directory " + dirname);
        }
        return dirEnt;
    }


    public void setOwner(String owner, String group) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setOwner(dirname, owner, group, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting owner for directory " + dirname);
        }
    }

    public void setTimes(Date atime, Date mtime) throws ADLException {
        long atimeLong = (atime == null)? -1 : atime.getTime();
        long mtimeLong = (mtime == null)? -1 : mtime.getTime();

        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setTimes(dirname, atimeLong, mtimeLong, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting times for directory " + dirname);
        }
    }

    public void setPermission(String octalPermissions) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setPermission(dirname, octalPermissions, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting times for directory " + dirname);
        }
    }

    public boolean checkAccess(String rwx) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.checkAccess(dirname, rwx, client, opts, resp);
        if (!resp.successful) {
            if (resp.httpResponseCode == 401 || resp.httpResponseCode == 403) return false;
            throw Core.getExceptionFromResp(resp, "Error checking access for directory " + dirname);
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
        Core.modifyAclEntries(dirname, aclSpec, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error modifying ACLs for directory " + dirname);
        }
    }

    public void removeAclEntries(List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeAclEntries(dirname, aclSpec, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing ACLs for directory " + dirname);
        }
    }

    public void removeAllAcls() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.removeAcl(dirname, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error removing all ACLs for directory " + dirname);
        }
    }



    public void replaceAllAcls(List<AclEntry> aclSpec) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.setAcl(dirname, aclSpec, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error replacing ACLs for directory " + dirname);
        }
    }

    public AclStatus getAclStatus() throws ADLException {
        AclStatus status = null;
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        status = Core.getAclStatus(dirname, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error getting  ACL Status for directory " + dirname);
        }
        return status;
    }
}
