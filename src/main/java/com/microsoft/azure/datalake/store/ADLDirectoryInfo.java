package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialOnThrottlePolicy;

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
        Core.mkdirs(dirname, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error creating directory " + dirname);
        }
    }

    public boolean rename(String newName) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.rename(dirname, newName, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error renaming directory " + dirname);
        }
        dirname = newName;
        return true;
    }

    public void delete(boolean recursive) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        Core.delete(dirname, recursive, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error deleting directory " + dirname);
        }
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


    public boolean canRead() {
        return true;
    }

    public boolean canWrite() {
        return true;
    }

    public boolean canExecute() {
        return true;
    }

    public List<String> getAcls() {
        return null;
    }

    public boolean replaceAcls(List<String> aclSpec) {
        return true;
    }

    public boolean addAcls(List<String> aclSpec) {
        return true;
    }

    public boolean removeAcls(List<String> aclSpec) {
        return true;
    }



}
