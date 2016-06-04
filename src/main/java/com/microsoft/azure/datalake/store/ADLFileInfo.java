package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.DefaultRetryPolicy;

import java.util.List;


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
        opts.retryPolicy = new DefaultRetryPolicy();
        OperationResponse resp = new OperationResponse();
        Core.concat(filename, fileList, deleteSourceDirectory, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error concatenating files into " + filename);
        }
        return true;
    }

    public void rename(String newName) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new DefaultRetryPolicy();
        OperationResponse resp = new OperationResponse();
        Core.rename(filename, newName, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error renaming file " + filename);
        }
        filename = newName;
    }

    public DirectoryEntry getDirectoryEntry() throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new DefaultRetryPolicy();
        OperationResponse resp = new OperationResponse();
        DirectoryEntry dirEnt  = Core.getFileStatus(filename, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error getting info for file " + filename);
        }
        return dirEnt;
    }

    public void setOwner(String owner, String group) throws ADLException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new DefaultRetryPolicy();
        OperationResponse resp = new OperationResponse();
        Core.setOwner(filename, owner, group, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error setting owner for file " + filename);
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
