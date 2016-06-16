package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialOnThrottlePolicy;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;

import java.io.*;
import java.net.URI;

/**
 * Utility methods to enable one-liners for simple functionality.
 *
 * The methods are all based on calls to the SDK methods, these are
 * just convenience methods for common tasks.
 *
 *
 *
 */
public class Utils {

    private AzureDataLakeStorageClient client;

    Utils(AzureDataLakeStorageClient client) {
        this.client = client;
    }

    public boolean checkDirectoryExists(String directoryName) throws ADLException {
        if (directoryName == null || directoryName.trim().equals(""))
            throw new IllegalArgumentException("directory name cannot be null");

        ADLDirectoryInfo di = client.getDirectoryInfo(directoryName);
        try {
            di.getDirectoryEntry();
        } catch (ADLException ex) {
            if (ex.httpResponseCode == 404) return false;
            else throw ex;
        }
        return true;
    }

    public boolean checkFileExists(String filename) throws ADLException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");

        ADLFileInfo fi = client.getFileInfo(filename);
        try {
            fi.getDirectoryEntry();
        } catch (ADLException ex) {
            if (ex.httpResponseCode == 404) return false;
            else throw ex;
        }
        return true;
    }

    public void createDirectory(String directoryName) throws ADLException {
        if (directoryName == null || directoryName.trim().equals(""))
            throw new IllegalArgumentException("directory name cannot be null");

        ADLDirectoryInfo di = client.getDirectoryInfo(directoryName);
        di.create();
    }

    public void createEmptyFile(String filename, boolean overwriteIfExists) throws IOException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");

        ADLFileInfo fi = client.getFileInfo(filename);
        OutputStream out = fi.createFromStream(overwriteIfExists);
        out.close();
    }

    public void upload(String filename, String localFilename, boolean overwriteIfExists) throws IOException  {
        if (localFilename == null || localFilename.trim().equals(""))
            throw new IllegalArgumentException("localFilename cannot be null");

        FileInputStream in = new FileInputStream(localFilename);
        upload(filename, in, overwriteIfExists);
    }

    public void upload(String filename, InputStream in, boolean overwriteIfExists) throws IOException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");
        if (in == null) throw new IllegalArgumentException("InputStream cannot be null");

        ADLFileInfo fi = client.getFileInfo(filename);
        OutputStream out = fi.createFromStream(overwriteIfExists);
        byte[] buffer = new byte[4 * 1000 * 1000];
        int n;

        while ((n=in.read(buffer)) != -1) {
            out.write(buffer, 0, n);
        }
        out.close();
        in.close();
    }


    /**
     * Does an atomic append to the file - the append either succeeds fully, or fails
     * fully - the contents are not partially appended.
     *
     * @param filename name of file to append to
     * @param bytesToAppend
     *          the byte buffer to append. Max buffer size can be 4MB.
     * @return returns true on success
     */
    public void appendBytes(String filename, byte[] bytesToAppend) throws IllegalArgumentException, ADLException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");
        if (bytesToAppend == null || bytesToAppend.length == 0)  return; // nothing to append
        if (bytesToAppend.length > 4*1024*1024) throw new IllegalArgumentException("maximum of 4MB can be appended in one request");

        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        OperationResponse resp = new OperationResponse();
        Core.concurrentAppend(filename, bytesToAppend, 0, bytesToAppend.length, true, client, opts, resp);
        if (!resp.successful) {
            throw Core.getExceptionFromResp(resp, "Error appending to file " + filename);
        }
    }
}
