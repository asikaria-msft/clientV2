/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;

import java.io.*;

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

    private ADLStoreClient client;

    Utils(ADLStoreClient client) {
        this.client = client;
    }

    /**
     * Check that a file or directory exists.
     *
     * @param filename path to check
     * @return true if the path exists, false otherwise
     * @throws ADLException thrown on error
     */
    public boolean checkExists(String filename) throws ADLException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");

        try {
            client.getDirectoryEntry(filename);
        } catch (ADLException ex) {
            if (ex.httpResponseCode == 404) return false;
            else throw ex;
        }
        return true;
    }

    /**
     * Creates a directory.
     *
     * @param directoryName name of the directory to create.
     * @throws ADLException thrown on error
     */
    public void createDirectory(String directoryName) throws ADLException {
        if (directoryName == null || directoryName.trim().equals(""))
            throw new IllegalArgumentException("directory name cannot be null");

        boolean succeeded = client.createDirectory(directoryName);
        if (!succeeded) {
            OperationResponse resp = new OperationResponse();
            throw Core.getExceptionFromResp(resp, "Error creating directory " + directoryName);
        }
    }

    /**
     * Creates an empty file.
     *
     * @param filename name of file to create.
     * @throws IOException thrown on error
     */
    public void createEmptyFile(String filename) throws IOException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");

        OutputStream out = client.createFromStream(filename, IfExists.FAIL);
        out.close();
    }

    /**
     * Uploads the contents of a local file to an Azure Data Lake file.
     *
     * @param filename path of file to upload to
     * @param localFilename path to local file
     * @param mode {@link IfExists} {@code enum} specifying whether to overwite or throw
     *                             an exception if the file already exists
     * @throws IOException thrown on error
     */
    public void upload(String filename, String localFilename, IfExists mode) throws IOException  {
        if (localFilename == null || localFilename.trim().equals(""))
            throw new IllegalArgumentException("localFilename cannot be null");

        FileInputStream in = new FileInputStream(localFilename);
        upload(filename, in, mode);
    }

    /**
     * Uploads an {@link InputStream} to an Azure Data Lake file.
     *
     * @param filename path of file to upload to
     * @param in {@link InputStream} whose contents will be uploaded
     * @param mode {@link IfExists} {@code enum} specifying whether to overwite or throw
     *                             an exception if the file already exists
     * @throws IOException thrown on error
     */
    public void upload(String filename, InputStream in, IfExists mode) throws IOException {
        if (filename == null || filename.trim().equals(""))
            throw new IllegalArgumentException("filename cannot be null");
        if (in == null) throw new IllegalArgumentException("InputStream cannot be null");

        ADLFileOutputStream out = client.createFromStream(filename, mode);
        int bufSize = 4 * 1000 * 1000;
        out.setBufferSize(bufSize);
        byte[] buffer = new byte[bufSize];
        int n;

        while ((n=in.read(buffer)) != -1) {
            out.write(buffer, 0, n);
        }
        out.close();
        in.close();
    }

    /**
     * Does an atomic append to the file - the append either succeeds fully, or fails
     * fully - the contents are not partially appended. The offset to append at
     * is determined by the server.
     *
     *
     * @param filename name of file to append to
     * @param bytesToAppend
     *          the byte buffer to append. Max buffer size can be 4MB (4*1024*1024).
     * @throws ADLException thrown if there is an error in upload
     * @throws IllegalArgumentException thrown if the buffer provided is larger than 4MB, or input filename is null
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
