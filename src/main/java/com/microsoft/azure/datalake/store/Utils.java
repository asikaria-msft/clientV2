package com.microsoft.azure.datalake.store;

import java.io.InputStream;
import java.net.URI;

public class Utils {

    private AzureDataLakeStorageClient client;

    Utils(AzureDataLakeStorageClient client) {
        this.client = client;
    }

    public boolean checkDirectoryExists(String directoryName) {
        return true;
    }

    public boolean checkFileExists(String fileName) {
        return true;
    }

    public boolean createDirectory(String directoryName) {
        return true;
    }

    public boolean createEmptyFile(boolean overwriteIfExists) {
        return true;
    }

    public boolean upload(String fileName, String localFileName, boolean overwriteIfExists)  {
        return true;
    }

    public boolean upload(String fileName, InputStream stream, boolean overwriteIfExists)  {
        return true;
    }

    public boolean uploadFromUrl(String fileName, URI sourceUri, boolean overwriteIfExists) {
        return true;
    }

    /**
     * Does an atomic append to the file - the append either succeeds fully, or fails
     * fully - the contents are not partially appended.
     *
     * @param bytesToAppend
     *          the byte buffer to append. Max buffer size can be 4MB.
     * @return returns true on success
     */
    public boolean appendBytes(String fileName, byte[] bytesToAppend) {
        return true;
    }

    public boolean appendBytes(String fileName, byte[] bytesToAppend, long fileOffset) {
        return true;
    }

    /**
     * Atomically creates a file with specified contents from byte buffer
     * The request either succeeds (in which case the file is created and
     * all the content is in the file), or fails (in which case the file is not
     * created).
     *
     * @param contents
     *          the byte buffer to append. Max buffer size can be 4MB.
     * @param overwriteIfExists
     *          if true, then the file overwrites any existing file. If false,
     *          then the request fails if a file by the same name already exists.
     * @return returns true on success
     */
    @Deprecated
    public boolean createFileWithContents(String fileName, byte[] contents, boolean overwriteIfExists) {
        return true;
    }

}
