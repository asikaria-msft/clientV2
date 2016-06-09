package com.microsoft.azure.datalake.store;

import java.io.InputStream;
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
     * @param fileName name of file to append to
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


}
