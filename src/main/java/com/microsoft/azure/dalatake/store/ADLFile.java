package com.microsoft.azure.dalatake.store;

import java.io.InputStream;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;


public class ADLFile {

    // no constructor - use factory method in AzureDatalakeStorageClient
    private ADLFile() { }

    public ADLFileOutputStream Create(boolean overwrite) {
        return null;
    }

    public boolean CreateEmptyFile(boolean overwrite) {
        return true;
    }

    public ADLFileInputStream OpenForRead() {
            return null;
    }

    public ADLFileOutputStream OpenForAppend() {
        return null;
    }


    /**
     * Does an atomic append to the file - the append either succeeds fully, or fails
     * fully - the contents are not partially appended.
     *
     * @param bytesToAppend
     *          the byte buffer to append. Max buffer size can be 4MB.
     * @return returns true on success
     */
    public boolean AppendBytes(byte[] bytesToAppend) {
        return true;
    }

    public boolean AppendBytes(byte[] bytesToAppend, long fileOffset) {
        return true;
    }

    /**
     * Atomically creates a file with specified contents from byte buffer
     * The request either succeeds (in which case the file is created and
     * all the content is in the file), or fails (in which case the file is not
     * created).
     *
     * @param bytesToAppend
     *          the byte buffer to append. Max buffer size can be 4MB.
     * @param overwrite
     *          if true, then the file overwrites any existing file. If false,
     *          then the request fails if a file by the same name already exists.
     * @return returns true on success
     */
    public boolean AtomicCreate(byte[] bytesToAppend, boolean overwrite) {
        return true;
    }

    public boolean Upload(String fileName, boolean overwrite)  {
        return true;
    }

    public boolean Upload(InputStream stream, boolean overwrite)  {
        return true;
    }

    public boolean Upload(byte[] bytesToUpload, boolean overwrite) {
        return false;
    }

    public boolean UploadFromUrl(URI url, boolean overwrite) {
        return true;
    }

    public boolean ConcatenateFiles(List<String> fileList){
        return true;
    }

    public boolean Rename(String newName) {
        return true;
    }

    public boolean GetFileInformation() {
        return true;
    }

    public long GetLength() {
        return 0;
    }

    public String GetName() {
        return null;
    }

    public LocalDateTime GetLastAccessTime() {
        return null;
    }

    public LocalDateTime GetLastModificationTime() {
        return null;
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

    public List<String> GetAcls() {
        return null;
    }

    public boolean ReplaceAcls(String AclSpec) {
        return true;
    }

    public boolean AddAcls(String AclSpec) {
        return true;
    }

    public boolean RemoveAcls(String AclSpec) {
        return true;
    }

    public boolean SetOwningUser(String owner) {
        return true;
    }

    public boolean SetOwningGroup(String group) {
        return true;
    }

    public boolean SetOwner(String owner, String group) {
        return true;
    }
}
