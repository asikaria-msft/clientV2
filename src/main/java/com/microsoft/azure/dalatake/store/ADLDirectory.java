package com.microsoft.azure.dalatake.store;

import java.io.InputStream;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;


public class ADLDirectory {

    // no constructor - use factory method in AzureDatalakeStorageClient
    private ADLDirectory() { }


    public boolean ExistsOnServer() {
        return true;
    }

    public List<DirectoryEntry> Enumerate(int maxEntriesToRetrieve) {
        return null;
    }

    public List<DirectoryEntry> Enumerate(int maxEntriesToRetrieve, String startAfter) {
        return null;
    }

    public ADLFileOutputStream Create() {
        return null;
    }

    public boolean Rename(String newName) {
        return true;
    }

    public boolean GetDirectoryInformation() {
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
