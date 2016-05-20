package com.microsoft.azure.datalake.store;

import java.time.LocalDateTime;
import java.util.List;


public class ADLDirectoryInfo {

    // no constructor - use factory method in AzureDatalakeStorageClient
    private ADLDirectoryInfo() { }


    public boolean existsOnServer() {
        return true;
    }

    public List<DirectoryEntry> enumerate(int maxEntriesToRetrieve) {
        return null;
    }

    public List<DirectoryEntry> enumerate(int maxEntriesToRetrieve, String startAfter) {
        return null;
    }

    public boolean create() {
        return true;
    }

    public boolean rename(String newName) {
        return true;
    }

    public boolean updateInfoFromServer() {
        return true;
    }

    public long getLength() {
        return 0;
    }

    public String getName() {
        return null;
    }

    public LocalDateTime getLastAccessTime() {
        return null;
    }

    public LocalDateTime getLastModificationTime() {
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

    public String getOwningUser() {
        return null;
    }

    public boolean setOwningUser(String owner) {
        return true;
    }

    public String getOwningGroup() {
        return null;
    }

    public boolean setOwningGroup(String group) {
        return true;
    }

    public boolean setOwner(String owner, String group) {
        return true;
    }
}
