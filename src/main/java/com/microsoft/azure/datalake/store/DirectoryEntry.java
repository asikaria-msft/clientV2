package com.microsoft.azure.datalake.store;

import java.util.Date;

public class DirectoryEntry {
    public final String name;
    public final String fullName;
    public final long length;
    public final String group;
    public final String user;
    public final Date lastAccessTime;
    public final Date lastModifiedTime;
    public final DirectoryEntryType type;
    public final String permission;

    public DirectoryEntry(String name,
                   String fullName,
                   long length,
                   String group,
                   String user,
                   Date lastAccessTime,
                   Date lastModifiedTime,
                   DirectoryEntryType type,
                   String permission) {
        this.name = name;
        this.fullName = fullName;
        this.length = length;
        this.group = group;
        this.user = user;
        this.lastAccessTime = lastAccessTime;
        this.lastModifiedTime = lastModifiedTime;
        this.type = type;
        this.permission = permission;
    }
}

