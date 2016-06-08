package com.microsoft.azure.datalake.store;

import java.util.Date;

/**
 * filesystem metadata of a directory enrty (a file or a directory) in ADL.
 */
public class DirectoryEntry {

    /**
     * the filename (minus the path) of the direcotry entry
     */
    public final String name;

    /**
     * the full path of the directory enrty.
     */
    public final String fullName;

    /**
     * the length of a file. zero for directories.
     */
    public final long length;

    /**
     * the ID of the group that owns this file/directory.
     */
    public final String group;

    /**
     * the ID of the user that owns this file/directory.
     */
    public final String user;

    /**
     * the timestamp of the last time the file was accessed
     */
    public final Date lastAccessTime;

    /**
     * the timestamp of the last time the file was modified
     */
    public final Date lastModifiedTime;

    /**
     * {@link DirectoryEntryType} enum indicating whether the object is a file or a directory
     */
    public final DirectoryEntryType type;

    /**
     * the unix-style permission string for this file or directory
     */
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

