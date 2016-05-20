package com.microsoft.azure.datalake.store;

import java.time.LocalDateTime;

public class DirectoryEntry {
    public String name;
    public String group;
    public String user;
    public LocalDateTime lastAccessTime;
    public LocalDateTime lastModifiedTime;
    public DirectoryEntryType type;
    public String permission;
}

