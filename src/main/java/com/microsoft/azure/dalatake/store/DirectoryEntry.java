package com.microsoft.azure.dalatake.store;

import java.time.LocalDateTime;

public class DirectoryEntry {
    public String Name;
    public String Group;
    public String User;
    public LocalDateTime LastAccessTime;
    public LocalDateTime LastModifiedTime;
    public DirectoryEntryType Type;
    public String Permission;
}

