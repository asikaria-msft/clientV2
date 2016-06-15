package com.microsoft.azure.datalake.store.acl;


import java.util.List;

public class AclStatus {
    public List<AclEntry> aclSpec;
    public String owner;
    public String group;
    public String octalPermissions;
}
