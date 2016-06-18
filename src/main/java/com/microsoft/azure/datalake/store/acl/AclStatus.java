/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.acl;


import java.util.List;

public class AclStatus {
    public List<AclEntry> aclSpec;
    public String owner;
    public String group;
    public String octalPermissions;
}
