/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.acl;

public enum AclAction {
    NONE          ("---"),
    EXECUTE       ("--x"),
    WRITE         ("-w-"),
    WRITE_EXECUTE ("-wx"),
    READ          ("r--"),
    READ_EXECUTE  ("r-x"),
    READ_WRITE    ("rw-"),
    ALL           ("rwx");

    private final String rwx;
    private static final AclAction[] values = AclAction.values();

    AclAction(String rwx) {
        this.rwx = rwx;
    }

    public String toString() {
        return this.rwx;
    }

    public static String toString(AclAction action) {
        return action.rwx;
    }

    public static AclAction fromRwx(String rwx) {
        if (rwx==null) throw new IllegalArgumentException("access specifier is null");
        rwx = rwx.trim().toLowerCase();
        for (AclAction a: values) {
            if (a.rwx.equals(rwx)) { return a; }
        }
        throw new IllegalArgumentException(rwx + " is not a valid access specifier");
    }

    public static boolean isValidRwx(String input) {
        try {
            fromRwx(input);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    public static AclAction fromOctal(int perm) {
        if (perm <0 || perm>7) throw new IllegalArgumentException(perm + " is not a valid access specifier");
        return values[perm];
    }

    public int toOctal() {
        return this.ordinal();
    }

    public static int toOctal(AclAction action) {
        return action.ordinal();
    }


}
