package com.microsoft.azure.datalake.store;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class ADLAcl {
    private String ownerAccess = null;
    private String groupAccess = null;
    private String otherAccess = null;
    private String mask = null;
    private Map<String, String> namedUserAcl = null;
    private Map<String, String> namedGroupAcl= null;
    private ADLAcl defaultAcl = null;



    public String getOwnerAccess() {
        return ownerAccess;
    }

    public void setOwnerAccess(String ownerAccess) {
        if (isValidRwx(ownerAccess)) {
            this.ownerAccess = ownerAccess.trim().toLowerCase();
        } else {
            throw new IllegalArgumentException("Incorrect access specifier: " + ownerAccess);
        }
    }

    public String getOtherAccess() {
        return otherAccess;
    }

    public void setOtherAccess(String otherAccess) {
        if (isValidRwx(otherAccess)) {
            this.otherAccess = otherAccess.trim().toLowerCase();
        } else {
            throw new IllegalArgumentException("Incorrect access specifier: " + otherAccess);
        }
    }

    public String getGroupAccess() {
        return groupAccess;
    }

    public void setGroupAccess(String groupAccess) {
        if (isValidRwx(groupAccess)) {
            this.groupAccess = groupAccess.trim().toLowerCase();
        } else {
            throw new IllegalArgumentException("Incorrect access specifier: " + groupAccess);
        }
    }

    public String getMask() {
        return mask;
    }

    public void setMask(String mask) {
        if (isValidRwx(mask)) {
            this.mask = mask.trim().toLowerCase();
        } else {
            throw new IllegalArgumentException("Incorrect access specifier: " + mask);
        }
    }

    public Map<String, String> getNamedUserAcl() {
        return namedUserAcl;
    }

    public void setNamedUserAcl(Map<String, String> namedUserAcl) {
        for (String rwx : namedUserAcl.values()) {
            if (!isValidRwx(rwx)) throw new IllegalArgumentException("Incorrect access specifier: " + rwx);
        }
        this.namedUserAcl = namedUserAcl;
    }

    public Map<String, String> getNamedGroupAcl() {
        return namedGroupAcl;
    }

    public void setNamedGroupAcl(Map<String, String> namedGroupAcl) {
        for (String rwx : namedGroupAcl.values()) {
            if (!isValidRwx(rwx)) throw new IllegalArgumentException("Incorrect access specifier: " + rwx);
        }
        this.namedGroupAcl = namedGroupAcl;
    }

    public ADLAcl getDefaultAcl() {
        return defaultAcl;
    }

    public void setDefaultAcl(ADLAcl defaultAcl) {
        this.defaultAcl = defaultAcl;
    }

    private static final Pattern rwxPattern = Pattern.compile("[r-][w-][x-]");
    public static boolean isValidRwx(String input) {
        input = input.trim().toLowerCase();
        return rwxPattern.matcher(input).matches();
    }

    private static final Pattern fullRwxPattern = Pattern.compile("[r-][w-][x-][r-][w-][x-][r-][w-][x-]");
    public static ADLAcl fromOldPermissions(String rwxAccessString) {
        rwxAccessString = rwxAccessString.trim().toLowerCase();
        if (!fullRwxPattern.matcher(rwxAccessString).matches()) throw new IllegalArgumentException("Incorrect access specifier: " + rwxAccessString);

        String user = rwxAccessString.substring(0,2);
        String group = rwxAccessString.substring(3,5);
        String other = rwxAccessString.substring(6,8);

        ADLAcl acl = new ADLAcl();
        acl.setOwnerAccess(user);
        acl.setGroupAccess(group);
        acl.setMask(group);
        acl.setOtherAccess(other);
        return acl;
    }

    private static final Pattern octalPattern = Pattern.compile("[0-7][0-7][0-7]");
    public static ADLAcl fromOldOctalPermissions(String octalAccessPermissions) {
        octalAccessPermissions = octalAccessPermissions.trim();
        if (!octalPattern.matcher(octalAccessPermissions).matches()) throw new IllegalArgumentException("Incorrect access specifier: " + octalAccessPermissions);

        String user = octalAccessPermissions.substring(0,0);
        String group = octalAccessPermissions.substring(1,1);
        String other = octalAccessPermissions.substring(2,2);

        ADLAcl acl = new ADLAcl();
        acl.setOwnerAccess(mapping.get(user));
        acl.setGroupAccess(mapping.get(group));
        acl.setMask(mapping.get(group));
        acl.setOtherAccess(mapping.get(other));
        return acl;
    }

    private static HashMap<String, String> mapping = new HashMap<String, String>();
    // Static initializer, runs once at class loading
    static {
        mapping.put("0", "---");
        mapping.put("1", "--x");
        mapping.put("2", "-w-");
        mapping.put("3", "-wx");
        mapping.put("4", "r--");
        mapping.put("5", "r-x");
        mapping.put("6", "rw-");
        mapping.put("7", "rwx");
    }


    public static ADLAcl fromAclString(String aclString) {
        // TODO: Not implemented yet
        return null;
    }

    // covert to string representation
    public String toString() {
        // TODO: Not implemented yet
        return null;
    }
}
