package com.microsoft.azure.datalake.store.acl;

import java.util.LinkedList;
import java.util.List;

public class AclEntry {
    public AclScope scope;
    public AclType type;
    public String name;
    public AclAction action;

    public AclEntry() {

    }

    public AclEntry(AclScope scope, AclType type, String name, AclAction action) {
        if (scope == null) throw new IllegalArgumentException("AclScope is null");
        if (type == null ) throw new IllegalArgumentException("AclType is null");
        if (type == AclType.MASK && name != null && !name.trim().equals(""))
                throw new IllegalArgumentException("mask should not have user/group name");
        if (type == AclType.OTHER && name != null && !name.trim().equals(""))
            throw new IllegalArgumentException("ACL entry type 'other' should not have user/group name");

        this.scope = scope;
        this.type = type;
        this.name = name;
        this.action = action;
    }

    public static AclEntry parseAclEntry(String entryString) throws IllegalArgumentException {
        return parseAclEntry(entryString, false);
    }

    public static AclEntry parseAclEntry(String entryString, boolean removeAcl) throws IllegalArgumentException {
        if (entryString == null || entryString.equals("")) return null;
        AclEntry aclEntry = new AclEntry();
        String aclString = entryString.trim();

        // check if this is default ACL
        int fColonPos = aclString.indexOf(":");
        String firstToken = aclString.substring(0, fColonPos).toLowerCase().trim();
        if (firstToken.equals("default")) {
            aclEntry.scope = com.microsoft.azure.datalake.store.acl.AclScope.DEFAULT;
            aclString = aclString.substring(fColonPos+1);
        } else {
            aclEntry.scope = com.microsoft.azure.datalake.store.acl.AclScope.ACCESS;
        }

        // remaining string should have 3 entries (or 2 for removeacl)
        String[] parts = aclString.split(":");
        if (parts.length <2 || parts.length >3) throw new IllegalArgumentException("invalid aclEntryString " + entryString);
        if (parts.length == 2 && !removeAcl) throw new IllegalArgumentException("invalid aclEntryString " + entryString);

        // entry type (user/group/other/mask)
        try {
            aclEntry.type = AclType.valueOf(parts[0].toUpperCase().trim());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Invalid ACL AclType in " + entryString);
        } catch (NullPointerException ex) {
            throw new IllegalArgumentException("ACL Entry AclType missing in " + entryString);
        }

        // user/group name
        aclEntry.name = parts[1].trim();
        if (aclEntry.type == AclType.MASK && !aclEntry.name.equals(""))
                throw new IllegalArgumentException("mask entry cannot contain user/group name: " + entryString);
        if (aclEntry.type == AclType.OTHER && !aclEntry.name.equals(""))
            throw new IllegalArgumentException("entry of type 'other' should not contain user/group name: " + entryString);

        // permission (rwx)
        if (!removeAcl) {
            try {
                aclEntry.action = AclAction.fromRwx(parts[2]);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Invalid ACL action in " + entryString);
            } catch (NullPointerException ex) {
                throw new IllegalArgumentException("ACL action missing in " + entryString);
            }
        }
        return aclEntry;
    }

    public String toString() {
        return this.toString(false);
    }

    public String toString(boolean removeAcl) {
        StringBuilder str = new StringBuilder();
        if (this.scope == null) throw new IllegalArgumentException("Acl Entry has no scope");
        if (this.type == null) throw new IllegalArgumentException("Acl Entry has no type");

        if (this.scope == com.microsoft.azure.datalake.store.acl.AclScope.DEFAULT) str.append("default:");

        str.append(this.type.toString().toLowerCase());
        str.append(":");

        str.append(this.name);

        if (this.action != null && !removeAcl) {
            str.append(":");
            str.append(this.action.toString());
        }
        return str.toString();
    }

    public static List<AclEntry> parseAclSpec(String aclString) throws IllegalArgumentException {
        if (aclString == null || aclString.trim().equals("")) return new LinkedList<AclEntry>();

        aclString = aclString.trim();
        String car,  // the first entry
                cdr;  // the rest of the list fater first entry
        int commaPos = aclString.indexOf(",");
        if (commaPos < 0) {
            car = aclString;
            cdr = null;
        } else {
            car = aclString.substring(0, commaPos).trim();
            cdr = aclString.substring(commaPos+1);
        }
        LinkedList<AclEntry> aclSpec = (LinkedList<AclEntry>) parseAclSpec(cdr);
        if (!car.equals("")) {
            aclSpec.addFirst(parseAclEntry(car));
        }
        return aclSpec;
    }

    public static String aclListToString(List<AclEntry> list) {
        return aclListToString(list, false);
    }

    public static String aclListToString(List<AclEntry> list, boolean removeAcl) {
        if (list == null || list.size() == 0) return "";
        String separator = "";
        StringBuilder output = new StringBuilder();

        for (AclEntry entry : list) {
            output.append(separator);
            output.append(entry.toString(removeAcl));
            separator = ",";
        }
        return output.toString();
    }
}
