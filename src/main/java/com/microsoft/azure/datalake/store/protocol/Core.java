/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.protocol;


import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.AzureDataLakeStorageClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;
import com.microsoft.azure.datalake.store.acl.AclEntry;
import com.microsoft.azure.datalake.store.acl.AclStatus;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * protocol.Core class implements the calls for the RESP API. There is one method in Core for every
 * REST API supported by the server.
 * <P>
 * The methods in this class tend to be lower-level, exposing all the details of the underlying operation.
 * To call the methods, instantiate a {@link RequestOptions} object first. Assign any of the
 * member values as needed (e.g., the RetryPolicy). Then create a new {@link OperationResponse} object. The
 * {@link OperationResponse} is used for passing the call results and stats back from the call.
 * </P><P>
 * Failures originating in Core methods are communicated back through the {@link OperationResponse} parameter,
 * not through exceptions. There is a convenience method ({@link #getExceptionFromResp(OperationResponse, String) getExceptionFromResp})
 * to generate an exception from the response, if the response indicates a failure.
 * </P><P>
 * <B>Thread Safety: </B> all static methods in this class are thread-safe
 *
 * </P>
 */
public class Core {

    // no constructor - class has static methods only
    private Core() {}



    /**
     * create a file and write to it.
     *
     *
     * @param path the full path of the file to create
     * @param overwrite whether to overwrite the file if it already exists
     * @param contents byte array containing the contents to be written to the file. Can be {@code null}
     * @param offsetWithinContentsArray offset within the byte array passed in {@code contents}. Bytes starting
     *                                  at this offset will be written to server
     * @param length number of bytes from {@code contents} to be written
     * @param client the {@link AzureDataLakeStorageClient}
     * @param opts options to change the behavior of the call
     * @param resp response from the call, and any error info generated by the call
     */
    public static void create(String path,
                              boolean overwrite,
                              byte[] contents,
                              int offsetWithinContentsArray,
                              int length,
                              AzureDataLakeStorageClient client,
                              RequestOptions opts,
                              OperationResponse resp) {
        QueryParams qp = new QueryParams();
        qp.add("overwrite", (overwrite? "true" : "false"));
        qp.add("write", "true");  // This is to suppress the 307-redirect from server (standard WebHdfs behavior)

        HttpTransport.makeCall(client, Operation.CREATE, path, qp, contents, offsetWithinContentsArray, length, opts, resp);
    }

    /**
     * append bytes to an existing file created with {@link #create(String, boolean, byte[], int, int, AzureDataLakeStorageClient, RequestOptions, OperationResponse) create}.
     *
     *
     * @param path the full path of the file to append to. The file must already exist.
     * @param contents byte array containing the contents to be written to the file. Can be {@code null}
     * @param offsetWithinContentsArray offset within the byte array passed in {@code contents}. Bytes starting
     *                                  at this offset will be written to server
     * @param length number of bytes from {@code contents} to be written
     * @param client the {@link AzureDataLakeStorageClient}
     * @param opts options to change the behavior of the call
     * @param resp response from the call, and any error info generated by the call
     */
    public static void append(String path,
                              byte[] contents,
                              int offsetWithinContentsArray,
                              int length,
                              AzureDataLakeStorageClient client,
                              RequestOptions opts,
                              OperationResponse resp) {
        QueryParams qp = new QueryParams();
        qp.add("append", "true");

        HttpTransport.makeCall(client, Operation.APPEND, path, qp, contents, offsetWithinContentsArray, length, opts, resp);
    }

    public static void concurrentAppend(String path,
                                        byte[] contents,
                                        int offsetWithinContentsArray,
                                        int length,
                                        boolean autoCreate,
                                        AzureDataLakeStorageClient client,
                                        RequestOptions opts,
                                        OperationResponse resp) {
        QueryParams qp = new QueryParams();
        if (autoCreate) qp.add("appendMode", "autocreate");

        HttpTransport.makeCall(client, Operation.CONCURRENTAPPEND, path, qp, contents, offsetWithinContentsArray, length, opts, resp);
        try {
            resp.responseStream.close();
        } catch (IOException ex) {
            // Dont care about response from server.
        }
    }

    /**
     * read from a file. This is the stateless read method, that reads bytes from an offset in a file.
     *
     *
     * @param path the full path of the file to read. The file must already exist.
     * @param offset the offset within the ADL file to read from
     * @param length the number of bytes to read from file
     * @param client the {@link AzureDataLakeStorageClient}
     * @param opts options to change the behavior of the call
     * @param resp response from the call, and any error info generated by the call
     * @return returns an {@link com.microsoft.azure.datalake.store.ADLFileInputStream}
     */
    public static InputStream open(String path,
                                   long offset,
                                   long length,
                                   AzureDataLakeStorageClient client,
                                   RequestOptions opts,
                                   OperationResponse resp) {
        QueryParams qp = new QueryParams();
        qp.add("read", "true");
        if (offset > 0) qp.add("offset", Long.toString(offset));
        if (length > 0) qp.add("length", Long.toString(length));

        HttpTransport.makeCall(client, Operation.OPEN, path, qp, null, 0, 0, opts, resp);

        if (resp.successful) {
            return resp.responseStream;
        } else {
            return null;
        }
    }

    /**
     * delete a file from Azure Data Lake.
     *
     * @param path the full path of the file to delete. The file must already exist.
     * @param recursive if deleting a directory, then whether to delete all files an directories
     *                  in the directory hierarchy underneath
     * @param client the {@link AzureDataLakeStorageClient}
     * @param opts options to change the behavior of the call
     * @param resp response from the call, and any error info generated by the call
     * @return returns {@code true} if the delete was successful. Also check {@code resp.successful}.
     */
    public static boolean delete(String path,
                                 boolean recursive,
                                 AzureDataLakeStorageClient client,
                                 RequestOptions opts,
                                 OperationResponse resp) {
        QueryParams qp = new QueryParams();
        qp.add("recursive", (recursive? "true" : "false"));

        HttpTransport.makeCall(client, Operation.DELETE, path, qp, null, 0, 0, opts, resp);
        if (!resp.successful) return false;

        boolean returnValue = true;
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(resp.responseStream);

            returnValue = rootNode.path("boolean").asBoolean();

        } catch (IOException ex) {
            resp.successful = false;
            resp.message = "Unexpected error happened reading response stream or parsing JSon from delete()";
        }

        return returnValue;
    }

    /**
     * rename a file.
     *
     * @param path the full path of the existing file to rename. (the old name)
     * @param destination the new name of the file. (the new name)
     * @param client the {@link AzureDataLakeStorageClient}
     * @param opts options to change the behavior of the call
     * @param resp response from the call, and any error info generated by the call
     * @return returns {@code true} if the rename was successful. Also check {@code resp.successful}.
     */
    public static boolean rename(String path,
                                 String destination,
                                 AzureDataLakeStorageClient client,
                                 RequestOptions opts,
                                 OperationResponse resp) {
        QueryParams qp = new QueryParams();
        qp.add("destination", destination);

        HttpTransport.makeCall(client, Operation.RENAME, path, qp, null, 0, 0, opts, resp);
        if (!resp.successful) return false;

        boolean returnValue = true;
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(resp.responseStream);
            returnValue = rootNode.path("boolean").asBoolean();
        } catch (IOException ex) {
            resp.successful = false;
            resp.message = "Unexpected error happened reading response stream or parsing JSon from rename()";
        }
        return returnValue;
    }

    public static boolean mkdirs(String path,
                                 AzureDataLakeStorageClient client,
                                 RequestOptions opts,
                                 OperationResponse resp) {
        HttpTransport.makeCall(client, Operation.MKDIRS, path, null, null, 0, 0, opts, resp);
        if (!resp.successful) return false;

        boolean returnValue = true;
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(resp.responseStream);

            returnValue = rootNode.path("boolean").asBoolean();

        } catch (IOException ex) {
            resp.successful = false;
            resp.message = "Unexpected error happened reading response stream or parsing JSon from mkdirs()";
        }
        return returnValue;
    }

    public static void concat(String path,
                              List<String> sources,
                              boolean deleteSourceDirectory,
                              AzureDataLakeStorageClient client,
                              RequestOptions opts,
                              OperationResponse resp) {
        if (sources == null || sources.size() == 0 ) {
            resp.successful = false;
            resp.message = "No source files specified to concatenate";
            return;
        }
        byte[] body = null;
        StringBuilder sb = new StringBuilder("sources=");
        boolean firstelem = true;
        for (String item : sources) {
            if (item.equals(path)) {
                resp.successful = false;
                resp.message = "One of the source files to concatenate is the destination file";
                return;
            }
            if (!firstelem) sb.append(',');
                       else firstelem = false;
            sb.append(item);
        }
        try {
            body = sb.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            //This shouldnt happen.
            assert false : "UTF-8 encoding is not supported";
        }

        QueryParams qp = new QueryParams();
        qp.add("deleteSourceDirectory", (deleteSourceDirectory? "true" : "false"));

        HttpTransport.makeCall(client, Operation.MSCONCAT, path, qp, body, 0, body.length, opts, resp);
    }


    public static DirectoryEntry getFileStatus(String path,
                                               AzureDataLakeStorageClient client,
                                               RequestOptions opts,
                                               OperationResponse resp) {
        HttpTransport.makeCall(client, Operation.MSGETFILESTATUS, path, null, null, 0, 0, opts, resp);

        if (resp.successful) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(resp.responseStream);

                JsonNode fsNode = rootNode.path("FileStatus");

                String name = fsNode.path("pathSuffix").asText();
                String fullName;
                if (!name.equals("")) {
                    if (path.endsWith("/")) {
                        fullName = path + name;
                    } else {
                        fullName = path + "/" + name;
                    }
                } else {
                    fullName = path;
                    name = path.substring(path.lastIndexOf("/")+1);
                }

                long length = fsNode.path("length").asLong(0);
                String user = fsNode.path("owner").asText();
                String group = fsNode.path("group").asText();
                Date lastAccessTime = new Date(fsNode.path("accessTime").asLong());
                Date lastModifiedTime = new Date(fsNode.path("modificationTime").asLong());
                DirectoryEntryType type = fsNode.path("type").asText().equals("FILE") ?
                                                              DirectoryEntryType.FILE :
                                                              DirectoryEntryType.DIRECTORY;
                String permission = fsNode.path("permission").asText();

                return new DirectoryEntry(name,
                                          fullName,
                                          length,
                                          group,
                                          user,
                                          lastAccessTime,
                                          lastModifiedTime,
                                          type,
                                          permission);
            } catch (IOException ex) {
                resp.successful = false;
                resp.message = "Unexpected error happened reading response stream or parsing JSon from getFileStatus()";
            }
        }
        return null;
    }

    public static List<DirectoryEntry> listStatus(String path,
                                                  String listAfter,
                                                  String listBefore,
                                                  int listSize,
                                                  AzureDataLakeStorageClient client,
                                                  RequestOptions opts,
                                                  OperationResponse resp) {
        QueryParams qp = new QueryParams();

        if (listAfter!=null && !listAfter.equals("")) {
            qp.add("listAfter", listAfter);
        }
        if (listBefore!=null && !listBefore.equals("")) {
            qp.add("listBefore", listBefore);
        }
        if (listSize > 0) {
            qp.add("listSize", Integer.toString(listSize));
        }

        HttpTransport.makeCall(client, Operation.MSLISTSTATUS, path, qp, null, 0, 0, opts, resp);

        if (resp.successful) {
            ArrayList<DirectoryEntry> list = new ArrayList<DirectoryEntry>();
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(resp.responseStream);

                JsonNode array = rootNode.path("FileStatuses").path("FileStatus");
                for (JsonNode fsNode : array) {
                    String name = fsNode.path("pathSuffix").asText();
                    String fullName;
                    if (!name.equals("")) {
                        if (path.endsWith("/")) {
                            fullName = path + name;
                        } else {
                            fullName = path + "/" + name;
                        }
                    } else {
                        fullName = path;
                        name = path.substring(path.lastIndexOf("/")+1);
                    }

                    long length = fsNode.path("length").asLong(0);
                    String user = fsNode.path("owner").asText();
                    String group = fsNode.path("group").asText();
                    Date lastAccessTime = new Date(fsNode.path("accessTime").asLong());
                    Date lastModifiedTime = new Date(fsNode.path("modificationTime").asLong());
                    DirectoryEntryType type = fsNode.path("type").asText().equals("FILE") ?
                            DirectoryEntryType.FILE :
                            DirectoryEntryType.DIRECTORY;
                    String permission = fsNode.path("permission").asText();

                    DirectoryEntry entry = new DirectoryEntry(name,
                            fullName,
                            length,
                            group,
                            user,
                            lastAccessTime,
                            lastModifiedTime,
                            type,
                            permission);
                    list.add(entry);
                }
                return list;
            } catch (IOException ex) {
                resp.successful = false;
                resp.message = "Unexpected error happened reading response stream or parsing JSon from listFiles()";
            }
        }
        return null;
    }

    public static void setTimes(String path,
                                long atime,
                                long mtime,
                                AzureDataLakeStorageClient client,
                                RequestOptions opts,
                                OperationResponse resp) {
        if (atime < -1) {
            resp.message = "Invalid Access Time specified";
            resp.successful = false;
            return;
        }

        if (mtime < -1) {
            resp.message = "Invalid Modification Time specified";
            resp.successful = false;
            return;
        }

        if (atime == -1 && mtime == -1) {
            resp.message = "Access time and Modification time cannot both be unspecified";
        }

        QueryParams qp = new QueryParams();
        if (mtime != -1 ) qp.add("modificationtime", Long.toString(mtime));
        if (atime != -1 ) qp.add("accesstime",       Long.toString(atime));

        HttpTransport.makeCall(client, Operation.SETTIMES, path, qp, null, 0, 0, opts, resp);
    }

    /**
     * sets the owning user and group of the file. If the user or group are {@code null}, then they are not changed.
     * It is illegal to pass both user and owner as {@code null}.
     *
     * @param path the full path of the file
     * @param user the ID of the user, or {@code null}
     * @param group the ID of the group, or {@code null}
     * @param client the {@link AzureDataLakeStorageClient}
     * @param opts options to change the behavior of the call
     * @param resp response from the call, and any error info generated by the call
     */
    public static void setOwner(String path,
                                String user,
                                String group,
                                AzureDataLakeStorageClient client,
                                RequestOptions opts,
                                OperationResponse resp) {
        // at least one of owner or user must be set
        if (       (user == null  || user.equals(""))
                && (group == null || group.equals(""))
                ) {
            resp.successful = false;
            resp.message = "Both user and owner names cannot be blank";
            return;
        }

        QueryParams qp = new QueryParams();
        if (user!=null && !user.equals("")) {
            qp.add("owner", user);
        }
        if (group!=null && !group.equals("")) {
            qp.add("group", group);
        }

        HttpTransport.makeCall(client, Operation.SETOWNER, path, qp, null, 0, 0, opts, resp);
    }

    public static void setPermission(String path,
                                     String octalPermissions,
                                     AzureDataLakeStorageClient client,
                                     RequestOptions opts,
                                     OperationResponse resp) {
        if (!isValidOctal(octalPermissions)) {
            resp.message = "Specified permissions are not valid Octal Permissions: " + octalPermissions;
            resp.successful = false;
            return;
        }

        QueryParams qp = new QueryParams();
        qp.add("permission", octalPermissions);

        HttpTransport.makeCall(client, Operation.SETPERMISSION, path, qp, null, 0, 0, opts, resp);
    }

    private static final Pattern octalPattern = Pattern.compile("[0-7][0-7][0-7]");
    private static boolean isValidOctal(String input) {
        return octalPattern.matcher(input).matches();
    }

    public static void checkAccess(String path,
                                   String rwx,
                                   AzureDataLakeStorageClient client,
                                   RequestOptions opts,
                                   OperationResponse resp) {
        if (rwx == null || rwx.trim().equals("")) {
            resp.message = "null or empty access specification passed in to check access for";
            resp.successful = false;
            return;
        }

        if (!isValidRwx(rwx)) {
            resp.message = "invalid access specification passed in to check access for: " + rwx;
            resp.successful = false;
            return;
        }

        QueryParams qp = new QueryParams();
        qp.add("fsaction", rwx);

        HttpTransport.makeCall(client, Operation.CHECKACCESS, path, qp, null, 0, 0, opts, resp);
    }

    private static final Pattern rwxPattern = Pattern.compile("[r-][w-][x-]");
    private static boolean isValidRwx(String input) {
        input = input.trim().toLowerCase();
        return rwxPattern.matcher(input).matches();
    }

    public static void modifyAclEntries(String path,
                                        String aclSpec,
                                        AzureDataLakeStorageClient client,
                                        RequestOptions opts,
                                        OperationResponse resp) {
        if (aclSpec == null || aclSpec.trim().equals("")) {
            resp.message = "null or empty AclSpec passed in to modifyAclEntries";
            resp.successful = false;
            return;
        }

        QueryParams qp = new QueryParams();
        qp.add("aclspec", aclSpec);

        HttpTransport.makeCall(client, Operation.MODIFYACLENTRIES, path, qp, null, 0, 0, opts, resp);
    }

    public static void modifyAclEntries(String path,
                                        List<AclEntry> aclSpec,
                                        AzureDataLakeStorageClient client,
                                        RequestOptions opts,
                                        OperationResponse resp) {
        if (aclSpec == null || aclSpec.size() == 0) {
            resp.message = "null or empty AclSpec passed in to modifyAclEntries";
            resp.successful = false;
            return;
        }

        QueryParams qp = new QueryParams();
        qp.add("aclspec", AclEntry.aclListToString(aclSpec));

        HttpTransport.makeCall(client, Operation.MODIFYACLENTRIES, path, qp, null, 0, 0, opts, resp);
    }

    public static void removeAclEntries(String path,
                                        String aclSpec,
                                        AzureDataLakeStorageClient client,
                                        RequestOptions opts,
                                        OperationResponse resp) {
        if (aclSpec == null || aclSpec.trim().equals("")) {
            resp.message = "null or empty AclSpec passed in to removeAclEntries";
            resp.successful = false;
            return;
        }

        QueryParams qp = new QueryParams();
        qp.add("aclspec", aclSpec);

        HttpTransport.makeCall(client, Operation.REMOVEACLENTRIES, path, qp, null, 0, 0, opts, resp);
    }


    public static void removeAclEntries(String path,
                                        List<AclEntry> aclSpec,
                                        AzureDataLakeStorageClient client,
                                        RequestOptions opts,
                                        OperationResponse resp) {
        if (aclSpec == null || aclSpec.size() == 0) {
            resp.message = "null or empty AclSpec passed in to removeAclEntries";
            resp.successful = false;
            return;
        }

        QueryParams qp = new QueryParams();
        qp.add("aclspec", AclEntry.aclListToString(aclSpec, true));

        HttpTransport.makeCall(client, Operation.REMOVEACLENTRIES, path, qp, null, 0, 0, opts, resp);
    }

    public static void removeDefaultAcl(String path,
                                        AzureDataLakeStorageClient client,
                                        RequestOptions opts,
                                        OperationResponse resp) {

        HttpTransport.makeCall(client, Operation.REMOVEDEFAULTACL, path, null, null, 0, 0, opts, resp);
    }

    public static void removeAcl(String path,
                                 AzureDataLakeStorageClient client,
                                 RequestOptions opts,
                                 OperationResponse resp) {

        HttpTransport.makeCall(client, Operation.REMOVEACL, path, null, null, 0, 0, opts, resp);
    }

    public static void setAcl(String path,
                              String aclSpec,
                              AzureDataLakeStorageClient client,
                              RequestOptions opts,
                              OperationResponse resp) {
        if (aclSpec == null || aclSpec.trim().equals("")) {
            resp.message = "null or empty AclSpec passed in to setAcl";
            resp.successful = false;
            return;
        }

        QueryParams qp = new QueryParams();
        qp.add("aclspec", aclSpec);

        HttpTransport.makeCall(client, Operation.SETACL, path, qp, null, 0, 0, opts, resp);
    }

    public static void setAcl(String path,
                              List<AclEntry> aclSpec,
                              AzureDataLakeStorageClient client,
                              RequestOptions opts,
                              OperationResponse resp) {
        if (aclSpec == null || aclSpec.size() == 0) {
            resp.message = "null or empty AclSpec passed in to setAcl";
            resp.successful = false;
            return;
        }

        QueryParams qp = new QueryParams();
        qp.add("aclspec", AclEntry.aclListToString(aclSpec));

        HttpTransport.makeCall(client, Operation.SETACL, path, qp, null, 0, 0, opts, resp);
    }

    public static AclStatus getAclStatus(String path,
                                         AzureDataLakeStorageClient client,
                                         RequestOptions opts,
                                         OperationResponse resp) {

        HttpTransport.makeCall(client, Operation.SETACL, path, null, null, 0, 0, opts, resp);

        if (resp.successful) {
            AclStatus status = new AclStatus();
            ArrayList<AclEntry> list = new ArrayList<AclEntry>();
            status.aclSpec = list;
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(resp.responseStream);
                JsonNode aclStatusNode = rootNode.path("AclStatus");
                JsonNode array = aclStatusNode.path("entries");
                for (JsonNode aclEntryNode : array) {
                    String aclEntryString = aclEntryNode.asText();
                    AclEntry aclEntry = AclEntry.parseAclEntry(aclEntryString);
                    list.add(aclEntry);
                }
                status.group = aclStatusNode.path("group").asText();
                status.owner = aclStatusNode.path("owner").asText();
                status.octalPermissions = aclStatusNode.path("permission").asText();
                return status;
            } catch (IOException ex) {
                resp.successful = false;
                resp.message = "Unexpected error happened reading response stream or parsing JSon from getAclStatus";
                return null;
            }
        } else {
            return null;
        }
    }



    /**
     * creates an {@link ADLException} from {@link OperationResponse}.
     *
     * @param resp the {@link OperationResponse} to convert to exception
     * @param defaultMessage message to use if the inner exception does not have a text message.
     * @return the {@link ADLException}, or {@code null} if the {@code resp.successful} is {@code true}
     */
    public static ADLException getExceptionFromResp(OperationResponse resp, String defaultMessage) {
        String msg = (resp.message == null) ? defaultMessage : resp.message;
        ADLException ex = new ADLException(msg, resp.ex);
        copyResponseToADLEXception(resp, ex);
        return ex;
    }

    /**
     * copies values from {@link OperationResponse} to {@link ADLException}.
     * @param resp the {@link OperationResponse} to copy from
     * @param ex the {@link ADLException} to copy to
     *
     * Throws NullPointerException if either of the input parameters are null
     */
    private static void copyResponseToADLEXception(OperationResponse resp, ADLException ex) {
        if (resp == null || ex == null) throw new NullPointerException("input parameters cannot be null");

        ex.httpResponseCode = resp.httpResponseCode;
        ex.httpResponseMessage = resp.httpResponseMessage;
        ex.requestId = resp.requestId;

        ex.numRetries = resp.numRetries;
        ex.lastCallLatency = resp.lastCallLatency;
        ex.responseContentLength = resp.responseContentLength;

        ex.remoteExceptionName = resp.remoteExceptionName;
        ex.remoteExceptionMessage = resp.remoteExceptionMessage;
        ex.remoteExceptionJavaClassName = resp.remoteExceptionJavaClassName;
    }
}
