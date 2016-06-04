package com.microsoft.azure.datalake.store.protocol;


import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.AzureDataLakeStorageClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Core {

    public static ADLException getExceptionFromResp(OperationResponse resp, String defaultMessage) {
        if (resp.successful) return null;
        String msg = (resp.message == null) ? defaultMessage : resp.message;
        ADLException ex = new ADLException(msg, resp.ex);
        ex.httpResponseCode = resp.httpResponseCode;
        ex.httpResponseMessage = resp.httpResponseMessage;
        ex.requestId = resp.requestId;

        ex.numRetries = resp.numRetries;
        ex.lastCallLatency = resp.lastCallLatency;
        ex.responseContentLength = resp.responseContentLength;

        ex.remoteExceptionName = resp.remoteExceptionName;
        ex.remoteExceptionMessage = resp.remoteExceptionMessage;
        ex.remoteExceptionJavaClassName = resp.remoteExceptionJavaClassName;
        return ex;
    }


    public static void create(String path,
                              boolean overwrite,
                              byte[] contents,
                              int offsetWithinContentsArray,
                              int length,
                              AzureDataLakeStorageClient client,
                              RequestOptions opts,
                              OperationResponse resp
    ) {
        QueryParams qp = new QueryParams();
        qp.add("overwrite", (overwrite? "true" : "false"));
        qp.add("write", "true");  // This is to suppress the 307-redirect from server (standard WebHdfs behavior)

        HttpTransport.makeCall(client, Operation.CREATE, path, qp, contents, offsetWithinContentsArray, length, opts, resp);
    }

    public static void append(String path,
                              byte[] contents,
                              int offsetWithinContentsArray,
                              int length,
                              AzureDataLakeStorageClient client,
                              RequestOptions opts,
                              OperationResponse resp
    ) {
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
                                        OperationResponse resp
    ) {
        QueryParams qp = new QueryParams();
        if (autoCreate) qp.add("appendMode", "autocreate");

        HttpTransport.makeCall(client, Operation.CONCURRENTAPPEND, path, qp, contents, offsetWithinContentsArray, length, opts, resp);
        try {
            resp.responseStream.close();
        } catch (IOException ex) { }
    }

    public static InputStream open(String path,
                                   long offset,
                                   long length,
                                   AzureDataLakeStorageClient client,
                                   RequestOptions opts,
                                   OperationResponse resp
                     ) {
        QueryParams qp = new QueryParams();

        if (offset > 0) qp.add("offset", Long.toString(offset));
        if (length > 0) qp.add("length", Long.toString(length));

        HttpTransport.makeCall(client, Operation.OPEN, path, qp, null, 0, 0, opts, resp);

        if (resp.successful) {
            return resp.responseStream;
        } else {
            return null;
        }
    }

    public static boolean delete(String path,
                       boolean recursive,
                       AzureDataLakeStorageClient client,
                       RequestOptions opts,
                       OperationResponse resp
                       ) {
        QueryParams qp = new QueryParams();
        qp.add("recursive", (recursive? "true" : "false"));

        HttpTransport.makeCall(client, Operation.DELETE, path, qp, null, 0, 0, opts, resp);

        boolean returnValue = true;
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(resp.responseStream);

            returnValue = rootNode.path("boolean").asBoolean();

        } catch (IOException ex) {}

        try {
            // TODO: This should not be needed: need to confrm if readTree closes the stream or not
            resp.responseStream.close();
        } catch (IOException ex) {}

        return returnValue;
    }

    public static boolean rename(String path,
                          String destination,
                          AzureDataLakeStorageClient client,
                          RequestOptions opts,
                          OperationResponse resp
    ) {
        QueryParams qp = new QueryParams();
        qp.add("destination", destination);

        HttpTransport.makeCall(client, Operation.RENAME, path, qp, null, 0, 0, opts, resp);

        boolean returnValue = true;
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(resp.responseStream);

            returnValue = rootNode.path("boolean").asBoolean();

        } catch (IOException ex) {}

        try {
            // TODO: This should not be needed: need to confrm if readTree closes the stream or not
            resp.responseStream.close();
        } catch (IOException ex) {}
        return returnValue;
    }


    public static void setOwner(String path,
                            String user,
                            String group,
                            AzureDataLakeStorageClient client,
                            RequestOptions opts,
                            OperationResponse resp
    ) {
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

    public static boolean mkdirs(String path,
                       AzureDataLakeStorageClient client,
                       RequestOptions opts,
                       OperationResponse resp
    ) {
        HttpTransport.makeCall(client, Operation.MKDIRS, path, null, null, 0, 0, opts, resp);

        boolean returnValue = true;
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(resp.responseStream);

            returnValue = rootNode.path("boolean").asBoolean();

        } catch (IOException ex) {}

        try {
            // TODO: This should not be needed: need to confrm if readTree closes the stream or not
            resp.responseStream.close();
        } catch (IOException ex) {}
        return returnValue;
    }

    public static void concat(String path,
                       List<String> sources,
                       boolean deleteSourceDirectory,
                       AzureDataLakeStorageClient client,
                       RequestOptions opts,
                       OperationResponse resp
    ) {
        if (sources == null || sources.size() == 0 ) {
            //TODO: Fill resp. Is the expecation to succeed or to return error?
            // currently just letting through and letting server decide
            // return;
        }
        byte[] body = null;
        StringBuilder sb = new StringBuilder("sources=");
        boolean firstelem = true;
        for (String item : sources) {
            if (!firstelem) sb.append(',');
                       else firstelem = false;
            sb.append(item);
        }
        try {
            body = sb.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            //This shouldnt happen
            //TODO: Log this
        }

        QueryParams qp = new QueryParams();
        qp.add("deleteSourceDirectory", (deleteSourceDirectory? "true" : "false"));

        HttpTransport.makeCall(client, Operation.MSCONCAT, path, qp, body, 0, body.length, opts, resp);
    }

    public static DirectoryEntry getFileStatus(String path,
                                        AzureDataLakeStorageClient client,
                                        RequestOptions opts,
                                        OperationResponse resp
    ) {
        HttpTransport.makeCall(client, Operation.GETFILESTATUS, path, null, null, 0, 0, opts, resp);

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
                                     OperationResponse resp
    ) {
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
            }
        }
        return null;
    }
}
