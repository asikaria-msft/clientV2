package com.contoso;

import com.microsoft.azure.datalake.store.*;
import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MyApp {
    public static void main(String [ ] args) {

        // Do Auth
        String clientID = "f536fd51-a65c-4df4-b50f-2656bdd37b84";
        String clientCreds = "LA4hobTrc4ZswTlK0SwMPufj3NyvProo+IgIYZzCV6w=";
        String tokenEndpoint = "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token";
        AzureADToken token;
        try {
            token = AzureADAuthenticator.getTokenUsingClientCreds(tokenEndpoint, clientID, clientCreds);
        } catch (IOException ex) {
            System.out.format("Error acquring AAD token: %s", ex.getMessage());
            return;
        }

        // createClient Client
        String adlsAccount = "asikaria2store.azuredatalakestore.net";
        AzureDataLakeStorageClient adlClient = AzureDataLakeStorageClient.createClient(adlsAccount, token);

        runSdkMethods(adlClient);
    }

    private static byte[] getSampleContent() {
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(s);
        out.println("This is a line");
        out.println("This is another line");
        out.println("This is yet another line");
        out.println("This is yet yet another line");
        out.println("This is yet yet yet another line");
        out.println("... and so on, ad infinitum");
        out.println();
        out.close();
        byte[] buf = s.toByteArray();
        return buf;
    }

    private static void runSdkMethods(AzureDataLakeStorageClient client) {

        // create directory
        ADLDirectoryInfo directory = client.getDirectoryInfo("/a");
        try {
            directory.delete(true);
        } catch (ADLException ex) {
            System.out.println(ex.getMessage());
        }


        // create directory
        ADLDirectoryInfo directory2 = client.getDirectoryInfo("/a/b/c");
        try {
            directory2.create();
        } catch (ADLException ex) {
            System.out.println(ex.getMessage());
        }

        //create file
        ADLFileInfo file = client.getFileInfo("/a/b/c/d.txt");  // this is just a local object - nothing on server yet
        boolean overwrite = true;
        ADLFileOutputStream stream = file.createFromStream(overwrite);
        PrintStream out = new PrintStream(stream);
        try {
            for (int i = 1; i <= 10; i++) {
                out.println("This is line #" + i);
                out.format("This is the same line (%d), but using formatted output. %n", i);
            }
            out.close();  // guarantees flush to server, if it has not happened yet already in a prior write call
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

        // create file
        ADLFileInfo file2 = client.getFileInfo("/a/b/c/e.txt");
        stream = file2.createFromStream(overwrite);
        try {
            stream.write(getSampleContent());
            stream.close();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        // append to file
        ADLFileInfo file3 = client.getFileInfo("/a/b/c/e.txt");
        stream = file3.getAppendStream();
        try {
            stream.write(getSampleContent());
            stream.close();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        // concatenate, rename, GetFileStatus
        ADLFileInfo file4 = client.getFileInfo("/a/b/c/f.txt");
        try {
            List<String> list = Arrays.asList("/a/b/c/d.txt", "/a/b/c/e.txt");
            file4.concatenateFiles(list, false);
            file4.rename("a/b/c/g.txt");
        }catch (ADLException ex) {
            System.out.println(ex.getMessage());
        }

        InputStream in = file4.getReadStream();
        byte[] b = new byte[64000];
        int count = 0;
        try {
            while (in.read(b) != -1) {
                System.out.write(b);
                System.out.println(count++);
                System.out.flush();
            }
            in.close();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        // get file status
        try {
            DirectoryEntry ent = file4.getDirectoryEntry();
            printDirectoryInfo(ent);
        } catch (ADLException ex) {
            System.out.println(ex.getMessage());
        }

        try {
            List<DirectoryEntry> list = directory2.enumerate(2000);
            System.out.println("Directory listing for directory :");
            for (DirectoryEntry entry : list) {
                printDirectoryInfo(entry);
            }
        } catch (ADLException ex) {
            System.out.println(ex.getMessage());
        }
    }


    private static void printDirectoryInfo(DirectoryEntry ent) {
        System.out.format("Name: %s%n", ent.name);
        System.out.format("FullName: %s%n", ent.fullName);
        System.out.format("Length: %d%n", ent.length);
        System.out.format("Type: %s%n", ent.type.toString());
        System.out.format("Group: %s%n", ent.group);
        System.out.format("User: %s%n", ent.user);
        System.out.format("Permission: %s%n", ent.permission);
        System.out.format("mtime: %s%n", ent.lastModifiedTime.toString());
        System.out.format("atime: %s%n", ent.lastAccessTime.toString());
        System.out.println();
    }

    private static void runCoreMethods(AzureDataLakeStorageClient client) {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        opts.timeout = 5000;
        OperationResponse resp = new OperationResponse();

        Core.mkdirs("/a/b/c", client,  opts, resp);
        if (!resp.successful) {
            System.out.format("Error creating directory: %s %s%n", resp.httpResponseCode, resp.httpResponseMessage);
            System.out.format("               Exception: %s%n",    resp.ex);
        }
        System.out.println("Done with mkdirs");

        byte[] buf = getSampleContent();
        resp = new OperationResponse();
        Core.create("/a/b/c/p.txt", true, buf, 0, buf.length, client, opts, resp);
        if (!resp.successful) {
            System.out.format("Error creating file: %s %s%n", resp.httpResponseCode, resp.httpResponseMessage);
            System.out.format("               Exception: %s%n",    resp.ex);
        }
        System.out.println("Done with create");
    }



}
