package com.contoso;

import com.microsoft.azure.datalake.store.*;
import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

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

        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        opts.timeout = 5000;
        OperationResponse resp = new OperationResponse();

        Core.mkdirs("/a/b/c", adlClient,  opts, resp);
        if (!resp.successful) {
            System.out.format("Error creating directory: %s %s%n", resp.httpResponseCode, resp.httpResponseMessage);
            System.out.format("               Exception: %s%n",    resp.ex);
        }
        System.out.println("Done with mkdirs");

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
        resp = new OperationResponse();
        Core.create("/a/b/c/p.txt", true, buf, 0, buf.length, adlClient, opts, resp);
        if (!resp.successful) {
            System.out.format("Error creating file: %s %s%n", resp.httpResponseCode, resp.httpResponseMessage);
            System.out.format("               Exception: %s%n",    resp.ex);
        }
        System.out.println("Done with create");






        /*

        //createClient Directory - uses a convenience static method on the client
        adlClient.utils.createDirectory("/a/b/c");

        // createClient File and write to it - uses a File object
        // Most Operation are defined on File and Directory objects
        boolean overwrite = true;
        ADLFileInfo file = adlClient.getFileInfo("a/b/c/d.txt");  // this is just a local object - nothing on server yet
        ADLFileOutputStream stream = file.createFromStream(overwrite);
        // Now you can layer any of java writers and streams over this one,
        // like printstream, text writer, GZIPOutputStream, etc. We will use PrintStream here.
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
        */
    }
}
