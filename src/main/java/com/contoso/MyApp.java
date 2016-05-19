package com.contoso;

import com.microsoft.azure.dalatake.store.*;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;

public class MyApp {
    public static void main(String [ ] args) {

        // Do Auth
        String clientID = "033E5080-A101-4932-8818-8EC130F43847";
        String clientCreds = "AleWx7yyGjd9sbSDFS09mZ=";
        String tokenEndpoint = "https://login.microsoftonline.com/DC24AE9F-CE88-4999-88C5-830D5DA6A873/oauth2/token";
        AzureADToken token =  AzureADAuthenticator.GetTokenUsingClientCreds(tokenEndpoint, clientID, clientCreds);

        // Create Client
        String adlsAccount = "contoso.azuredatalakestore.net";
        AzureDatalakeStorageClient adlClient = AzureDatalakeStorageClient.Create(adlsAccount, token);

        //Create Directory - uses a convenience static method on the client
        adlClient.CreateDirectory("/a/b/c");

        // Create File and write to it - uses a File object
        // Most operations are defined on File and Directory objects
        boolean overwrite = true;
        ADLFile file = adlClient.GetFileReference("a/b/c/d.txt");  // this is just a local object - nothing on server yet
        ADLFileOutputStream stream = file.Create(overwrite);
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
    }
}
