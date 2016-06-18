/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.contoso.liveservicetests;

import com.microsoft.azure.datalake.store.*;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Pattern;


public class TestFileSdk {
    private final UUID instanceGuid = UUID.randomUUID();

    private static String directory = null;
    private static AzureDataLakeStorageClient client = null;
    private static boolean testsEnabled = true;

    @BeforeClass
    public static void setup() throws IOException {
        Properties prop;
        AzureADToken aadToken;

        prop = HelperUtils.getProperties();
        aadToken = AzureADAuthenticator.getTokenUsingClientCreds(prop.getProperty("OAuth2TokenUrl"),
                prop.getProperty("ClientId"),
                prop.getProperty("ClientSecret") );
        UUID guid = UUID.randomUUID();
        directory = "/" + prop.getProperty("dirName") + "/" + UUID.randomUUID();
        String account = prop.getProperty("StoreAcct") + ".azuredatalakestore.net";
        client = AzureDataLakeStorageClient.createClient(account, aadToken);
        testsEnabled = Boolean.parseBoolean(prop.getProperty("SdkTestsEnabled", "true"));
    }


    @Test
    public void createEmptyFile() throws IOException {
        Assume.assumeTrue(false);   // TODO: There is a bug in reading empty files
        String filename = directory + "/" + "Sdk.createEmptyFile.txt";

        // write some text to file
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(true);
        out.close();

        // read text from file
        System.out.format("reading %s%n", filename);
        InputStream in = f.getReadStream();
        byte[] b1 = new byte[4096]; // to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify nothing was read
        assertTrue("file length should be zero", 0 == count);
    }


    @Test
    public void smallFileNoOverwrite() throws IOException {
        Assume.assumeTrue(true);
        String filename = directory + "/" + "Sdk.smallFileNoOverwrite.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(false);
        out.write(contents);
        out.close();

        // read text from file
        System.out.format("reading %s%n", filename);
        InputStream in = f.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test(expected = IOException.class)
    public void existingFileNoOverwrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.existingFileNoOverwrite.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(false);
        out.write(contents);
        out.close();

        // overwrite the text with new text - SHOULD FAIL since file already exists
        contents = HelperUtils.getSampleText2();
        out = f.createFromStream(false);
        out.write(contents);
        out.close();  //  <<-- FAIL here
    }

    @Test
    public void smallFileWithOverwrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.smallFileWithOverwrite.txt";

        // write some text to file
        ADLFileInfo f = client.getFileInfo(filename);
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = f.createFromStream(true);
        out.write(contents);
        out.close();

        // overwrite the text with new text
        contents = HelperUtils.getSampleText2();
        out = f.createFromStream(true);
        out.write(contents);
        out.close();

        // read text from file
        System.out.format("reading %s%n", filename);
        InputStream in = f.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to the second text
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void nonExistingFileWithOverwrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.nonExistingFileWithOverwrite.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(true);
        out.write(contents);
        out.close();

        // read text from file
        System.out.format("reading %s%n", filename);
        InputStream in = f.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void large11MBWrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.large11MBWrite.txt";

        // write some text to file
        byte [] contents = HelperUtils.getRandomBuffer(11 * 1024 * 1024);
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(true);
        out.write(contents);
        out.close();

        // read from file
        System.out.format("reading %s%n", filename);
        InputStream in = f.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void multiple4Mbwrites() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.multiple4Mbwrites.txt";

        // do three 4mb writes
        ADLFileInfo f = client.getFileInfo(filename);
        byte [] contents = HelperUtils.getRandom4mbBuffer();
        OutputStream out = f.createFromStream(true);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(contents.length*3);
        for (int i = 0; i<3; i++) {
            out.write(contents);
            bos.write(contents);
        }
        out.close();
        bos.close();
        byte[] b1 = bos.toByteArray();

        // read file contents
        System.out.format("reading %s%n", filename);
        InputStream in = f.getReadStream();
        byte[] b2 = new byte[b1.length*2]; // double the size, to account for possible bloat due to bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b2, count, b2.length-count)) >=0 && count<=b2.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", b1.length == count);
        byte[] b3 = Arrays.copyOfRange(b2, 0, count);
        assertTrue("file contents should match", Arrays.equals(b1, b3));
    }

    @Test
    public void createFileAndDoManySmallWrites() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.CreateFileAndDoManySmallWrites.txt";

        // write a small text many times to file, creating a large file (multiple 4MB chunks + partial chunk)
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(true);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(13000*742);
        for (int i = 0; i<13000; i++) {  // 742 bytes * 13000 == 9.2MB upload total
            out.write(contents);
            bos.write(contents);
        }
        out.close();
        bos.close();
        byte[] b1 = bos.toByteArray();

        // read file contents
        System.out.format("reading %s%n", filename);
        InputStream in = f.getReadStream();
        byte[] b2 = new byte[13000*742*2]; // double the size, to account for possible bloat due to bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b2, count, b2.length-count)) >=0 && count<=b2.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", b1.length == count);
        byte[] b3 = Arrays.copyOfRange(b2, 0, count);
        assertTrue("file contents should match", Arrays.equals(b1, b3));
    }

    @Test(expected = ADLException.class)
    public void concatZeroFiles() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String fnc = directory + "/" + "Sdk.concatZeroFiles-c.txt";

        // concatenate single file
        ADLFileInfo fc = client.getFileInfo(fnc);
        List<String> flist = new ArrayList<String>(1);
        fc.concatenateFiles(flist, false);

        // read text from file
        System.out.format("reading %s%n", fnc);
        InputStream in = fc.getReadStream();
        byte[] b1 = new byte[4096]; // to account for bloat due to possible bug
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should be zero", 0 == count);
    }

    @Test
    public void concatSingleFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String fn1 = directory + "/" + "Sdk.concatSingleFile.txt";
        String fn2 = directory + "/" + "Sdk.concatSingleFile-c.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f1 = client.getFileInfo(fn1);
        OutputStream out = f1.createFromStream(false);
        out.write(contents);
        out.close();

        // concatenate single file
        ADLFileInfo f2 = client.getFileInfo(fn2);
        List<String> flist = new ArrayList<String>(1);
        flist.add(fn1);
        f2.concatenateFiles(flist, false);

        // read text from file
        System.out.format("reading %s%n", fn2);
        InputStream in = f2.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test(expected = ADLException.class)
    public void concatSingleFileOntoItself() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String fn1 = directory + "/" + "Sdk.concatSingleFileOntoItself.txt";
        String fn2 = directory + "/" + "Sdk.concatSingleFileOntoItself-c.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f1 = client.getFileInfo(fn1);
        OutputStream out = f1.createFromStream(false);
        out.write(contents);
        out.close();

        // concatenate single file
        ADLFileInfo f2 = client.getFileInfo(fn1);
        List<String> flist = new ArrayList<String>(1);
        flist.add(fn1);
        f2.concatenateFiles(flist, false);

        // read text from file
        System.out.format("reading %s%n", fn2);
        InputStream in = f1.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }


    @Test
    public void concatTwoFiles() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String fn1 = directory + "/" + "Sdk.concatTwoFiles-1.txt";
        String fn2 = directory + "/" + "Sdk.concatTwoFiles-2.txt";
        String fnc = directory + "/" + "Sdk.concatTwoFiles-c.txt";

        ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f1 = client.getFileInfo(fn1);
        OutputStream out = f1.createFromStream(false);
        out.write(contents);
        bos.write(contents);
        out.close();

        contents = HelperUtils.getSampleText2();
        ADLFileInfo f2 = client.getFileInfo(fn2);
        out = f2.createFromStream(false);
        out.write(contents);
        bos.write(contents);
        out.close();

        bos.close();
        contents = bos.toByteArray();

        // concatenate files
        ADLFileInfo fc = client.getFileInfo(fnc);
        List<String> flist = new ArrayList<String>(7);
        flist.add(fn1);
        flist.add(fn2);
        fc.concatenateFiles(flist, false);

        // read text from file
        System.out.format("reading %s%n", fnc);
        InputStream in = fc.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void concatThreeFiles() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String fn1 = directory + "/" + "Sdk.concatThreeFiles-1.txt";
        String fn2 = directory + "/" + "Sdk.concatThreeFiles-2.txt";
        String fn3 = directory + "/" + "Sdk.concatThreeFiles-3.txt";
        String fnc = directory + "/" + "Sdk.concatThreeFiles-c.txt";

        ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f1 = client.getFileInfo(fn1);
        OutputStream out = f1.createFromStream(false);
        out.write(contents);
        bos.write(contents);
        out.close();

        contents = HelperUtils.getSampleText2();
        ADLFileInfo f2 = client.getFileInfo(fn2);
        out = f2.createFromStream(false);
        out.write(contents);
        bos.write(contents);
        out.close();

        contents = HelperUtils.getRandomBuffer(1024);
        ADLFileInfo f3 = client.getFileInfo(fn3);
        out = f3.createFromStream(false);
        out.write(contents);
        bos.write(contents);
        out.close();

        bos.close();
        contents = bos.toByteArray();

        // concatenate files
        ADLFileInfo fc = client.getFileInfo(fnc);
        List<String> flist = new ArrayList<String>(7);
        flist.add(fn1);
        flist.add(fn2);
        flist.add(fn3);
        fc.concatenateFiles(flist, false);

        // read text from file
        System.out.format("reading %s%n", fnc);
        InputStream in = fc.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void renameFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.renameFile.txt";
        String fnr = directory + "/" + "Sdk.renameFile-r.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(false);
        out.write(contents);
        out.close();

        //rename file
        f.rename(fnr);

        // read text from file
        System.out.format("reading %s%n", filename);
        InputStream in = f.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void renameFileAndReadFromAnotherReference() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.renameFileAndReadFromAnotherReference.txt";
        String fnr = directory + "/" + "Sdk.renameFileAndReadFromAnotherReference-r.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(false);
        out.write(contents);
        out.close();

        //rename file
        f.rename(fnr);

        // open same file name as a different ADFIleInfo object and then read file
        System.out.format("reading %s%n", fnr);
        ADLFileInfo fr = client.getFileInfo(fnr);
        InputStream in = fr.getReadStream();
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
            System.out.format("  Read %d bytes,  cumulative %d%n", bytesRead, count);
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test(expected = ADLException.class)
    public void renameNonExistentFile() throws ADLException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.renameNonExistentFile.txt";
        String fnr = directory + "/" + "Sdk.renameNonExistentFile-r.txt";

        ADLFileInfo f = client.getFileInfo(filename);
        f.rename(fnr);  // <<-- Should FAIL
    }

    @Test(expected = ADLException.class)
    public void renameOntoSelf() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.renameOntoSelf.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(false);
        out.write(contents);
        out.close();

        f.rename(filename);  // <<-- Should FAIL
    }

    @Test(expected = ADLException.class)
    public void deleteNonExistentFile() throws ADLException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.deleteNonExistentFile.txt";

        ADLFileInfo f = client.getFileInfo(filename);
        f.delete();
    }

    @Test
    public void deleteFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.deleteFile.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(filename);
        OutputStream out = f.createFromStream(false);
        out.write(contents);
        out.close();

        f.delete();
    }

    @Test
    public void getDirectoryEntryforFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.getDirectoryEntryforFile.txt";

        ADLFileInfo f = client.getFileInfo(filename);
        DirectoryEntry d;


        try {
            d = f.getDirectoryEntry();
            assertTrue("getDirectoryEnrty on non-existent file should throw exception", false);
        } catch (ADLException ex) {
            assertTrue("Exception should be 404", ex.httpResponseCode == 404);
        }

        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = f.createFromStream(false);
        out.write(contents);
        try {
            d = f.getDirectoryEntry();
            assertTrue("unflushed file should still not exist", false);
        } catch (ADLException ex) {
            assertTrue("Unflushed file should get 404", ex.httpResponseCode == 404);
        }

        out.close();
        d = f.getDirectoryEntry();
        assertTrue("File fullname should match", d.fullName.equals(filename));
        assertTrue("File name should match", d.name.equals(filename.substring(filename.lastIndexOf('/')+1)));
        assertTrue("File should be of type FILE", d.type == DirectoryEntryType.FILE);
        assertTrue("File length should match", d.length == contents.length);
        assertTrue("user should not be missing", d.user!=null && !d.user.trim().equals(""));
        assertTrue("group should not be missing", d.group!=null && !d.group.trim().equals(""));

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR_OF_DAY, -1);
        Date dt = cal.getTime();
        assertTrue("mtime should be recent", d.lastAccessTime.after(dt));
        assertTrue("atime should be recent", d.lastModifiedTime.after(dt));

        Pattern rwxPattern = Pattern.compile("[0-7r-][0-7w-][0-7x-]");
        assertTrue("permission should match rwx or Octal pattern", rwxPattern.matcher(d.permission).matches());
    }

    @Test
    public void getDirectoryEntryforDirectory() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String dirname = directory + "/" + "getDirectoryEntryforDirectory/a/b/c";

        ADLDirectoryInfo d = client.getDirectoryInfo(dirname);
        DirectoryEntry de;

        d.create();

        de = d.getDirectoryEntry();
        assertTrue("Directory fullname should match", de.fullName.equals(dirname));
        assertTrue("Directory name should match", de.name.equals(dirname.substring(dirname.lastIndexOf('/')+1)));
        assertTrue("Directory should be of type DIRECTORY", de.type == DirectoryEntryType.DIRECTORY);
        assertTrue("Directory length should be zero", de.length == 0);
        assertTrue("user should not be missing", de.user!=null && !de.user.trim().equals(""));
        assertTrue("group should not be missing", de.group!=null && !de.group.trim().equals(""));

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR_OF_DAY, -1);
        Date dt = cal.getTime();
        assertTrue("mtime should be recent", de.lastAccessTime.after(dt));
        assertTrue("atime should be recent", de.lastModifiedTime.after(dt));

        Pattern rwxPattern = Pattern.compile("[0-7r-][0-7w-][0-7x-]");
        assertTrue("permission should match rwx or Octal pattern", rwxPattern.matcher(de.permission).matches());
    }

    @Test
    public void deleteDirectoryRecursive() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String dirname = directory + "/" + "deleteDirectoryRecursive";


        String fn1 = dirname + "/a/b/c/f1.txt";
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(fn1);
        OutputStream out = f.createFromStream(false);
        out.write(contents);
        out.close();

        String fn2 = dirname + "/a/b/f2.txt";
        contents = HelperUtils.getSampleText2();
        ADLFileInfo f2 = client.getFileInfo(fn2);
        out = f2.createFromStream(false);
        out.write(contents);
        out.close();

        String parentDir = dirname + "/a";
        ADLDirectoryInfo pdir = client.getDirectoryInfo(parentDir);
        pdir.delete(true);

        try {
            ADLDirectoryInfo d = client.getDirectoryInfo(parentDir);
            d.getDirectoryEntry();
            assertTrue("getDirectoryEntry should fail on a deleted directory", false);
        } catch (ADLException ex) {
            if (ex.httpResponseCode!=404) throw ex;
        }
    }

    @Test
    public void deleteDirectoryNonRecursive() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String dirname = directory + "/" + "deleteDirectoryNonRecursive";

        String fn1 = dirname + "/a/b/c/f1.txt";
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileInfo f = client.getFileInfo(fn1);
        OutputStream out = f.createFromStream(false);
        out.write(contents);
        out.close();

        String fn2 = dirname + "/a/b/f2.txt";
        contents = HelperUtils.getSampleText2();
        ADLFileInfo f2 = client.getFileInfo(fn2);
        out = f2.createFromStream(false);
        out.write(contents);
        out.close();

        String parentDir = dirname + "/a";
        ADLDirectoryInfo pdir = client.getDirectoryInfo(parentDir);

        try {
            pdir.delete(false);
            assertTrue("Non-recursive delete should fail on a non-empty directory tree", false);
        } catch (ADLException ex) {
        }
    }


}
