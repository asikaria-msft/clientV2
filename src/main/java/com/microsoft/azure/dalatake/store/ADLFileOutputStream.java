package com.microsoft.azure.dalatake.store;

import java.io.IOException;
import java.io.OutputStream;

public class ADLFileOutputStream extends OutputStream {

    // no constructor - use Factory Method in AzureDataLakeStoreClient
    private ADLFileOutputStream() {
        super();
    }

    @Override
    public void write(int b) throws IOException {

    }

    @Override
    public void write(byte[] b) throws IOException {
        super.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        super.flush();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
