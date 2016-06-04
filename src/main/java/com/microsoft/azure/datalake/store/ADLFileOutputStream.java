package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;

import java.io.IOException;
import java.io.OutputStream;

public class ADLFileOutputStream extends OutputStream {

    private final String filename;
    private final AzureDataLakeStorageClient client;
    private final boolean isCreate;

    private final boolean overwrite;
    private final int blocksize = 4 * 1024 *1024;
    private final byte[] buffer = new byte[blocksize]; //4MB byte buffer
    private int cursor = 0;
    private boolean created;
    private boolean streamClosed = false;

    // package-private constructor - use Factory Method in AzureDataLakeStoreClient
    ADLFileOutputStream(String filename, AzureDataLakeStorageClient client, boolean isCreate, boolean overwrite ) {
        super();
        this.overwrite = overwrite;
        this.filename = filename;
        this.client = client;
        this.isCreate = isCreate;
        created = !isCreate;          // for appends, created is already supposed to be true
    }

    @Override
    public void write(int b) throws IOException {
        byte buf[] = new byte[1];
        buf[0] = (byte) b;
        write(buf, 0, 1);
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (b == null) return;
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {

        if (streamClosed) throw new IOException("attempting to write to a closed stream;");
        if (b == null) {
            throw new NullPointerException();
        }
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }

        if (off > b.length || len > (b.length - off)) throw new IllegalArgumentException("array offset and length are > array size");

        // if len > 4MB, then we force-break the write into 4MB chunks
        while (len > blocksize) {
            flush(); // flush first, because we want to preserve record boundary of last append
            addToBuffer(b, off, blocksize);
            off += blocksize;
            len -= blocksize;
        }
        // now len == the remaining length

        //if adding this to buffer would overflow buffer, then flush buffer first
        if (len > buffer.length - cursor) {
            flush();
        }
        // now we know b will fit in remaining buffer, so just add it in
        addToBuffer(b, off, len);

        // if buffer is full, then just flush it right away rather than waiting for next write request
        if (cursor >= blocksize) flush();
    }

    private void addToBuffer(byte[] b, int off, int len) {
        if (len > buffer.length - cursor) { // if requesting to copy more than remaining space in buffer
            throw new IllegalArgumentException("invalid buffer copy requested in addToBuffer");
        }
        System.arraycopy(b, off, buffer, cursor, len);
        cursor += len;
    }

    @Override
    public void flush() throws IOException {
        if (streamClosed) throw new IOException("attempting to flush a closed stream;");
        if (cursor == 0) return;  // nothing to flush
        if (!created && isCreate) {   // actually, checking just !created would suffice; but leaving isCreated for readability
            RequestOptions opts = new RequestOptions();
            opts.retryPolicy = new NoRetryPolicy();
            OperationResponse resp = new OperationResponse();
            Core.create(filename, overwrite, buffer, 0, cursor, client, opts, resp);
            if (!resp.successful) {
                throw Core.getExceptionFromResp(resp, "Error creating file " + filename);
            }
            created = true;
        } else {
            RequestOptions opts = new RequestOptions();
            opts.retryPolicy = new NoRetryPolicy();
            OperationResponse resp = new OperationResponse();
            Core.append(filename, buffer, 0, cursor, client, opts, resp);
            if (!resp.successful) {
                throw Core.getExceptionFromResp(resp, "Error appending to file " + filename);
            }
        }
        cursor = 0;
    }

    @Override
    public void close() throws IOException {
        flush();
        streamClosed = true;
    }
}
