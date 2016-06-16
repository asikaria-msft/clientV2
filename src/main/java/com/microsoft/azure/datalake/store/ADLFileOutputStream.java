package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@code ADLFileOutputStream} is used to add data to an Azure Data Lake File.
 * It is a buffering stream that accumulates user writes, and then writes to the server
 * in chunks. Default chunk size if 4MB.
 *
 */
public class ADLFileOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.ADLFileOutputStream");

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
        if (log.isTraceEnabled()) {
            log.trace("ADLFIleOutputStream created for client {} for file {}, create={}", client.getClientId(), filename, isCreate);
        }
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

        if (log.isTraceEnabled()) {
            log.trace("Stream write of size {} for client {} for file {}", len, client.getClientId(), filename);
        }

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
        if (!created && isCreate) {   // actually, checking just !created would suffice; but leaving isCreate for readability
            RequestOptions opts = new RequestOptions();
            opts.retryPolicy = new NoRetryPolicy();
            OperationResponse resp = new OperationResponse();
            if (log.isTraceEnabled()) {
                log.trace("create file with data size {} for client {} for file {}", cursor, client.getClientId(), filename);
            }
            Core.create(filename, overwrite, buffer, 0, cursor, client, opts, resp);
            if (!resp.successful) {
                throw Core.getExceptionFromResp(resp, "Error creating file " + filename);
            }
            created = true;
        } else if (cursor > 0){  // if there is anything to flush
            RequestOptions opts = new RequestOptions();
            opts.retryPolicy = new NoRetryPolicy();
            OperationResponse resp = new OperationResponse();
            if (log.isTraceEnabled()) {
                log.trace("append to file with data size {} for client {} for file {}", cursor, client.getClientId(), filename);
            }
            Core.append(filename, buffer, 0, cursor, client, opts, resp);
            if (!resp.successful) {
                throw Core.getExceptionFromResp(resp, "Error appending to file " + filename);
            }
        }
        cursor = 0;
    }

    @Override
    public void close() throws IOException {
        if(streamClosed) return; // Return silently upon multiple closes
        flush();
        streamClosed = true;
        if (log.isTraceEnabled()) {
            log.trace("Stream closed for client {} for file {}", client.getClientId(), filename);
        }
    }
}
