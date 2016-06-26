/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialOnThrottlePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;


/**
 * ADLFileInputStream can be used to read data from an open file on ADL.
 * It is a buffering stream, that reads data from the server in bulk, and then
 * satisfies user reads from the buffer. Default buffer size is 4MB.
 *
 *
 */
public class ADLFileInputStream extends InputStream {

    /*
    Hadoop interfaces not implemented: (Remove comment after first Code Review from Vishwajeet)
     - PositionedReadable: can be done in the shim, using Core.open directly
     - ByteBufferReadable: can be done in the shim, using Core.open directly
     - HasFileDescriptor: seems orthogonal to this class's functionality
     - CanSetDropBehind: seems like a synonym of the unbuffer() method (already implemented in this class)
     - HasEnhancedByteBufferAccess: Not sure why this even is in the FSDataInputStream - seems orthogonal
     - CanSetReadahead/CanSetDropbehind - not that hard to do, but not sure who uses it. Will do if needed.
     */

    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.ADLFileInputStream");

    private final String filename;
    private final AzureDataLakeStorageClient client;

    private int blocksize = 4 * 1024 * 1024;
    private byte[] buffer = new byte[blocksize]; //4MB byte-buffer

    private long fCursor = 0;  // bCursor of buffer within file - offset of next byte to read
    private int bCursor = 0;   // bCursor of read within buffer - offset of next byte to be returned from buffer
    private int limit = 0;     // offset of next byte to be read into buffer from service (i.e., upper marker+1
                               //                                                      of valid bytes in buffer)
    private boolean streamClosed = false;


    // no constructor - use Factory Method in AzureDataLakeStoreClient
    ADLFileInputStream(String filename, AzureDataLakeStorageClient client) {
        super();
        this.filename = filename;
        this.client = client;
        if (log.isTraceEnabled()) {
            log.trace("ADLFIleInputStream created for client {} for file {}", client.getClientId(), filename);
        }
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int i = read(b, 0, 1);
        if (i<0) return i;
            else return b[0];
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (streamClosed) throw new IOException("attempting to read from a closed stream;");
        if (b == null) {
            throw new NullPointerException();
        }

        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (log.isTraceEnabled()) {
            log.trace("read at offset {} size {} using client {} from file {}", getPos(), len, client.getClientId(), filename);
        }

        if (len == 0) {
            return 0;
        }

        //If buffer is empty, then fill the buffer. If EOF, then return -1
        if (bCursor == limit)
        {
            if (readFromService() < 0) return -1;
        }

        //If there is anything in the buffer, then return lesser of (requested bytes) and (bytes in buffer)
        //(bytes returned may be less than requested)
        int bytesRemaining = limit - bCursor;
        int bytesToRead = Math.min(len, bytesRemaining);
        System.arraycopy(buffer, bCursor, b, off, bytesToRead);
        bCursor += bytesToRead;
        return bytesToRead;
    }

    /**
     * Read from service attempts to read {@code blocksize} bytes from service.
     * Returns how many bytes are actually read, could be less than blocksize.
     *
     * @return number of bytes actually read
     * @throws ADLException if error
     */
    protected long readFromService() throws ADLException {
        if (bCursor < limit) return 0; //if there's still unread data in the buffer then dont overwrite it

        if (log.isTraceEnabled()) {
            log.trace("read from server at offset {} using client {} from file {}", getPos(), client.getClientId(), filename);
        }

        //reset buffer to initial state - i.e., throw away existing data
        bCursor = 0;
        limit = 0;

        // make server call to get more data
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        InputStream str = Core.open(filename, fCursor, blocksize, client, opts, resp);
        if (resp.httpResponseCode == 403 || resp.httpResponseCode == 416) {
            resp.successful = true;
            return -1; //End-of-file
        }
        if (!resp.successful) throw Core.getExceptionFromResp(resp, "Error reading from file " + filename);
        if (resp.responseContentLength == 0 && !resp.responseChunked) return 0;  //Got nothing
        int bytesRead;
        int totalBytesRead = 0;
        try {
            do {
                bytesRead = str.read(buffer, limit, blocksize - limit);
                if (bytesRead > 0) { // if not EOF
                    limit += bytesRead;
                    fCursor += bytesRead;
                    totalBytesRead += bytesRead;
                }
            } while (bytesRead >= 0 && limit < blocksize);
            str.close();
        } catch (IOException ex) {
            throw new ADLException("Error reading data", ex);
        }
        return totalBytesRead;
    }

    /**
     * Seek to given position in stream.
     * @param n position to seek to
     * @return actual position after the seek. This may differ from the requested position.
     * @throws IOException if there is an error
     */
    public long seek(long n) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("begin seek to offset {} using client {} from file {}", n, client.getClientId(), filename);
        }
        long skiplength = n - getPos();
        skip(skiplength);
        return getPos();
    }

    @Override
    public long skip(long n) throws IOException {
        if (streamClosed) throw new IOException("attempting to read from a closed stream;");
        if (n==0) return 0;
        if (n < -getPos()) throw new IllegalArgumentException("Cannot seek past beginning of file");
        // cannot do corresponding check for end of file, because we dont know length without doing a server call

        if (log.isTraceEnabled()) {
            log.trace("begin skip by {} using client {} from file {}", n, client.getClientId(), filename);
        }

        if (n<0) {
            int max = -bCursor; // max distance we can go back within buffer
            if (n<max) { // if past beginning of buffer, then need to seek from server. Invalidate buffer and read
                fCursor = getPos() + n; // n is -ve
                limit=0;
                bCursor = 0;
                readFromService();
            } else {  // within buffer; all we need to do is reset the buffer pointer backwards
                bCursor += n; // n is -ve
            }
            return n;
        }

        // at this point n>0, since we've already checked for <0 and ==0
        if (bCursor + n <= limit) { // within buffer; all we need to do is reset the buffer pointer
            bCursor += n;
        } else {  // past end of buffer; read from server.
            int oldLimit = limit;
            int oldBCursor = bCursor;
            long oldFCursor = fCursor;
            fCursor = getPos() + n;
            limit=0;
            bCursor = 0;
            if (readFromService()<0)
            {   // past end of file. restore state and report zero movement
                limit = oldLimit;
                bCursor = oldBCursor;
                fCursor = oldFCursor;
                return 0;
            }
        }
        return n;
    }

    /**
     * Sets the size of the internal read buffer (default is 4MB).
     * @param newSize requested size of buffer
     * @throws ADLException if there is an error
     */
    public void setBufferSize(int newSize) throws ADLException {
        if (newSize <=0) throw new IllegalArgumentException("Buffer size cannot be zero or less: " + newSize);
        if (newSize == blocksize) return;  // nothing to do

        // discard existing buffer.
        // We could write some code to keep what we can from existing buffer, but given this call will
        // be rarely used, and even when used will likely be right after the stream is constructed,
        // the extra complexity is not worth it.
        unbuffer();

        blocksize = newSize;
        buffer = new byte[blocksize];
    }

    /**
     * returns the remaining number of bytes available to read from the buffer, without having to call
     * the server
     *
     * @return the number of bytes availabel
     * @throws IOException throws {@link ADLException} if call fails
     */
    @Override
    public int available() throws IOException {
        return limit - bCursor;
    }

    /**
     * gets the position of the cursor within the file
     * @return position of the cursor
     */
    long getPos() {
        return fCursor - limit + bCursor;
    }

    /**
     * invalidates the buffer. The next read will fetch data from server.
     */
    public void unbuffer() {
        if (log.isTraceEnabled()) {
            log.trace("ADLInput Stream cleared buffer for client {} for file {}", client.getClientId(), filename);
        }
        fCursor = getPos();
        limit = 0;
        bCursor = 0;
    }

    @Override
    public void close() throws IOException {
        streamClosed = true;
        if (log.isTraceEnabled()) {
            log.trace("ADLInput Stream closed for client {} for file {}", client.getClientId(), filename);
        }
    }

    /**
     * Not supported by this stream. Throws {@link UnsupportedOperationException}
     * @param readlimit ignored
     */
    @Override
    public synchronized void mark(int readlimit) {
      throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
    }

    /**
     * Not supported by this stream. Throws {@link UnsupportedOperationException}
     */
    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
    }

    /**
     * gets whether mark and reset are supported by {@code ADLFileInputStream}. Always returns false.
     *
     * @return always {@code false}
     */
    @Override
    public boolean markSupported() {
        return false;
    }
}
