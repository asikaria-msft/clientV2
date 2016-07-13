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

import java.io.EOFException;
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
    private final ADLStoreClient client;
    private final DirectoryEntry directoryEntry;

    private int blocksize = 4 * 1024 * 1024;
    private byte[] buffer = new byte[blocksize]; //4MB byte-buffer

    private long fCursor = 0;  // bCursor of buffer within file - offset of next byte to read
    private int bCursor = 0;   // bCursor of read within buffer - offset of next byte to be returned from buffer
    private int limit = 0;     // offset of next byte to be read into buffer from service (i.e., upper marker+1
                               //                                                      of valid bytes in buffer)
    private boolean streamClosed = false;


    // no constructor - use Factory Method in AzureDataLakeStoreClient
    ADLFileInputStream(String filename, DirectoryEntry de, ADLStoreClient client) {
        super();
        this.filename = filename;
        this.client = client;
        this.directoryEntry = de;
        if (log.isTraceEnabled()) {
            log.trace("ADLFIleInputStream created for client {} for file {}", client.getClientId(), filename);
        }
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int i = read(b, 0, 1);
        if (i<0) return i;
            else return (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (b == null) {
            throw new NullPointerException("null byte array passed in to read() method");
        }
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (streamClosed) throw new IOException("attempting to read from a closed stream");
        if (b == null) {
            throw new NullPointerException("null byte array passed in to read() method");
        }

        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.read(b,off,{}) at offset {} using client {} from file {}", len, getPos(), client.getClientId(), filename);
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
    protected long readFromService() throws IOException {
        if (bCursor < limit) return 0; //if there's still unread data in the buffer then dont overwrite it
        if (fCursor >= directoryEntry.length) return -1; // At or past end of file

        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.readFromService() - at offset {} using client {} from file {}", getPos(), client.getClientId(), filename);
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
        if (!resp.successful) throw client.getExceptionFromResp(resp, "Error reading from file " + filename);
        if (resp.responseContentLength == 0 && !resp.responseChunked) return 0;  //Got nothing
        int bytesRead;
        int totalBytesRead = 0;
        try {
            do {
                bytesRead = str.read(buffer, limit, blocksize - limit);
                if (bytesRead > 0) { // if not EOF of the Core.open's stream
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
     * @throws IOException if there is an error
     * @throws EOFException if attempting to seek past end of file
     */
    public void seek(long n) throws IOException, EOFException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.seek({}) using client {} for file {}", n, client.getClientId(), filename);
        }
        if (streamClosed) throw new IOException("attempting to seek into a closed stream;");
        if (n<0) throw new EOFException("Cannot seek to before the beginning of file");
        if (n>directoryEntry.length) throw new EOFException("Cannot seek past end of file");

        if (n>=fCursor-limit && n<=fCursor) { // within buffer
            bCursor = (int) (n-(fCursor-limit));
            return;
        }

        // next read will read from here
        fCursor = n;

        //invalidate buffer
        limit = 0;
        bCursor = 0;
    }

    @Override
    public long skip(long n) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.skip({}) using client {} for file {}", n, client.getClientId(), filename);
        }
        if (streamClosed) throw new IOException("attempting to skip() on a closed stream");
        long currentPos = getPos();
        long newPos = currentPos + n;
        if (newPos < 0) {
            newPos = 0;
            n = newPos - currentPos;
        }
        if (newPos > directoryEntry.length) {
            newPos = directoryEntry.length;
            n = newPos - currentPos;
        }
        seek(newPos);
        return n;
    }

    /**
     * Sets the size of the internal read buffer (default is 4MB).
     * @param newSize requested size of buffer
     * @throws ADLException if there is an error
     */
    public void setBufferSize(int newSize) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.setBufferSize({}) using client {} for file {}", newSize, client.getClientId(), filename);
        }
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
        if (streamClosed) throw new IOException("attempting to call available() on a closed stream");
        return limit - bCursor;
    }

    /**
     * gets the position of the cursor within the file
     * @return position of the cursor
     * @throws IOException throws {@link IOException} if there is an error
     */
    public long getPos() throws IOException {
        if (streamClosed) throw new IOException("attempting to call getPos() on a closed stream");
        return fCursor - limit + bCursor;
    }

    /**
     * invalidates the buffer. The next read will fetch data from server.
     * @throws IOException throws {@link IOException} if there is an error
     */
    public void unbuffer() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.unbuffer() for client {} for file {}", client.getClientId(), filename);
        }
        fCursor = getPos();
        limit = 0;
        bCursor = 0;
    }

    @Override
    public void close() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.close() for client {} for file {}", client.getClientId(), filename);
        }
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
