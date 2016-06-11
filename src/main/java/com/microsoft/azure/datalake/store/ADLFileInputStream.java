package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.protocol.Core;
import com.microsoft.azure.datalake.store.protocol.OperationResponse;
import com.microsoft.azure.datalake.store.protocol.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialOnThrottlePolicy;

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
     - CanSetDropBehing: seems like a synonym of the unbuffer() method (already implemented in this class)
     - HasEnhancedByteBufferAccess: Not sure why this even is in the FSDataInputStream - seems orthogonal
     - CanSetReadahead/CanSetDropbehind - not that hard to do, but not sure who uses it. Will do if needed.
     */

    private final String filename;
    private final AzureDataLakeStorageClient client;

    private final int blocksize = 4 * 1024 *1024;
    private final byte[] buffer = new byte[blocksize]; //4MB byte buffer

    private long fCursor = 0;  // bCursor of buffer within file - offset of next offset to read
    private int bCursor = 0;   // bCursor of read within buffer - offset of next byte to be returned from buffer
    private int limit = 0;     // offset of next byte to be read into buffer from service (i.e., upper marker+1
                               //                                                      of valid bytes in buffer)
    private boolean streamClosed = false;

    protected long readFromService(long len) throws ADLException {
        if (bCursor < limit) return 0; //if there's still unread data in the buffer then dont overwrite it

        //reset buffer to initial state - i.e., throw away existing data
        bCursor = 0;
        limit = 0;

        // make server call to get more data
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialOnThrottlePolicy();
        OperationResponse resp = new OperationResponse();
        InputStream str = Core.open(filename, fCursor, len, client, opts, resp);
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
                if (bytesRead > 0) {
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


    // no constructor - use Factory Method in AzureDataLakeStoreClient
    ADLFileInputStream(String filename, AzureDataLakeStorageClient client) {
        super();
        this.filename = filename;
        this.client = client;
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

        if (len == 0) {
            return 0;
        }

        //If buffer is empty, then fill the buffer. If EOF, then return -1
        if (bCursor == limit)
        {
            if (readFromService(blocksize) < 0) return -1;
        }

        //If there is anything in the buffer, then return lesser of (requested bytes) and (bytes in buffer)
        //(bytes returned may be less than requested)
        int bytesRemaining = limit - bCursor;
        int bytesToRead = Math.min(len, bytesRemaining);
        System.arraycopy(buffer, bCursor, b, off, bytesToRead);
        bCursor += bytesToRead;
        return bytesToRead;
    }

    @Override
    public long skip(long n) throws IOException {
        if (streamClosed) throw new IOException("attempting to read from a closed stream;");
        if (n==0) return 0;
        if (n < -(fCursor+bCursor)) throw new IllegalArgumentException("Cannot seek past beginning of file");
        // cannot do corresponding check for end of file, because we dont know length without doing a server call

        if (n<0) {
            int max = -(limit-bCursor); // max distance we can go back within buffer
            if (n<max) { // if past beginning of buffer, then need to seek from server. Invalidate buffer and read
                limit=0;
                bCursor = 0;
                fCursor = fCursor + bCursor + n; // n is -ve
                readFromService(blocksize);
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
            limit=0;
            bCursor = 0;
            fCursor = fCursor + bCursor + n; // n is -ve
            if (readFromService(blocksize)<0)
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
     * @throws IOException throws {@link ADLException} if call fails
     */
    long getPos() throws IOException {
        return fCursor + bCursor;
    }

    /**
     * invalidates the buffer. The next read will fetch data from server.
     */
    public void unbuffer() {
        limit = 0;
        bCursor = 0;
    }

    @Override
    public void close() throws IOException {
        streamClosed = true;
    }

    @Override
    public synchronized void mark(int readlimit) {
      throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
    }

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
