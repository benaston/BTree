package org.bplusj;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 * Chunked singly linked file with garbage collection.
 * 
 * @author Aaron Watters
 */
public class LinkedFile {
  
    //ATTRRIBUTES
    private static long NULLBUFFERPOINTER = -1;

    private java.io.RandomAccessFile fromfile;

    private BufferFile buffers;

    private int buffersize;

    private int headersize;

    private long seekStart = 0;

    private long FreeListHead = NULLBUFFERPOINTER;

    private long RecentNewBufferNumber = NULLBUFFERPOINTER;

    private boolean headerDirty = true;

    private byte FREE = 0;

    private byte HEAD = 1;

    private byte BODY = 2;

    private static byte[] HEADERPREFIX = { 98, 112, 78, 108, 102 };

    private static byte VERSION = 0;

    private static int MINBUFFERSIZE = 20;

    // next pointer and indicator flag
    private static int BUFFEROVERHEAD = BufferFile.LONGSTORAGE + 1;

    //CONSTRUCTORS
    /**
     * @param buffersize
     * @param seekStart
     * @throws Exception
     */
    public LinkedFile(int buffersize, long seekStart) throws Exception {
        this.seekStart = seekStart;
        this.buffersize = buffersize;
        this.headersize = HEADERPREFIX.length + 1 + BufferFile.INTSTORAGE
                + BufferFile.LONGSTORAGE;
        this.sanityCheck();
    }

    //PUBLIC METHODS
    /**
     * @param fromFile
     * @return
     * @throws Exception
     */
    public static LinkedFile SetupFromExistingStream(
            java.io.RandomAccessFile fromfile) throws Exception {
        return SetupFromExistingStream(fromfile, (long) 0);
    }

    /**
     * @param fromFile
     * @param StartSeek
     * @return
     * @throws Exception
     */
    public static LinkedFile SetupFromExistingStream(
            java.io.RandomAccessFile fromfile, long StartSeek) throws Exception {
        LinkedFile result = new LinkedFile(100, StartSeek); // dummy buffer size
        // for now
        result.fromfile = fromfile;
        result.readHeader();
        result.buffers = BufferFile.SetupFromExistingStream(fromfile, StartSeek
                + result.headersize);
        return result;
    }

    /**
     * @throws Exception
     */
    void readHeader() throws Exception {
        byte[] header = new byte[this.headersize];
        this.fromfile.seek(this.seekStart);
        this.fromfile.read(header, 0, this.headersize);
        int index = 0;
        // check prefix
        for (int i = 0; i < HEADERPREFIX.length; i++) {
            byte b = HEADERPREFIX[i];
            if (header[index] != b) {
                throw new LinkedFileException("invalid header prefix");
            }
            index++;
        }
        // skip version (for now)
        index++;
        // read buffersize
        this.buffersize = BufferFile.Retrieve(header, index);
        index += BufferFile.INTSTORAGE;
        this.FreeListHead = BufferFile.RetrieveLong(header, index);
        this.sanityCheck();
        this.headerDirty = false;
    }

    /**
     * @param fromFile
     * @param buffersize
     * @return
     * @throws Exception
     */
    public static LinkedFile InitializeLinkedFileInStream(
            java.io.RandomAccessFile fromfile, int buffersize) throws Exception {
        return InitializeLinkedFileInStream(fromfile, buffersize, (long) 0);
    }

    /**
     * @param fromFile
     * @param buffersize
     * @param StartSeek
     * @return
     * @throws Exception
     */
    public static LinkedFile InitializeLinkedFileInStream(
            java.io.RandomAccessFile fromfile, int buffersize, long StartSeek)
            throws Exception {
        LinkedFile result = new LinkedFile(buffersize, StartSeek);
        result.fromfile = fromfile;
        result.setHeader();
        // buffersize should be increased by overhead...
        result.buffers = BufferFile.InitializeBufferFileInStream(fromfile,
                buffersize + BUFFEROVERHEAD, StartSeek + result.headersize);
        return result;
    }

    /**
     * @throws Exception
     */
    public void setHeader() throws Exception {
        byte[] header = this.makeHeader();
        this.fromfile.seek(this.seekStart);
        this.fromfile.write(header, 0, header.length);
        this.headerDirty = false;
    }

    /**
     * @return
     * @throws Exception
     */
    public byte[] makeHeader() throws Exception {
        byte[] result = new byte[this.headersize];
        for (int i = 0; i < HEADERPREFIX.length; i++) {
            result[i] = HEADERPREFIX[i];
        }
        result[HEADERPREFIX.length] = VERSION;
        int index = HEADERPREFIX.length + 1;
        BufferFile.Store(this.buffersize, result, index);
        index += BufferFile.INTSTORAGE;
        BufferFile.Store(this.FreeListHead, result, index);
        return result;
    }

    /**
     * @param ChunksInUse
     * @param FixErrors
     * @throws Exception
     */
    public void Recover(Hashtable ChunksInUse, boolean FixErrors)
            throws Exception {
        // find missing space and recover it
        this.checkStructure(ChunksInUse, FixErrors);
    }

    /**
     * @throws Exception
     */
    void sanityCheck() throws Exception {
        if (this.seekStart < 0) {
            throw new LinkedFileException("cannot seek negative "
                    + this.seekStart);
        }
        if (this.buffersize < MINBUFFERSIZE) {
            throw new LinkedFileException("buffer size too small "
                    + this.buffersize);
        }
    }

    /**
     * @throws Exception
     */
    public void Shutdown() throws Exception {
        this.fromfile.close();
    }

    /**
     * @author Aaron Watters
     */
    public class ParseBuffer {
        public byte[] payload;

        public byte type;

        public long NextBufferNumber;

        /**
         * @param bufferNumber
         * @throws Exception
         */
        public ParseBuffer(long bufferNumber) throws Exception {
            byte[] thebuffer = new byte[LinkedFile.this.buffersize];
            byte[] fullbuffer = new byte[LinkedFile.this.buffersize
                    + LinkedFile.BUFFEROVERHEAD];
            LinkedFile.this.buffers.getBuffer(bufferNumber, fullbuffer, 0,
                    fullbuffer.length);
            this.type = fullbuffer[0];
            this.NextBufferNumber = BufferFile.RetrieveLong(fullbuffer, 1);
            for (int i = 0; i < LinkedFile.this.buffersize; i++) {
                thebuffer[i] = fullbuffer[i + LinkedFile.BUFFEROVERHEAD];
            }
            this.payload = thebuffer;
        }
    }

    /**
     * @param buffernumber
     * @param type
     * @param thebuffer
     * @param start
     * @param length
     * @param NextBufferNumber
     * @throws Exception
     */
    void SetBuffer(long buffernumber, byte type, byte[] thebuffer, int start,
            int length, long NextBufferNumber) throws Exception {
        if (this.buffersize < length) {
            throw new LinkedFileException("buffer size too small "
                    + this.buffersize + "<" + length);
        }
        byte[] fullbuffer = new byte[length + BUFFEROVERHEAD];
        fullbuffer[0] = type;
        BufferFile.Store(NextBufferNumber, fullbuffer, 1);
        if (thebuffer != null) {
            //Array.Copy(thebuffer, start, fullbuffer, BUFFEROVERHEAD, length);
            for (int i = 0; i < length; i++) {
                fullbuffer[BUFFEROVERHEAD + i] = thebuffer[i];
            }
        }
        this.buffers.setBuffer(buffernumber, fullbuffer, 0, fullbuffer.length);
    }

    /**
     * @param buffernumber
     * @throws Exception
     */
    void DeallocateBuffer(long buffernumber) throws Exception {

        // should be followed by resetting the header eventually.
        this.SetBuffer(buffernumber, FREE, null, 0, 0, this.FreeListHead);
        this.FreeListHead = buffernumber;
        this.headerDirty = true;
    }

    /**
     * @return
     * @throws Exception
     */
    long AllocateBuffer() throws Exception {
        if (this.FreeListHead != NULLBUFFERPOINTER) {
            // reallocate a freed buffer
            long result = this.FreeListHead;
            ParseBuffer P = new ParseBuffer(result);
            byte buffertype = P.type;
            long NextFree = P.NextBufferNumber;
            if (buffertype != FREE) {
                throw new LinkedFileException(
                        "free head buffer not marked free");
            }
            this.FreeListHead = NextFree;
            this.headerDirty = true;
            this.RecentNewBufferNumber = NULLBUFFERPOINTER;
            return result;
        } else {
            // allocate a new buffer
            long NextBufferNumber = this.buffers.nextBufferNumber();
            if (this.RecentNewBufferNumber == NextBufferNumber) {
                // the previous buffer has been allocated but not yet written.
                // It must be written before the following one...
                NextBufferNumber++;
            }
            this.RecentNewBufferNumber = NextBufferNumber;
            return NextBufferNumber;
        }
    }

    /**
     * @throws Exception
     */
    public void checkStructure() throws Exception {
        checkStructure(null, false);
    }

    /**
     * @param ChunksInUse
     * @param FixErrors
     * @throws Exception
     */
    public void checkStructure(Hashtable ChunksInUse, boolean FixErrors)
            throws Exception {
        Hashtable buffernumberToType = new Hashtable();
        Hashtable buffernumberToNext = new Hashtable();
        Hashtable visited = new Hashtable();
        long LastBufferNumber = this.buffers.nextBufferNumber();
        for (long buffernumber = 0; buffernumber < LastBufferNumber; buffernumber++) {
            ParseBuffer P = new ParseBuffer(buffernumber);
            byte buffertype = P.type;
            long NextBufferNumber = P.NextBufferNumber;
            buffernumberToType
                    .put(new Long(buffernumber), new Byte(buffertype));
            buffernumberToNext.put(new Long(buffernumber), new Long(
                    NextBufferNumber));
        }
        // traverse the freelist
        long thisFreeBuffer = this.FreeListHead;
        while (thisFreeBuffer != NULLBUFFERPOINTER) {
            if (visited.containsKey(new Long(thisFreeBuffer))) {
                throw new LinkedFileException("cycle in freelist "
                        + thisFreeBuffer);
            }
            visited.put(new Long(thisFreeBuffer), new Long(thisFreeBuffer));
            byte thetype = ((Byte) buffernumberToType.get(new Long(
                    thisFreeBuffer))).byteValue();
            long NextBufferNumber = ((Long) buffernumberToNext.get(new Long(
                    thisFreeBuffer))).longValue();
            if (thetype != FREE) {
                throw new LinkedFileException(
                        "free list element not marked free " + thisFreeBuffer);
            }
            thisFreeBuffer = NextBufferNumber;
        }
        // traverse all nodes marked head
        Hashtable allchunks = new Hashtable();
        for (long buffernumber = 0; buffernumber < LastBufferNumber; buffernumber++) {
            byte thetype = ((Byte) buffernumberToType
                    .get(new Long(buffernumber))).byteValue();
            if (thetype == HEAD) {
                if (visited.containsKey(new Long(buffernumber))) {
                    throw new LinkedFileException(
                            "head buffer already visited " + buffernumber);
                }
                allchunks.put(new Long(buffernumber), new Long(buffernumber));
                visited.put(new Long(buffernumber), new Long(buffernumber));
                long bodybuffernumber = ((Long) buffernumberToNext
                        .get(new Long(buffernumber))).longValue();
                while (bodybuffernumber != NULLBUFFERPOINTER) {
                    byte bodytype = ((Byte) buffernumberToType.get(new Long(
                            bodybuffernumber))).byteValue();
                    long NextBufferNumber = ((Long) buffernumberToNext
                            .get(new Long(bodybuffernumber))).longValue();
                    if (visited.containsKey(new Long(bodybuffernumber))) {
                        throw new LinkedFileException(
                                "body buffer visited twice " + bodybuffernumber);
                    }
                    visited.put(new Long(bodybuffernumber), new Byte(bodytype));
                    if (bodytype != BODY) {
                        throw new LinkedFileException(
                                "body buffer not marked body " + thetype);
                    }
                    bodybuffernumber = NextBufferNumber;
                }
                // check retrieval
                this.GetChunk(buffernumber);
            }
        }
        // make sure all were visited
        for (long buffernumber = 0; buffernumber < LastBufferNumber; buffernumber++) {
            if (!visited.containsKey(new Long(buffernumber))) {
                throw new LinkedFileException(
                        "buffer not found either as data or free "
                                + buffernumber);
            }
        }
        // check against in use list
        if (ChunksInUse != null) {
            ArrayList notInUse = new ArrayList();
            for (Enumeration e = ChunksInUse.keys(); e.hasMoreElements();) {
                long buffernumber = ((Long) e.nextElement()).longValue();
                if (!allchunks.containsKey(new Long(buffernumber))) {
                    throw new LinkedFileException(
                            "buffer in used list not found in linked file "
                                    + buffernumber);
                }
            }
            for (Enumeration e = allchunks.keys(); e.hasMoreElements();) {
                long buffernumber = ((Long) e.nextElement()).longValue();
                if (!ChunksInUse.containsKey(new Long(buffernumber))) {
                    if (!FixErrors) {
                        throw new LinkedFileException(
                                "buffer in linked file not in used list "
                                        + buffernumber);
                    }
                    notInUse.add(new Long(buffernumber));
                }
            }
            for (int iii = 0; iii < notInUse.size(); iii++) {
                long buffernumber = ((Long) notInUse.get(iii)).longValue();
                this.ReleaseBuffers(buffernumber);
            }
        }
    }

    /**
     * @param HeadBufferNumber
     * @return
     * @throws Exception
     */
    public byte[] GetChunk(long HeadBufferNumber) throws Exception {
        // get the head, interpret the length
        ParseBuffer P = new ParseBuffer(HeadBufferNumber);
        byte buffertype = P.type;
        long NextBufferNumber = P.NextBufferNumber;
        byte[] buffer = P.payload;
        int length = BufferFile.Retrieve(buffer, 0);
        if (length < 0) {
            throw new LinkedFileException(
                    "negative length block? must be garbage: " + length);
        }
        if (buffertype != HEAD) {
            throw new LinkedFileException("first buffer not marked HEAD");
        }
        byte[] result = new byte[length];
        // read in the data from the first buffer
        int firstLength = this.buffersize - BufferFile.INTSTORAGE;
        if (firstLength > length) {
            firstLength = length;
        }
        //Array.Copy(buffer, BufferFile.INTSTORAGE, result, 0, firstLength);
        for (int i = 0; i < firstLength; i++) {
            result[i] = buffer[BufferFile.INTSTORAGE + i];
        }
        int stored = firstLength;
        while (stored < length) {
            // get the next buffer
            long thisBufferNumber = NextBufferNumber;
            P = new ParseBuffer(thisBufferNumber);
            buffer = P.payload;
            buffertype = P.type;
            NextBufferNumber = P.NextBufferNumber;
            int nextLength = this.buffersize;
            if (length - stored < nextLength) {
                nextLength = length - stored;
            }
            for (int i = 0; i < nextLength; i++) {
                result[stored + i] = buffer[i];
            }
            stored += nextLength;
        }
        return result;
    }

    public long StoreNewChunk(byte[] fromArray, int startingAt, int length)
            throws Exception {
        // get the first buffer as result value
        long currentBufferNumber = this.AllocateBuffer();
        long result = currentBufferNumber;
        if (length < 0 || startingAt < 0) {
            throw new LinkedFileException(
                    "cannot store negative length chunk (" + startingAt + ","
                            + length + ")");
        }
        int endingAt = startingAt + length;
        // special case: zero length chunk
        if (endingAt > fromArray.length) {
            throw new LinkedFileException("array doesn't have this much data: "
                    + endingAt);
        }
        int index = startingAt;
        // store header with length information
        byte[] buffer = new byte[this.buffersize];
        BufferFile.Store(length, buffer, 0);
        int fromIndex = startingAt;
        int firstLength = this.buffersize - BufferFile.INTSTORAGE;
        int stored = 0;
        if (firstLength > length) {
            firstLength = length;
        }
        for (int i = 0; i < firstLength; i++) {
            buffer[BufferFile.INTSTORAGE + i] = fromArray[i];
        }
        stored += firstLength;
        fromIndex += firstLength;
        byte CurrentBufferType = HEAD;
        // store any remaining buffers (no length info)
        while (stored < length) {
            // store current buffer and get next block number
            long NextBufferNumber = this.AllocateBuffer();
            this.SetBuffer(currentBufferNumber, CurrentBufferType, buffer, 0,
                    buffer.length, NextBufferNumber);
            currentBufferNumber = NextBufferNumber;
            CurrentBufferType = BODY;
            int nextLength = this.buffersize;
            if (stored + nextLength > length) {
                nextLength = length - stored;
            }
            for (int i = 0; i < nextLength; i++) {
                buffer[i] = fromArray[fromIndex + i];
            }
            stored += nextLength;
            fromIndex += nextLength;
        }
        // store final buffer
        this.SetBuffer(currentBufferNumber, CurrentBufferType, buffer, 0,
                buffer.length, NULLBUFFERPOINTER);
        return result;
    }

    /**
     * @throws Exception
     */
    public void Flush() throws Exception {
        if (this.headerDirty) {
            this.setHeader();
        }
        this.buffers.Flush();
    }

    /**
     * @param HeadBufferNumber
     * @throws Exception
     */
    public void ReleaseBuffers(long HeadBufferNumber) throws Exception {
        // KISS
        long thisbuffernumber = HeadBufferNumber;
        ParseBuffer P = new ParseBuffer(HeadBufferNumber);
        long NextBufferNumber = P.NextBufferNumber;
        byte buffertype = P.type;
        if (buffertype != HEAD) {
            throw new LinkedFileException("head buffer not marked HEAD");
        }
        this.DeallocateBuffer(HeadBufferNumber);
        while (NextBufferNumber != NULLBUFFERPOINTER) {
            thisbuffernumber = NextBufferNumber;
            P = new ParseBuffer(thisbuffernumber);
            NextBufferNumber = P.NextBufferNumber;
            buffertype = P.type;
            if (buffertype != BODY) {
                throw new LinkedFileException("body buffer not marked BODY");
            }
            this.DeallocateBuffer(thisbuffernumber);
        }
    }
}
