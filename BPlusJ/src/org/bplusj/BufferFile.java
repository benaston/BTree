package org.bplusj;

/**
 * Provides an indexed object which maps to buffers in an underlying file object
 * 
 * @author Aaron Watters
 */
public class BufferFile {
    
    //ATTRIBUTES
    private java.io.RandomAccessFile fromfile;

    private int headerSize;

    // this should really be a read only property
    public int bufferSize;

    long seekStart = 0;

    public static byte[] HEADERPREFIX = { 98, 112, 78, 98, 102 };

    public static/* const */byte VERSION = 0;

    public static/* const */int INTSTORAGE = 4;

    public static/* const */int LONGSTORAGE = 8;

    public static/* const */int SHORTSTORAGE = 2;

    public static int MINBUFFERSIZE = 16;

    //CONSTRUCTORS
    /**
     * @param fromFile
     * @param buffersize
     * @param seekStart
     * @throws Exception
     */
    public BufferFile(java.io.RandomAccessFile fromfile, int buffersize,
            long seekStart) throws Exception {
        this.seekStart = seekStart;
        this.fromfile = fromfile;
        this.bufferSize = buffersize;
        this.headerSize = HEADERPREFIX.length + INTSTORAGE + 1; // +version
        // byte+4 bytes
        // for
        // buffersize
        this.sanityCheck();
    }

    /**
     * @param fromFile
     * @param buffersize
     * @throws Exception
     */
    public BufferFile(java.io.RandomAccessFile fromfile, int buffersize)
            throws Exception {
        this(fromfile, buffersize, (long) 0);
    }
    
    //PUBLIC METHODS

    /**
     * @param fromFile
     * @return
     * @throws Exception
     */
    public static BufferFile SetupFromExistingStream(
            java.io.RandomAccessFile fromfile) throws Exception {
        return SetupFromExistingStream(fromfile, (long) 0);
    }

    public static BufferFile SetupFromExistingStream(
            java.io.RandomAccessFile fromfile, long StartSeek) throws Exception {
        BufferFile result = new BufferFile(fromfile, 100, StartSeek); // dummy
        // buffer
        // size
        // for now
        result.readHeader();
        return result;
    }

    /**
     * @param fromFile
     * @param buffersize
     * @return
     * @throws Exception
     */
    public static BufferFile InitializeBufferFileInStream(
            java.io.RandomAccessFile fromfile, int buffersize) throws Exception {
        return InitializeBufferFileInStream(fromfile, buffersize, (long) 0);
    }

    /**
     * @param fromFile
     * @param buffersize
     * @param StartSeek
     * @return
     * @throws Exception
     */
    public static BufferFile InitializeBufferFileInStream(
            java.io.RandomAccessFile fromfile, int buffersize, long StartSeek)
            throws Exception {
        BufferFile result = new BufferFile(fromfile, buffersize, StartSeek);
        result.setHeader();
        return result;
    }

    /**
     * @throws Exception
     */
    void sanityCheck() throws Exception {
        if (this.bufferSize < MINBUFFERSIZE) {
            throw new BufferFileException("buffer size too small "
                    + this.bufferSize);
        }
        if (this.seekStart < 0) {
            throw new BufferFileException("can't start at negative position "
                    + this.seekStart);
        }
    }

    /**
     * @param buffernumber
     * @param toArray
     * @param startingAt
     * @param length
     * @throws Exception
     */
    public void getBuffer(long buffernumber, byte[] toArray, int startingAt,
            int length) throws Exception {
        if (buffernumber >= this.nextBufferNumber()) {
            throw new BufferFileException("last buffer is "
                    + this.nextBufferNumber() + " not " + buffernumber);
        }
        if (length > this.bufferSize) {
            throw new BufferFileException(
                    "buffer size too small for retrieval " + bufferSize
                            + " need " + length);
        }
        long seekPosition = this.bufferSeek(buffernumber);
        this.fromfile.seek(seekPosition);
        this.fromfile.read(toArray, startingAt, length);
    }

    /**
     * @param buffernumber
     * @param fromArray
     * @param startingAt
     * @param length
     * @throws Exception
     */
    public void setBuffer(long buffernumber, byte[] fromArray, int startingAt,
            int length) throws Exception {
        if (length > this.bufferSize) {
            throw new BufferFileException(
                    "buffer size too small for assignment " + bufferSize
                            + " need " + length);
        }
        if (buffernumber > this.nextBufferNumber()) {
            throw new BufferFileException("cannot skip buffer numbers from "
                    + this.nextBufferNumber() + " to " + buffernumber);
        }
        long seekPosition = this.bufferSeek(buffernumber);
        this.fromfile.seek(seekPosition);
        this.fromfile.write(fromArray, startingAt, length);
    }

    /**
     * @throws Exception
     */
    void setHeader() throws Exception {
        byte[] header = this.makeHeader();
        this.fromfile.seek(this.seekStart);
        this.fromfile.write(header, 0, header.length);
    }

    /**
     * @throws Exception
     */
    public void Flush() throws Exception {
        // In java apparently the file doesn't need to be flushed
        // XXXX might improve performance to use a buffered file wrapper?
        //this.fromFile.Flush();
    }

    /**
     * @throws Exception
     */
    void readHeader() throws Exception {
        byte[] header = new byte[this.headerSize];
        this.fromfile.seek(this.seekStart);
        this.fromfile.read(header, 0, this.headerSize);
        int index = 0;
        for (int i = 0; i < HEADERPREFIX.length; i++) {
            byte b = HEADERPREFIX[i];
            if (header[index] != b) {
                throw new BufferFileException("invalid header prefix");
            }
            index++;
        }
        index++;
        // read buffersize
        this.bufferSize = Retrieve(header, index);
        this.sanityCheck();
    }

    /**
     * @return
     * @throws Exception
     */
    public byte[] makeHeader() throws Exception {
        byte[] result = new byte[this.headerSize];
        copyBytesTo(HEADERPREFIX, result, 0);
        result[HEADERPREFIX.length] = VERSION;
        Store(this.bufferSize, result, HEADERPREFIX.length + 1);
        return result;
    }

    /**
     * @param fromArray
     * @param toArray
     * @param destinationIndex
     * @throws Exception
     */
    public static void copyBytesTo(byte[] fromArray, byte[] toArray,
            int destinationIndex) throws Exception {
        for (int i = 0; i < fromArray.length; i++) {
            toArray[i + destinationIndex] = fromArray[i];
        }
    }

    /**
     * @param bufferNumber
     * @return
     * @throws Exception
     */
    long bufferSeek(long bufferNumber) throws Exception {
        if (bufferNumber < 0) {
            throw new BufferFileException("buffer number cannot be negative");
        }
        return this.seekStart + this.headerSize
                + (this.bufferSize * bufferNumber);
    }

    /**
     * @return
     * @throws Exception
     */
    public long nextBufferNumber() throws Exception {
        // round up the buffer number based on the current file length
        long filelength = this.fromfile.length();
        long bufferspace = filelength - this.headerSize - this.seekStart;
        long nbuffers = bufferspace / this.bufferSize;
        long remainder = bufferspace % this.bufferSize;
        if (remainder > 0) {
            return nbuffers + 1;
        }
        return nbuffers;
    }

    // there are probably libraries for this, but whatever...
    public static void Store(int TheInt, byte[] ToArray, int atIndex)
            throws Exception {
        int limit = INTSTORAGE;
        if (atIndex + limit > ToArray.length) {
            throw new BufferFileException("can't access beyond end of array");
        }
        for (int i = 0; i < limit; i++) {
            byte thebyte = (byte) (TheInt & 0xff);
            //System.out.println("storing "+thebyte+" from "+TheInt);
            ToArray[atIndex + i] = thebyte;
            TheInt = TheInt >> 8;
        }
    }

    /**
     * @param TheShort
     * @param ToArray
     * @param atIndex
     * @throws Exception
     */
    public static void Store(short TheShort, byte[] ToArray, int atIndex)
            throws Exception {
        int limit = SHORTSTORAGE;
        int TheInt = TheShort;
        if (atIndex + limit > ToArray.length) {
            throw new BufferFileException("can't access beyond end of array");
        }
        for (int i = 0; i < limit; i++) {
            byte thebyte = (byte) (TheInt & 0xff);
            ToArray[atIndex + i] = thebyte;
            TheInt = TheInt >> 8;
        }
    }

    /**
     * @param ToArray
     * @param atIndex
     * @return
     * @throws Exception
     */
    public static int Retrieve(byte[] ToArray, int atIndex) throws Exception {
        int limit = INTSTORAGE;
        if (atIndex + limit > ToArray.length) {
            throw new BufferFileException("can't access beyond end of array");
        }
        int result = 0;
        for (int i = 0; i < limit; i++) {
            int thebyte = ToArray[atIndex + limit - i - 1];
            if (thebyte < 0) {
                thebyte += 256;
            }
            result = result << 8;
            result = result | thebyte;
        }
        return result;
    }

    /**
     * @param TheLong
     * @param ToArray
     * @param atIndex
     * @throws Exception
     */
    public static void Store(long TheLong, byte[] ToArray, int atIndex)
            throws Exception {
        int limit = LONGSTORAGE;
        if (atIndex + limit > ToArray.length) {
            throw new BufferFileException("can't access beyond end of array");
        }
        for (int i = 0; i < limit; i++) {
            byte thebyte = (byte) (TheLong & 0xff);
            ToArray[atIndex + i] = thebyte;
            TheLong = TheLong >> 8;
        }
    }

    /**
     * @param ToArray
     * @param atIndex
     * @return
     * @throws Exception
     */
    public static long RetrieveLong(byte[] ToArray, int atIndex)
            throws Exception {
        int limit = LONGSTORAGE;
        if (atIndex + limit > ToArray.length) {
            throw new BufferFileException("can't access beyond end of array");
        }
        long result = 0;
        for (int i = 0; i < limit; i++) {
            int thebyte = ToArray[atIndex + limit - i - 1];
            if (thebyte < 0) {
                thebyte += 256;
            }
            result = result << 8;
            result = result | thebyte;
        }
        return result;
    }

    /**
     * @param ToArray
     * @param atIndex
     * @return
     * @throws Exception
     */
    public static short RetrieveShort(byte[] ToArray, int atIndex)
            throws Exception {
        int limit = SHORTSTORAGE;
        if (atIndex + limit > ToArray.length) {
            throw new BufferFileException("can't access beyond end of array");
        }
        int result = 0;
        for (int i = 0; i < limit; i++) {
            int thebyte = ToArray[atIndex + limit - i - 1];
            if (thebyte < 0) {
                thebyte += 256;
            }
            result = (result << 8);
            result = result | thebyte;
        }
        return (short) result;
    }
}
