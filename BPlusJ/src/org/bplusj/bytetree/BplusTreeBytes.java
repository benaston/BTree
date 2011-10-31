package org.bplusj.bytetree;

import java.util.Enumeration;
import java.util.Hashtable;

import org.bplusj.BplusTreeException;
import org.bplusj.LinkedFile;
import org.bplusj.treelong.BplusTreeLong;

/**
 * Summary description for BplusTreeBytes.
 * 
 * @author Aaron Watters
 */
public class BplusTreeBytes implements IByteTree {

    //ATTRIBUTES
    private BplusTreeLong tree;

    private LinkedFile archive;

    private Hashtable freeChunksOnCommit = new Hashtable();

    private Hashtable freeChunksOnAbort = new Hashtable();

    private static int DEFAULTBLOCKSIZE = 1024;

    private static int DEFAULTNODESIZE = 32;

    //CONSTRUCTORS
    /**
     * @param tree
     * @param archive
     */
    public BplusTreeBytes(BplusTreeLong tree, LinkedFile archive) {
        this.tree = tree;
        this.archive = archive;
    }

    //PUBLIC METHODS

    /**
     * @param path
     * @return
     * @throws Exception
     */
    public static java.io.RandomAccessFile makeFile(String path)
            throws Exception {
        java.io.File f = new java.io.File(path);
        if (f.exists()) {
            f.delete();
        }
        return new java.io.RandomAccessFile(path, "rw");
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @param KeyLength
     * @param CultureId
     * @param nodesize
     * @param buffersize
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes initialize(String treefileName,
            String blockfileName, int KeyLength, int CultureId, int nodesize,
            int buffersize) throws Exception {
        java.io.RandomAccessFile treefile = makeFile(treefileName);
        java.io.RandomAccessFile blockfile = makeFile(blockfileName);
        return initialize(treefile, blockfile, KeyLength, CultureId, nodesize,
                buffersize);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @param KeyLength
     * @param CultureId
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes initialize(String treefileName,
            String blockfileName, int KeyLength, int CultureId)
            throws Exception {
        java.io.RandomAccessFile treefile = makeFile(treefileName);
        java.io.RandomAccessFile blockfile = makeFile(blockfileName);
        return initialize(treefile, blockfile, KeyLength, CultureId);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @param KeyLength
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes initialize(String treefileName,
            String blockfileName, int KeyLength) throws Exception {
        java.io.RandomAccessFile treefile = makeFile(treefileName);
        java.io.RandomAccessFile blockfile = makeFile(blockfileName);
        return initialize(treefile, blockfile, KeyLength);
    }

    /**
     * @param treefile
     * @param blockfile
     * @param KeyLength
     * @param CultureId
     * @param nodesize
     * @param buffersize
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int KeyLength, int CultureId,
            int nodesize, int buffersize) throws Exception {
        BplusTreeLong tree = BplusTreeLong.initializeInStream(treefile,
                KeyLength, nodesize, CultureId);
        LinkedFile archive = LinkedFile.InitializeLinkedFileInStream(blockfile,
                buffersize);
        return new BplusTreeBytes(tree, archive);
    }

    /**
     * @param treefile
     * @param blockfile
     * @param KeyLength
     * @param CultureId
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int KeyLength, int CultureId)
            throws Exception {
        return initialize(treefile, blockfile, KeyLength, CultureId,
                DEFAULTNODESIZE, DEFAULTBLOCKSIZE);
    }

    /**
     * @param treefile
     * @param blockfile
     * @param KeyLength
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int KeyLength) throws Exception {
        int CultureId = BplusTreeLong.INVARIANTCULTUREID;
        return initialize(treefile, blockfile, KeyLength, CultureId,
                DEFAULTNODESIZE, DEFAULTBLOCKSIZE);
    }

    /**
     * @param treefile
     * @param blockfile
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes reOpen(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile) throws Exception {
        BplusTreeLong tree = BplusTreeLong.setupFromExistingStream(treefile);
        LinkedFile archive = LinkedFile.SetupFromExistingStream(blockfile);
        return new BplusTreeBytes(tree, archive);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @param access
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes reOpen(String treefileName,
            String blockfileName, String access) throws Exception {
        java.io.RandomAccessFile treefile = new java.io.RandomAccessFile(
                treefileName, access);
        java.io.RandomAccessFile blockfile = new java.io.RandomAccessFile(
                blockfileName, access);
        return reOpen(treefile, blockfile);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes reOpen(String treefileName,
            String blockfileName) throws Exception {
        return reOpen(treefileName, blockfileName, "rw");
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @return
     * @throws Exception
     */
    public static BplusTreeBytes readOnly(String treefileName,
            String blockfileName) throws Exception {
        return reOpen(treefileName, blockfileName, "r");
    }

    /**
     * Use non-culture sensitive total order on binary Strings.
     */
    public void NoCulture() {
    }

    /**
     * @return
     * @throws Exception
     */
    public int MaxKeyLength() throws Exception {
        return this.tree.maxKeyLength();
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#compare(java.lang.String,
     *      java.lang.String)
     */
    public int compare(String left, String right) throws Exception {
        return this.tree.compare(left, right);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#shutDown()
     */
    public void shutDown() throws Exception {
        this.tree.shutDown();
        this.archive.Shutdown();
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#recover(boolean)
     */
    public void recover(boolean CorrectErrors) throws Exception {
        this.tree.recover(CorrectErrors);
        Hashtable ChunksInUse = new Hashtable();
        String key = this.tree.firstKey();
        while (key != null) {
            Long buffernumber = new Long(this.tree.get(key));
            if (ChunksInUse.containsKey(buffernumber)) {
                throw new BplusTreeException("buffer number " + buffernumber
                        + " associated with more than one key '" + key
                        + "' and '" + ChunksInUse.get(buffernumber) + "'");
            }
            ChunksInUse.put(buffernumber, key);
            key = this.tree.nextKey(key);
        }
        // also consider the un-deallocated chunks to be in use
        for (Enumeration e = this.freeChunksOnCommit.keys(); e
                .hasMoreElements();) {
            Long buffernumber = (Long) e.nextElement();
            ChunksInUse.put(buffernumber, "awaiting commit");
        }
        this.archive.Recover(ChunksInUse, CorrectErrors);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#removeKey(java.lang.String)
     */
    public void removeKey(String key) throws Exception {
        long map = this.tree.get(key);
        Long M = new Long(map);
        if (this.freeChunksOnAbort.containsKey(M)) {
            // free it now
            this.freeChunksOnAbort.remove(M);
            this.archive.ReleaseBuffers(map);
        } else {
            // free when committed
            this.freeChunksOnCommit.put(M, M);
        }
        this.tree.removeKey(key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#firstKey()
     */
    public String firstKey() throws Exception {
        return this.tree.firstKey();
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#nextKey(java.lang.String)
     */
    public String nextKey(String AfterThisKey) throws Exception {
        return this.tree.nextKey(AfterThisKey);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#containsKey(java.lang.String)
     */
    public boolean containsKey(String key) throws Exception {
        return this.tree.containsKey(key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#get(java.lang.String,
     *      java.lang.Object)
     */
    public Object get(String key, Object defaultValue) throws Exception {
        long map;
        if (this.tree.containsKey(key)) {
            map = this.tree.lastValueFound;
            return (Object) this.archive.GetChunk(map);
        }
        return defaultValue;
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#set(java.lang.String,
     *      java.lang.Object)
     */
    public void set(String key, Object map) throws Exception {
        byte[] thebytes = (byte[]) map;
        this.set(key, thebytes);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.IByteTree#set(java.lang.String,
     *      byte[])
     */
    public void set(String key, byte[] value) throws Exception {
        long storage = this.archive.StoreNewChunk(value, 0, value.length);
        Long S = new Long(storage);
        this.freeChunksOnAbort.put(S, S);
        long valueFound;
        if (this.tree.containsKey(key)) {
            valueFound = this.tree.lastValueFound;
            Long F = new Long(valueFound);
            if (this.freeChunksOnAbort.containsKey(F)) {
                // free it now
                this.freeChunksOnAbort.remove(F);
                this.archive.ReleaseBuffers(valueFound);
            } else {
                this.freeChunksOnCommit.put(F, F);
            }
        }
        this.tree.set(key, storage);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.IByteTree#get(java.lang.String)
     */
    public byte[] get(String key) throws Exception {
        long map = this.tree.get(key);
        return this.archive.GetChunk(map);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#commit()
     */
    public void commit() throws Exception {
        // store all new bufferrs
        this.archive.Flush();
        // commit the tree
        this.tree.commit();
        // at this point the new buffers have been committed, now free the old
        // ones
        for (Enumeration e = this.freeChunksOnCommit.keys(); e
                .hasMoreElements();) {
            long chunknumber = ((Long) e.nextElement()).longValue();
            this.archive.ReleaseBuffers(chunknumber);
        }
        this.archive.Flush();
        this.clearBookKeeping();
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#abort()
     */
    public void abort() throws Exception {
        for (Enumeration e = this.freeChunksOnAbort.keys(); e.hasMoreElements();) {
            long chunknumber = ((Long) e.nextElement()).longValue();
            this.archive.ReleaseBuffers(chunknumber);
        }
        this.tree.abort();
        this.archive.Flush();
        this.clearBookKeeping();
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#setFootPrintLimit(int)
     */
    public void setFootPrintLimit(int limit) throws Exception {
        this.tree.setFootPrintLimit(limit);
    }

    /**
     * @throws Exception
     */
    void clearBookKeeping() throws Exception {
        this.freeChunksOnCommit.clear();
        this.freeChunksOnAbort.clear();
    }

}
