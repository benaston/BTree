package org.bplusj;

import org.bplusj.bytetree.BplusTreeBytes;

/**
 * Tree index mapping Strings to Strings.
 * 
 * @author Aaron Waters
 */
public class BplusTree implements IStringTree {
    
    //ATTRIBUTES
    // Internal tree mapping Strings to bytes (for conversion to Strings).
    private ITreeIndex tree;

    //CONSTRUCTORS
    /**
     * @param tree
     */
    public BplusTree(ITreeIndex tree) {
        this.tree = tree;
    }

    //PUBLIC METHODS
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
    public static BplusTree initialize(String treefileName,
            String blockfileName, int KeyLength, int CultureId, int nodesize,
            int buffersize) throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.initialize(treefileName,
                blockfileName, KeyLength, CultureId, nodesize, buffersize);
        return new BplusTree(tree);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @param KeyLength
     * @param CultureId
     * @return
     * @throws Exception
     */
    public static BplusTree initialize(String treefileName,
            String blockfileName, int KeyLength, int CultureId)
            throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.initialize(treefileName,
                blockfileName, KeyLength, CultureId);
        return new BplusTree(tree);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @param KeyLength
     * @return
     * @throws Exception
     */
    public static BplusTree initialize(String treefileName,
            String blockfileName, int KeyLength) throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.initialize(treefileName,
                blockfileName, KeyLength);
        return new BplusTree(tree);
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
    public static BplusTree initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int KeyLength, int CultureId,
            int nodesize, int buffersize) throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.initialize(treefile, blockfile,
                KeyLength, CultureId, nodesize, buffersize);
        return new BplusTree(tree);
    }

    /**
     * @param treefile
     * @param blockfile
     * @param KeyLength
     * @param CultureId
     * @return
     * @throws Exception
     */
    public static BplusTree initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int KeyLength, int CultureId)
            throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.initialize(treefile, blockfile,
                KeyLength, CultureId);
        return new BplusTree(tree);
    }

    /**
     * @param treefile
     * @param blockfile
     * @param KeyLength
     * @return
     * @throws Exception
     */
    public static BplusTree initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int KeyLength) throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.initialize(treefile, blockfile,
                KeyLength);
        return new BplusTree(tree);
    }

    /**
     * @param treefile
     * @param blockfile
     * @return
     * @throws Exception
     */
    public static BplusTree reOpen(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile) throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.reOpen(treefile, blockfile);
        return new BplusTree(tree);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @return
     * @throws Exception
     */
    public static BplusTree reOpen(String treefileName, String blockfileName)
            throws Exception {
        BplusTreeBytes tree = BplusTreeBytes
                .reOpen(treefileName, blockfileName);
        return new BplusTree(tree);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @return
     * @throws Exception
     */
    public static BplusTree readOnly(String treefileName, String blockfileName)
            throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.readOnly(treefileName,
                blockfileName);
        return new BplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#recover(boolean)
     */
    public void recover(boolean CorrectErrors) throws Exception {
        this.tree.recover(CorrectErrors);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#removeKey(java.lang.String)
     */
    public void removeKey(String key) throws Exception {
        this.tree.removeKey(key);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#firstKey()
     */
    public String firstKey() throws Exception {
        return this.tree.firstKey();
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#nextKey(java.lang.String)
     */
    public String nextKey(String AfterThisKey) throws Exception {
        return this.tree.nextKey(AfterThisKey);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#containsKey(java.lang.String)
     */
    public boolean containsKey(String key) throws Exception {
        return this.tree.containsKey(key);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#get(java.lang.String, java.lang.Object)
     */
    public Object get(String key, Object defaultValue) throws Exception {
        Object test = this.tree.get(key, null);
        if (test != null) {
            return bytesToString((byte[]) test);
        }
        return defaultValue;
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#set(java.lang.String, java.lang.Object)
     */
    public void set(String key, Object map) throws Exception {
        String theString = (String) map;
        byte[] bytes = stringToBytes(theString);
        this.tree.set(key, bytes);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#commit()
     */
    public void commit() throws Exception {
        this.tree.commit();
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#abort()
     */
    public void abort() throws Exception {
        this.tree.abort();
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#setFootPrintLimit(int)
     */
    public void setFootPrintLimit(int limit) throws Exception {
        this.tree.setFootPrintLimit(limit);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#shutDown()
     */
    public void shutDown() throws Exception {
        this.tree.shutDown();
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#compare(java.lang.String, java.lang.String)
     */
    public int compare(String left, String right) throws Exception {
        return this.tree.compare(left, right);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.IStringTree#get(java.lang.String)
     */
    public String get(String key) throws Exception {
        Object theGet = this.tree.get(key, null);
        if (theGet != null) {
            byte[] bytes = (byte[]) theGet;
            return bytesToString(bytes);
        }
        throw new BplusTreeKeyMissing("key not found " + key);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.IStringTree#set(java.lang.String, java.lang.String)
     */
    public void set(String key, String value) throws Exception {
        byte[] bytes = stringToBytes(value);
        this.tree.set(key, bytes);
    }

    /**
     * @param bytes
     * @return
     * @throws Exception
     */
    public static String bytesToString(byte[] bytes) throws Exception {
        return new String(bytes, 0, bytes.length, "UTF-8");
    }

    public static byte[] stringToBytes(String theString) throws Exception {
        return theString.getBytes("UTF-8");
    }
}
