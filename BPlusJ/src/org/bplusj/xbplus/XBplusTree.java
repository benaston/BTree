package org.bplusj.xbplus;

import org.bplusj.BplusTree;

/**
 * Tree index mapping Strings to Strings with unlimited key length
 * 
 * @author Aaron Watters
 */
public class XBplusTree extends BplusTree {
    //ATTRIBUTES
    private XBplusTreeBytes xtree;

    //CONSTRUCTORS
    /**
     * @param tree
     * @throws Exception
     */
    public XBplusTree(XBplusTreeBytes tree) throws Exception {
        super(tree);
        this.xtree = tree;
    }

    //PUBLIC METHODS
    /**
     * @param limit
     */
    public void limitBucketSize(int limit) {
        this.xtree.bucketSizeLimit = limit;
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.lang.String, java.lang.String, int, int, int, int)
     */
    public static BplusTree initialize(String treefileName,
            String blockfileName, int PrefixLength, int CultureId,
            int nodesize, int buffersize) throws Exception {
        XBplusTreeBytes tree = XBplusTreeBytes.initialize(treefileName,
                blockfileName, PrefixLength, CultureId, nodesize, buffersize);
        return new XBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.lang.String, java.lang.String, int, int)
     */
    public static BplusTree initialize(String treefileName,
            String blockfileName, int PrefixLength, int CultureId)
            throws Exception {
        XBplusTreeBytes tree = XBplusTreeBytes.initialize(treefileName,
                blockfileName, PrefixLength, CultureId);
        return new XBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.lang.String, java.lang.String, int)
     */
    public static BplusTree initialize(String treefileName,
            String blockfileName, int PrefixLength) throws Exception {
        XBplusTreeBytes tree = XBplusTreeBytes.initialize(treefileName,
                blockfileName, PrefixLength);
        return new XBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.io.RandomAccessFile, java.io.RandomAccessFile, int, int, int, int)
     */
    public static BplusTree initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int PrefixLength,
            int CultureId, int nodesize, int buffersize) throws Exception {
        XBplusTreeBytes tree = XBplusTreeBytes.initialize(treefile, blockfile,
                PrefixLength, CultureId, nodesize, buffersize);
        return new XBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.io.RandomAccessFile, java.io.RandomAccessFile, int, int)
     */
    public static BplusTree initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId)
            throws Exception {
        XBplusTreeBytes tree = XBplusTreeBytes.initialize(treefile, blockfile,
                PrefixLength, CultureId);
        return new XBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.io.RandomAccessFile, java.io.RandomAccessFile, int)
     */
    public static BplusTree initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int KeyLength) throws Exception {
        XBplusTreeBytes tree = XBplusTreeBytes.initialize(treefile, blockfile,
                KeyLength);
        return new XBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#reOpen(java.io.RandomAccessFile, java.io.RandomAccessFile)
     */
    public static BplusTree reOpen(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile) throws Exception {
        XBplusTreeBytes tree = XBplusTreeBytes.reOpen(treefile, blockfile);
        return new XBplusTree(tree);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @return
     * @throws Exception
     */
    public static BplusTree reOpen(String treefileName, String blockfileName)
            throws Exception {
        XBplusTreeBytes tree = XBplusTreeBytes.reOpen(treefileName,
                blockfileName);
        return new XBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#readOnly(java.lang.String, java.lang.String)
     */
    public static BplusTree readOnly(String treefileName, String blockfileName)
            throws Exception {
        XBplusTreeBytes tree = XBplusTreeBytes.readOnly(treefileName,
                blockfileName);
        return new XBplusTree(tree);
    }
}
