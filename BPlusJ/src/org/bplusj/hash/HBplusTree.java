package org.bplusj.hash;

import org.bplusj.BplusTree;

/**
 * Tree index mapping Strings to Strings with unlimited key length
 * 
 * @author Aaron Watters
 */
public class HBplusTree extends BplusTree {
    private HBplusTreeBytes xtree;

    /**
     * @param tree
     */
    public HBplusTree(HBplusTreeBytes tree) {
        super(tree);
        this.xtree = tree;
    }

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
        HBplusTreeBytes tree = (HBplusTreeBytes) HBplusTreeBytes.initialize(
                treefileName, blockfileName, PrefixLength, CultureId, nodesize,
                buffersize);
        return new HBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.lang.String, java.lang.String, int, int)
     */
    public static BplusTree initialize(String treefileName,
            String blockfileName, int PrefixLength, int CultureId)
            throws Exception {
        HBplusTreeBytes tree = (HBplusTreeBytes) HBplusTreeBytes.initialize(
                treefileName, blockfileName, PrefixLength, CultureId);
        return new HBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.lang.String, java.lang.String, int)
     */
    public static BplusTree initialize(String treefileName,
            String blockfileName, int PrefixLength) throws Exception {
        HBplusTreeBytes tree = (HBplusTreeBytes) HBplusTreeBytes.initialize(
                treefileName, blockfileName, PrefixLength);
        return new HBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.io.RandomAccessFile, java.io.RandomAccessFile, int, int, int, int)
     */
    public static BplusTree initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int PrefixLength,
            int CultureId, int nodesize, int buffersize) throws Exception {
        HBplusTreeBytes tree = (HBplusTreeBytes) HBplusTreeBytes.initialize(
                treefile, blockfile, PrefixLength, CultureId, nodesize,
                buffersize);
        return new HBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.io.RandomAccessFile, java.io.RandomAccessFile, int, int)
     */
    public static BplusTree initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId)
            throws Exception {
        HBplusTreeBytes tree = (HBplusTreeBytes) HBplusTreeBytes.initialize(
                treefile, blockfile, PrefixLength, CultureId);
        return new HBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#initialize(java.io.RandomAccessFile, java.io.RandomAccessFile, int)
     */
    public static BplusTree initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int KeyLength) throws Exception {
        HBplusTreeBytes tree = (HBplusTreeBytes) HBplusTreeBytes.initialize(
                treefile, blockfile, KeyLength);
        return new HBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#reOpen(java.io.RandomAccessFile, java.io.RandomAccessFile)
     */
    public static BplusTree reOpen(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile) throws Exception {
        HBplusTreeBytes tree = (HBplusTreeBytes) HBplusTreeBytes.reOpen(
                treefile, blockfile);
        return new HBplusTree(tree);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.BplusTree#readOnly(java.lang.String, java.lang.String)
     */
    public static BplusTree readOnly(String treefileName, String blockfileName)
            throws Exception {
        HBplusTreeBytes tree = (HBplusTreeBytes) HBplusTreeBytes.readOnly(
                treefileName, blockfileName);
        return new HBplusTree(tree);
    }
}
