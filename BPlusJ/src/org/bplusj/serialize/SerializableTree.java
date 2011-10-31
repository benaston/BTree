/*
 * Created on Sep 27, 2005
 * Contribution for the BPlusTree product from visioncodified
 * Copyright (c) 2005, Siddhartha P. Chandurkar siddhartha@visioncodified.com
 */
package org.bplusj.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.bplusj.bytetree.IByteTree;

/**
 * This is a SerializableTree. which serializes and java object which has
 * implemented the java.io.Serializable Interface.
 * 
 * @author Siddhartha P. Chandurkar (siddhartha@visioncodified.com)
 */
public class SerializableTree implements IObjectTree {
    //ATTRIBUTES
    private IByteTree tree;

    //CONSTRUCTORS
    /**
     * The constructor takes any class which implements the IByteTree interface.
     * 
     * @param tree
     */
    public SerializableTree(IByteTree tree) {
        this.tree = tree;
    }

    //PUBLIC METHODS
    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.IObjectTree#get(java.lang.String)
     */
    public Object get(String key) throws Exception {
        byte[] bytes = this.tree.get(key);
        ByteArrayInputStream baInStream = new ByteArrayInputStream(bytes);
        ObjectInputStream iStream = new ObjectInputStream(baInStream);
        return iStream.readObject();
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.IObjectTree#set(java.lang.String,
     *      java.lang.Object)
     */
    public void set(String key, Serializable value) throws Exception {
        ByteArrayOutputStream baStream = new ByteArrayOutputStream();
        ObjectOutputStream oStream = new ObjectOutputStream(baStream);
        oStream.writeObject(value);
        this.tree.set(key, baStream.toByteArray());
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#recover(boolean)
     */
    public void recover(boolean correctErrors) throws Exception {
        this.tree.recover(correctErrors);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#removeKey(java.lang.String)
     */
    public void removeKey(String key) throws Exception {
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
        return this.tree.firstKey();
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
        if (this.tree.containsKey(key)) {
            return get(key);
        }
        return defaultValue;
    }

    public void set(String key, Object map) {
        this.set(key, map);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#commit()
     */
    public void commit() throws Exception {
        this.tree.commit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#abort()
     */
    public void abort() throws Exception {
        this.tree.abort();
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#setFootPrintLimit(int)
     */
    public void setFootPrintLimit(int limit) throws Exception {
        this.tree.setFootPrintLimit(limit);
    }

    /*
     * (non-Javadoc)
     * 
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#shutDown()
     */
    public void shutDown() throws Exception {
        this.tree.shutDown();

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

}
