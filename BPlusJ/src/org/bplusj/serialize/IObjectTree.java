/*
 * Created on Sep 27, 2005
 * Contribution for the BPlusTree product from visioncodified
 * Copyright (c) 2005, Siddhartha P. Chandurkar siddhartha@visioncodified.com
 */
package org.bplusj.serialize;

import java.io.Serializable;

import org.bplusj.ITreeIndex;

/**
 * The interface to implement a Tree which stores serializable object
 * @author Siddhartha P. Chandurkar (siddhartha@visioncodified.com)
 */
public interface IObjectTree extends ITreeIndex {
    /**
     * Takes the key based on which the serialized object has to be 
     * searched.
     * @param key the key
     * @return serialized object
     * @throws Exception
     */
    Object get(String key) throws Exception;
    /**
     * Takes the key based on which the serialized object has to be 
     * saved in the tree and the object which has to be serialized.
     * The value object has to implement the java.io.Serializable interface.
     * @param key the key
     * @return serialized object
     * @throws Exception
     */
    void set(String key,  Serializable value) throws Exception;
}
