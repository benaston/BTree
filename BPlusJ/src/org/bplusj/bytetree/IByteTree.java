package org.bplusj.bytetree;

import org.bplusj.ITreeIndex;

/**
 * A tree which returns byte array values
 * 
 * @author Aaron Watters
 */
public interface IByteTree extends ITreeIndex {
    /**
     * @param key
     * @return
     * @throws Exception
     */
    byte[] get(String key) throws Exception;
    /**
     * @param key
     * @param value
     * @throws Exception
     */
    void set(String key, byte[] value) throws Exception;
}