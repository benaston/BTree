package org.bplusj;

/**
 * A tree which returns byte array values
 * 
 * @author Aaron Watters
 */
public interface IStringTree extends ITreeIndex {
    /**
     * @param key
     * @return
     * @throws Exception
     */
    String get(String key) throws Exception;

    /**
     * @param key
     * @param value
     * @throws Exception
     */
    void set(String key, String value) throws Exception;
}