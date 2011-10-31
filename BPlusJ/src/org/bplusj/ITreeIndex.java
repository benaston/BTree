package org.bplusj;

/**
 * This is the shared interface among the various tree index implementations.
 * Each implementation also supports an indexing notation this[key] which is not
 * included here because of type incompatibilities.
 * 
 * @author Aaron Watters
 */
public interface ITreeIndex {
    /**
     * Examine the structure and optionally try to reclaim unreachable space. A
     * structure which was modified without a concluding commit or abort may
     * contain unreachable space.
     * 
     * @param CorrectErrors
     *            if true try to correct errors detected, if false throw an
     *            exception on errors.
     * @throws Exception
     */
    void recover(boolean CorrectErrors) throws Exception;

    /**
     * Dispose of the key and its associated value. Throw an exception if the
     * key is missing.
     * 
     * @param key
     *            Key to erase.
     * @throws Exception
     */
    void removeKey(String key) throws Exception;

    /**
     * Get the least key in the structure.
     * 
     * @return least key value or null if the tree is empty.
     * @throws Exception
     */
    String firstKey() throws Exception;

    /**
     * Get the least key in the structure strictly "larger" than the argument.
     * Return null if there is no such key.
     * 
     * @param AfterThisKey
     *            The "lower limit" for the value to return
     * @return Least key greater than argument or null
     * @throws Exception
     */
    String nextKey(String AfterThisKey) throws Exception;

    /**
     * Return true if the key is present in the structure.
     * 
     * @param key
     *            Key to test
     * @return true if present, otherwise false.
     * @throws Exception
     */
    boolean containsKey(String key) throws Exception;

    /**
     * Get the Object associated with the key, or return the default if the key
     * is not present.
     * 
     * @param key
     *            Key to retrieve.
     * @param defaultValue
     *            default value to use if absent.
     * @return the mapped value boxed as an Object
     * @throws Exception
     */
    Object get(String key, Object defaultValue) throws Exception;

    /**
     * map the key to the value in the structure.
     * 
     * @param key the key
     * @param map
     *            The value (must coerce to the appropriate value for the tree
     *            instance).
     * @throws Exception
     */

    void set(String key, Object map) throws Exception;

    /**
     * Make changes since the last commit permanent.
     * 
     * @throws Exception
     */
    void commit() throws Exception;

    /**
     * Discard changes since the last commit and return to the state at the last
     * commit point.
     * 
     * @throws Exception
     */
    void abort() throws Exception;

    /**
     * Set a parameter used to decide when to release memory mapped buffers.
     * Larger values mean that more memory is used but accesses may be faster
     * especially if there is locality of reference. 5 is too small and 1000 may
     * be too big.
     * 
     * @param limit
     *            maximum number of leaves with no materialized children to keep
     *            in memory.
     * @throws Exception
     */
    void setFootPrintLimit(int limit) throws Exception;

    /**
     * Close and flush the streams without committing or aborting. (This is
     * equivalent to abort, except unused space in the streams may be left
     * unreachable).
     * 
     * @throws Exception
     */
    void shutDown() throws Exception;

    /**
     * Use the culture context for this tree to compare two Strings.
     * 
     * @param left
     * @param right
     * @return
     * @throws Exception
     */
    int compare(String left, String right) throws Exception;
}
