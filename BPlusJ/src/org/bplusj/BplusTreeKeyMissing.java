package org.bplusj;

/**
 * No such key found for attempted retrieval.
 * 
 * @author Aaron Watters
 */
public class BplusTreeKeyMissing extends Exception {
    /**
     * @param message
     */
    public BplusTreeKeyMissing(String message) {
        // do nothing extra
        super(message);
    }
}