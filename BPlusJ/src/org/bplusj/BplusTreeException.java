package org.bplusj;

/**
 * Generic error including programming errors.
 * 
 * @author Aaron Watters
 */
public class BplusTreeException extends Exception {
    
    //PUBLIC METHODS
    /**
     * @param message
     */
    public BplusTreeException(String message) {
        // do nothing extra
        super(message);
    }
}