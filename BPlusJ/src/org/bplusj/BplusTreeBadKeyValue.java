package org.bplusj;

/**
 * @author Aaron Waters
 */
public class BplusTreeBadKeyValue extends Exception {
    
    
    
    //PUBLIC METHODS
    /**
     * @param message
     */
    public BplusTreeBadKeyValue(String message)//: base(message)
    {
        super(message);
    }
}