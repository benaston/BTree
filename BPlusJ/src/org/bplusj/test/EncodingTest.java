package org.bplusj.test;

import org.bplusj.hash.HBplusTreeBytes;

/**
 * @author Aaron Watters
 */
public class EncodingTest 
{

/**
 * The main function
 * @param argv
 * @throws Exception
 */
public static void main(String argv[]) 
			throws Exception
{
	HBplusTreeBytes HT = (HBplusTreeBytes) 
		HBplusTreeBytes.initialize("/tmp/junk.bin", "/tmp/junk2.bin", 6);
	String stuff = "cæser";
	String test = HT.prefixForByteCount(stuff, 5);
	System.out.println("test="+test);
	//HT[stuff] = "goober";
	byte[] bytes = new byte[0];
	HT.set(stuff, bytes);
}

}