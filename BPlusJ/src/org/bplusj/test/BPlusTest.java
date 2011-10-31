package org.bplusj.test;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeSet;

import org.bplusj.BplusTree;
import org.bplusj.BufferFile;
import org.bplusj.IStringTree;
import org.bplusj.LinkedFile;
import org.bplusj.hash.HBplusTree;
import org.bplusj.treelong.BplusTreeLong;
import org.bplusj.xbplus.XBplusTree;


/**
 * tests main entry point for BplusJ. Throws exception on failure.
 * 
 * @author Aaron Watters
 *  
 */
public class BPlusTest {

    static String tempdirectory = "c:\\tmp"; // set to a directory to test

    // storing to/from files

    static Hashtable allinserts = new Hashtable();

    static Hashtable lastcommittedinserts = new Hashtable();

    static boolean full = true;

    static int keylength = 20;

    static int prefixlength = 6;

    static int nodesize = 6;

    static int buffersize = 100;

    static int bucketsizelimit = 100; // sanity check

    static boolean DoAllTests = true;

    public static void main(String argv[]) throws Exception {
        if (DoAllTests) {
            //byteStringTest();
            intTests();
            longTests();
            shortTests();
            testBufferFile();
            LinkedFileTest();
            BplusTreeLongTest();
            Test();
            xTest();
            hTest();
            //sTest(); -- not implemented in java yet
        }
        //hTest();
        CompatTest();
    }
    private class Person implements Serializable{
        private int id;
        private String name;
        private int age;
        
        /**
         * @return Returns the age.
         */
        public int getAge() {
            return age;
        }
        /**
         * @param age The age to set.
         */
        public void setAge(int age) {
            this.age = age;
        }
        /**
         * @return Returns the id.
         */
        public int getId() {
            return id;
        }
        /**
         * @param id The id to set.
         */
        public void setId(int id) {
            this.id = id;
        }
        /**
         * @return Returns the name.
         */
        public String getName() {
            return name;
        }
        /**
         * @param name The name to set.
         */
        public void setName(String name) {
            this.name = name;
        }
    }
    
    /**
     * @param i
     * @param j
     * @param k
     * @param l
     * @return
     */
    public static String CompatKey(int i, int j, int k, int l) {
        String seed = "i=" + i + " j=" + j + " k=" + k + " ";
        String result = seed;
        //			for (int ii=0; ii<l; ii++)
        //			{
        //				result += seed;
        //			}
        return "" + l + result;
    }

    /**
     * @param i
     * @param j
     * @param k
     * @param l
     * @return
     */
    public static String CompatValue(int i, int j, int k, int l) {
        String result = CompatKey(k, j, l, i) + CompatKey(l, k, j, i);
        return result + result;
    }

    /**
     * @throws Exception
     */
    public static void CompatTest() throws Exception {
        if (tempdirectory == null) {
            System.out
                    .println(" compatibility test requires temp directory to be defined: please edit test source file");
            return;
        }
        String otherTreeFileName = tempdirectory + "/CsharpTree.dat";
        String otherBlocksFileName = tempdirectory + "/CsharpBlocks.dat";
        String myTreeFileName = tempdirectory + "/JavaTree.dat";
        String myBlocksFileName = tempdirectory + "/JavaBlocks.dat";
        Hashtable map = new Hashtable();
        System.out.println(" creating " + myTreeFileName + " and "
                + myBlocksFileName);
        if ((new java.io.File(myTreeFileName)).exists()) {
            System.out.println(" deleting existing files");
            (new java.io.File(myTreeFileName)).delete();
            (new java.io.File(myBlocksFileName)).delete();
        }
        BplusTree myTree = HBplusTree.initialize(myTreeFileName,
                myBlocksFileName, 6);
        for (int i = 0; i < 10; i++) {

            System.out.println(" " + i);
            for (int j = 0; j < 10; j++) {
                for (int k = 0; k < 10; k++) {
                    for (int l = 0; l < 10; l++) {
                        String TheKey = CompatKey(i, j, k, l);
                        String TheValue = CompatValue(i, j, k, l);
                        //map[TheKey] = TheValue;
                        map.put(TheKey, TheValue);
                        //myTree[TheKey] = TheValue;
                        myTree.set(TheKey, TheValue);
                    }
                }
            }
        }
        myTree.commit();
        myTree.shutDown();
        System.out.println(" trying to test " + otherTreeFileName + " and "
                + otherBlocksFileName);
        if (!(new java.io.File(otherTreeFileName)).exists()) {
            System.out.println(" file not created yet :(");
            return;
        }
        int count = 0;
        BplusTree otherTree = HBplusTree.readOnly(otherTreeFileName,
                otherBlocksFileName);
        //foreach (DictionaryEntry D in map)
        for (Enumeration e = map.keys(); e.hasMoreElements();) {
            if ((count % 1000) == 1) {
                System.out.println(" ... " + count);
            }
            String TheKey = (String) e.nextElement();
            String TheValue = (String) map.get(TheKey);
            String OtherValue = otherTree.get(TheKey);
            if (!OtherValue.equals(TheValue)) {
                throw new Exception(" Values don't match " + TheValue + " "
                        + OtherValue);
            }
            count++;
        }
        System.out.println(" compatibility test ok");
    }

    /**
     * @param name
     * @return
     * @throws Exception
     */
    public static java.io.RandomAccessFile makeFile(String name)
            throws Exception {
        if (tempdirectory == null) {
            System.out
                    .println("to run these tests you need to edit the source file, adding a String definition for tempdirectory");
            throw new Exception(
                    "to run these tests you need to edit the source file, adding a String definition for tempdirectory");
        }
        String path = tempdirectory + "/" + name;
        // delete it if it exists
        java.io.File f = new java.io.File(path);
        if (f.exists()) {
            System.out.println("<br>				DELETING FILE " + path);
            f.delete();
        }
        return new java.io.RandomAccessFile(path, "rw");
    }

    /**
     * @param i
     * @param j
     * @param k
     * @return
     */
    static String keyMaker(int i, int j, int k) {
        int selector = (i + j + k) % 3;
        String result = "" + i + "." + j + "." + k;
        if (selector == 0) {
            result = "" + k + "." + (j % 5) + "." + i;
        } else if (selector == 1) {
            result = "" + k + "." + j + "." + i;
        }
        return result;
    }

    /**
     * @param i
     * @param j
     * @param k
     * @return
     */
    static String xkeyMaker(int i, int j, int k) {
        String result = keyMaker(i, j, k);
        result = result + result + result;
        result = result + keyMaker(k, i, j);
        return result;
    }

    /**
     * @param i
     * @param j
     * @param k
     * @return
     */
    static String ValueMaker(int i, int j, int k) {
        if (((i + j + k) % 5) == 3) {
            return "";
        }
        //System.Text.StringBuilder sb = new System.Text.StringBuilder();
        String result = "";
        //sb.Append("value");
        result += "value";
        for (int x = 0; x < i + k * 5; x++) {
            //sb.Append(j);
            result += j;
            //sb.Append(k);
            result += k;
        }
        return result;
    }

    /**
     * @throws Exception
     */
    public static void Test() throws Exception {
        System.out.println("TESTING BPLUSTREE");
        //System.IO.Stream treefile=null, blockfile=null;
        java.io.RandomAccessFile treefile = makeFile("bptTree.dat");
        java.io.RandomAccessFile blockfile = makeFile("bptBlock.dat");
        BplusTree bpt = BplusTree.initialize(treefile, blockfile, keylength);
        //BplusTree bpt = getTree(treefile, blockfile);
        Hashtable allmaps = new Hashtable();
        for (int i = 0; i < 10; i++) {
            System.out.println("Pass " + i + " of 10");
            bpt.setFootPrintLimit(16 - i);
            for (int j = 0; j < 30; j++) {
                exercise(bpt, i, j, allmaps, false);
                if ((j % 4) == 2) {
                    bpt = BplusTree.reOpen(treefile, blockfile);
                }
                // now check the structure
                checkStructure(allmaps, bpt, true);
            }
        }
    }

    /**
     * @throws Exception
     */
    public static void xTest() throws Exception {
        System.out.println("TESTING BPLUSTREE");
        //System.IO.Stream treefile=null, blockfile=null;
        java.io.RandomAccessFile treefile = makeFile("bptTreeX.dat");
        java.io.RandomAccessFile blockfile = makeFile("bptBlockX.dat");
        BplusTree bpt = XBplusTree.initialize(treefile, blockfile, keylength);
        //BplusTree bpt = getTree(treefile, blockfile);
        Hashtable allmaps = new Hashtable();
        for (int i = 0; i < 10; i++) {
            System.out.println("Pass " + i + " of 10");
            bpt.setFootPrintLimit(16 - i);
            for (int j = 0; j < 30; j++) {
                exercise(bpt, i, j, allmaps, true);
                if ((j % 4) == 2) {
                    bpt = XBplusTree.reOpen(treefile, blockfile);
                }
                // now check the structure
                checkStructure(allmaps, bpt, true);
            }
        }
    }

    /**
     * @throws Exception
     */
    public static void hTest() throws Exception {
        System.out.println("TESTING BPLUSTREE");
        //System.IO.Stream treefile=null, blockfile=null;
        java.io.RandomAccessFile treefile = makeFile("bptTreeH.dat");
        java.io.RandomAccessFile blockfile = makeFile("bptBlockH.dat");
        BplusTree bpt = HBplusTree.initialize(treefile, blockfile, keylength);
        //BplusTree bpt = getTree(treefile, blockfile);
        Hashtable allmaps = new Hashtable();
        for (int i = 0; i < 10; i++) {
            System.out.println("Pass " + i + " of 10");
            bpt.setFootPrintLimit(16 - i);
            for (int j = 0; j < 30; j++) {
                exercise(bpt, i, j, allmaps, true);
                if ((j % 4) == 2) {
                    bpt = HBplusTree.reOpen(treefile, blockfile);
                }
                // now check the structure
                checkStructure(allmaps, bpt, false);
            }
        }
    }

    /**
     * @param bpt
     * @param i
     * @param j
     * @param allmaps
     * @param extended
     * @throws Exception
     */
    public static void exercise(IStringTree bpt, int i, int j,
            Hashtable allmaps, boolean extended) throws Exception {
        Hashtable record = new Hashtable();
        for (int k = 0; k < 30; k++) {
            String thiskey;
            if (extended) {
                thiskey = xkeyMaker(i, j, k);
            } else {
                thiskey = keyMaker(i, j, k);
            }
            String thisvalue = ValueMaker(j, k, i);
            //record[thiskey] = thisvalue;
            record.put(thiskey, thisvalue);
            //bpt[thiskey] = thisvalue;
            bpt.set(thiskey, thisvalue);
        }
        if ((j % 3) == 1) {
            bpt.recover(false);
        }
        if (((i + j) % 2) == 1) {
            bpt.commit();
            bpt.abort(); // should have no effect
            bpt.commit(); // ditto
            if ((i + j) % 5 < 2) {
                //System.out.println(bpt.toHtml());
                //foreach (DictionaryEntry d in record)
                for (Enumeration e = record.keys(); e.hasMoreElements();) {
                    String thiskey = (String) e.nextElement();
                    bpt.removeKey(thiskey);
                    if (allmaps.containsKey(thiskey)) {
                        allmaps.remove(thiskey);
                    }
                }
                //System.out.println(bpt.toHtml());
                bpt.commit();
                //return;
            } else {
                //foreach (DictionaryEntry d in record)
                for (Enumeration e = record.keys(); e.hasMoreElements();) {
                    //allmaps[d.Key] = d.Value;
                    String key = (String) e.nextElement();
                    String value = (String) record.get(key);
                    allmaps.put(key, value);
                }
            }
        } else {
            bpt.abort();
        }
    }

    /**
     * @param allmaps
     * @param bpt
     * @param ordered
     * @throws Exception
     */
    public static void checkStructure(Hashtable allmaps, IStringTree bpt,
            boolean ordered) throws Exception {
        TreeSet allkeys = new TreeSet();
        //ArrayList allkeys = new ArrayList();
        //foreach (DictionaryEntry d in allmaps)
        for (Enumeration e = allmaps.keys(); e.hasMoreElements();) {
            String thiskey = (String) e.nextElement();
            String thisvalue = (String) allmaps.get(thiskey);
            String treemap = bpt.get(thiskey);
            if (!treemap.equals(thisvalue)) {
                throw new Exception("key " + thiskey + " maps to " + treemap
                        + " but should map to " + thisvalue);
            }
            allkeys.add(thiskey);
        }
        String currentkey = bpt.firstKey();
        //allkeys.Sort();
        if (ordered) {
            for (Iterator e = allkeys.iterator(); e.hasNext();) {
                Object thing = e.next();
                String recordedkey = (String) thing;
                if (currentkey == null) {
                    throw new Exception("end of keys found when expecting "
                            + recordedkey);
                }
                if (!currentkey.equals(recordedkey)) {
                    //System.out.println(bpt.toHtml());
                    throw new Exception("key " + currentkey
                            + " found where expecting " + recordedkey);
                }
                currentkey = bpt.nextKey(currentkey);
            }
            if (currentkey != null) {
                throw new Exception("found " + currentkey
                        + " when expecting end of keys");
            }
        }
        // should add test for unordered case too...
    }

    /**
     * @param bpt
     * @throws Exception
     */
    public static void abort(BplusTreeLong bpt) throws Exception {
        System.out.println(" <h3>ABORT!</H3>");
        bpt.abort();
        allinserts = (Hashtable) lastcommittedinserts.clone();
        checkit(bpt);
    }

    /**
     * @param bpt
     * @throws Exception
     */
    public static void commit(BplusTreeLong bpt) throws Exception {
        System.out.println(" <h3>COMMIT!</H3>");
        bpt.commit();
        lastcommittedinserts = (Hashtable) allinserts.clone();
        checkit(bpt);
    }

    /**
     * @param bpt
     * @return
     * @throws Exception
     */
    public static BplusTreeLong restart(BplusTreeLong bpt) throws Exception {
        System.out.println(" <h3>RESTART!</H3>");
        commit(bpt);
        return BplusTreeLong.setupFromExistingStream(bpt.fromFile,
                bpt.seekStart);
    }

    /**
     * @param bpt
     * @param key
     * @param map
     * @throws Exception
     */
    public static void inserttest(BplusTreeLong bpt, String key, long map)
            throws Exception {
        inserttest(bpt, key, map, false);
    }

    /**
     * @param bpt
     * @param key
     * @param map
     * @throws Exception
     */
    public static void deletetest(BplusTreeLong bpt, String key, long map)
            throws Exception {
        inserttest(bpt, key, map, true);
    }

    /**
     * @param bpt
     * @param key
     * @param map
     * @param del
     * @throws Exception
     */
    public static void inserttest(BplusTreeLong bpt, String key, long map,
            boolean del) throws Exception {
        if (del) {
            System.out.println(" <h3>DELETE bpt[" + key + "] = " + map
                    + "</h3>");
            bpt.removeKey(key);
            allinserts.remove(key);
        } else {
            System.out.println("<h3>bpt[" + key + "] = " + map + "</h3>");
            //bpt[key] = map;
            bpt.set(key, map);
            //allinserts[key] = map;
            allinserts.put(key, new Long(map));
        }
        checkit(bpt);
    }

    /**
     * @param bpt
     * @throws Exception
     */
    public static void checkit(BplusTreeLong bpt) throws Exception {
        bpt.sanityCheck(true);
        TreeSet allkeys = new TreeSet();
        for (Enumeration e = allinserts.keys(); e.hasMoreElements();) {
            allkeys.add(e.nextElement());
        }
        for (Iterator e = allkeys.iterator(); e.hasNext();) {
            Object thing = e.next();
            String thekey = (String) thing;
            long thevalue = ((Long) allinserts.get(thing)).longValue();
            if (thevalue != bpt.get(thekey)) {
                throw new Exception("no match on retrieval " + thekey + " --> "
                        + bpt.get(thekey) + " not " + thevalue);
            }
        }
        String currentkey = bpt.firstKey();
        for (Iterator e = allkeys.iterator(); e.hasNext();) {
            Object thing = e.next();
            String testkey = (String) thing;
            if (currentkey == null) {
                throw new Exception("end of keys found when expecting "
                        + testkey);
            }
            if (!testkey.equals(currentkey)) {
                throw new Exception("when walking found " + currentkey
                        + " when expecting " + testkey);
            }
            currentkey = bpt.nextKey(testkey);
        }
    }

    /**
     * @throws Exception
     */
    public static void BplusTreeLongTest() throws Exception {
        System.out.println("TESTING BPLUSTREELONG"); // -- LOTS OF OUTPUT to
        for (int nodesize = 2; nodesize < 6; nodesize++) {
            allinserts = new Hashtable();
            java.io.RandomAccessFile mstream = makeFile("jbpl.dat");
            int keylength = 10 + nodesize;
            BplusTreeLong bpt = BplusTreeLong.initializeInStream(mstream,
                    keylength, nodesize);
            bpt = restart(bpt);
            inserttest(bpt, "d", 15);
            deletetest(bpt, "d", 15);
            inserttest(bpt, "d", 15);
            bpt.serializationCheck();
            inserttest(bpt, "ab", 55);
            inserttest(bpt, "b", -5);
            deletetest(bpt, "b", 0);
            inserttest(bpt, "b", -5);
            inserttest(bpt, "c", 34);
            inserttest(bpt, "a", 8);
            commit(bpt);
            System.out.println("<h1>after commit</h1>\r\n");
            inserttest(bpt, "a", 800);
            inserttest(bpt, "ca", -999);
            inserttest(bpt, "da", -999);
            inserttest(bpt, "ea", -9991);
            inserttest(bpt, "aa", -9992);
            inserttest(bpt, "ba", -9995);
            commit(bpt);
            inserttest(bpt, "za", -9997);
            inserttest(bpt, " a", -9999);
            commit(bpt);
            deletetest(bpt, "d", 0);
            deletetest(bpt, "da", 0);
            deletetest(bpt, "ca", 0);
            bpt = restart(bpt);
            inserttest(bpt, "aaa", 88);
            System.out.println(" now doing torture test for " + nodesize);
            System.out.println("<h1>now doing torture test for " + nodesize
                    + "</h1>");
            if (full) {
                for (int i = 0; i < 33; i++) {
                    for (int k = 0; k < 10; k++) {
                        int m = (i * 5 + k * 23) % 77;
                        String s = "b" + m;
                        inserttest(bpt, s, m);
                        if (i % 2 == 1 || k % 3 == 1) {
                            deletetest(bpt, s, m);
                        }
                    }
                    int j = i % 3;
                    if (j == 0) {
                        abort(bpt);
                    } else if (j == 1) {
                        commit(bpt);
                    } else {
                        bpt = restart(bpt);
                    }
                }
            }
            commit(bpt);
            deletetest(bpt, "za", 0);
            deletetest(bpt, "ea", 0);
            deletetest(bpt, "c", 0);
            deletetest(bpt, "ba", 0);
            deletetest(bpt, "b", 0);
            deletetest(bpt, "ab", 0);
            abort(bpt);
            inserttest(bpt, "dog", 1);
            commit(bpt);
            deletetest(bpt, "dog", 1);
            inserttest(bpt, "pig", 2);
            abort(bpt);
            inserttest(bpt, "cat", 3);
            bpt.recover(true);
            mstream.close();
            mstream = null;
        }
    }

    public static void LinkedFileTest() throws Exception {
        System.out.println("TESTING LINKED FILE");
        java.io.RandomAccessFile mstream = makeFile("jlf.dat");
        // make a bunch of sample data
        int asize = 200;
        int maxsizing = 53;
        int prime = 17;
        int buffersize = 33;
        String seedData = "a wop bop a loo bop";
        byte[][] stuff = new byte[asize][];
        for (int i = 0; i < asize; i++) {
            stuff[i] = makeSampleData(seedData, (i * prime) % maxsizing);
        }
        // store them off
        LinkedFile lf = LinkedFile.InitializeLinkedFileInStream(mstream,
                buffersize, prime);
        lf.checkStructure();
        long[] seeks = new long[asize];
        for (int i = 0; i < asize; i++) {
            seeks[i] = lf.StoreNewChunk(stuff[i], 0, stuff[i].length);
            // allocated it again and delete it off to mix things up...
            long dummy = lf.StoreNewChunk(stuff[i], 0, stuff[i].length);
            lf.ReleaseBuffers(dummy);
            lf.checkStructure();
        }
        // delete the last one
        lf.ReleaseBuffers(seeks[asize - 1]);
        lf.checkStructure();
        lf.Flush();
        // create new handle
        lf = LinkedFile.SetupFromExistingStream(mstream, prime);
        // read them back and check (except for last)
        for (int i = 0; i < asize - 1; i++) {
            byte[] retrieved = lf.GetChunk(seeks[i]);
            testByteArrays(retrieved, stuff[i]);
            // delete every so often
            if (i % prime == 1) {
                lf.ReleaseBuffers(seeks[i]);
                lf.checkStructure();
            }
        }
        lf.checkStructure();
        System.out.println("");
        System.out.println("linked file tests ok");
    }

    /**
     * @param testdata
     * @param sizing
     * @return
     * @throws Exception
     */
    public static byte[] makeSampleData(String testdata, int sizing)
            throws Exception {
        if (testdata.length() < 1 || sizing < 1) {
            return new byte[0];
        }
        java.io.ByteArrayOutputStream sb = new java.io.ByteArrayOutputStream();
        byte[] bytedata = testdata.getBytes();
        for (int i = 0; i < sizing; i++) {
            sb.write(bytedata);
            int index = i % bytedata.length;
            sb.write(bytedata, index, 1);
        }
        return sb.toByteArray();
    }

    /**
     * @throws Exception
     */
    public static void testBufferFile() throws Exception {
        System.out.println("TESTING BUFFERFILE");
        int buffersize = 17;
        int writesize = 10;
        //System.IO.Stream mstream = new System.IO.MemoryStream();
        java.io.RandomAccessFile mstream = makeFile("jbf.dat");
        int offset = 55;
        BufferFile bf = BufferFile.InitializeBufferFileInStream(mstream,
                buffersize, offset);
        byte[] testheader = bf.makeHeader();
        byte[] inputarray = makeSampleData(
                "THIS IS SOME sample data off the cuff...", 100);
        byte[] outputarray = new byte[inputarray.length];
        int position = 0;
        // shove in the testdata in reverse order
        for (int i = inputarray.length; i > writesize; i -= writesize) {
            System.out.println(" " + position);
            bf.setBuffer(position, inputarray, i - writesize, writesize);
            position++;
        }
        bf.setBuffer(position, inputarray, 0, writesize);
        // extract it again
        bf = BufferFile.SetupFromExistingStream(mstream, offset);
        position = 0;
        System.out.println("");
        for (int i = inputarray.length; i > writesize; i -= writesize) {
            System.out.println(" " + position);
            //Console.Write(" "+position);
            bf.getBuffer(position, outputarray, i - writesize, writesize);
            position++;
        }
        bf.getBuffer(position, outputarray, 0, writesize);
        testByteArrays(inputarray, outputarray);
        System.out.println("");
        System.out.println(" buffer file test ok");
    }

    /**
     * @param a
     * @param b
     * @throws Exception
     */
    public static void testByteArrays(byte[] a, byte[] b) throws Exception {
        if (a.length != b.length) {
            throw new Exception("array lengths don't match " + a.length + " "
                    + b.length);
        }
        for (int i = 0; i < b.length; i++) {
            if (a[i] != b[i]) {
                throw new Exception("first error at " + i + " " + a[i] + " "
                        + b[i]);
            }
        }
    }

    /**
     * @throws Exception
     */
    public static void intTests() throws Exception {
        int bsize = 13;
        byte[] buffer = new byte[bsize];
        int[] ints = { 1, 566, -55, 32888, 4201010, 87878, -8989898 };
        int index = 99;
        for (int i = 0; i < ints.length; i++) {
            int theInt = ints[i];
            index = Math.abs(index) % (bsize - 4);
            BufferFile.Store(theInt, buffer, index);
            int otherInt = BufferFile.Retrieve(buffer, index);
            if (theInt != otherInt) {
                throw new Exception("encode/decode int failed " + theInt + "!="
                        + otherInt);
            }
            index = (index + theInt);
        }
        System.out.println("encode/decode of ints ok");
    }

    /**
     * @throws Exception
     */
    public static void shortTests() throws Exception {
        int bsize = 13;
        byte[] buffer = new byte[bsize];
        short[] shorts = { 1, 566, -32766, 32, 32755, 80, -8989 };
        int index = 99;
        for (int i = 0; i < shorts.length; i++) {
            short theInt = shorts[i];
            index = Math.abs(index) % (bsize - 4);
            BufferFile.Store(theInt, buffer, index);
            short otherInt = BufferFile.RetrieveShort(buffer, index);
            if (theInt != otherInt) {
                throw new Exception("encode/decode int failed " + theInt + "!="
                        + otherInt);
            }
            index = (index + theInt);
        }
        System.out.println("encode/decode of longs ok");
    }

    /**
     * @throws Exception
     */
    public static void longTests() throws Exception {
        int bsize = 17;
        byte[] buffer = new byte[bsize];
        long[] longs = { 1, 566, -55, 32888, 4201010, 87878, -8989898,
                0xefaefabbccddeeL, -0xefaefabbccddeeL };
        int index = 99;
        for (int i = 0; i < longs.length; i++) {
            long theLong = longs[i];
            index = Math.abs(index) % (bsize - 8);
            BufferFile.Store(theLong, buffer, index);
            long otherLong = BufferFile.RetrieveLong(buffer, index);
            if (theLong != otherLong) {
                throw new Exception("encode/decode int failed " + theLong
                        + "!=" + otherLong);
            }
            index = (index + ((int) (theLong & 0xffffff)));
        }
        System.out.println("encode/decode of longs ok");
    }
}
