package org.bplusj.xbplus;

import java.util.ArrayList;

import org.bplusj.BplusTree;
import org.bplusj.BplusTreeBadKeyValue;
import org.bplusj.BplusTreeException;
import org.bplusj.BplusTreeKeyMissing;
import org.bplusj.BufferFile;
import org.bplusj.bytetree.BplusTreeBytes;
import org.bplusj.bytetree.IByteTree;

/**
 * Bplustree with unlimited length Strings (but only a fixed prefix is indexed
 * in the tree directly).
 * 
 * @author Aaron Watters
 */
public class XBplusTreeBytes implements IByteTree {
    
    //ATTRIBUTES
    public BplusTreeBytes tree;

    private int prefixLength;

    public int bucketSizeLimit = -1;

    //CONSTRUCTORS
    /**
     * @param tree
     * @param prefixLength
     * @throws Exception
     */
    public XBplusTreeBytes(BplusTreeBytes tree, int prefixLength)
            throws Exception {
        if (prefixLength < 3) {
            throw new BplusTreeException("prefix cannot be smaller than 3 :: "
                    + prefixLength);
        }
        if (prefixLength > tree.MaxKeyLength()) {
            throw new BplusTreeException(
                    "prefix length cannot exceed keylength for internal tree");
        }
        this.tree = tree;
        this.prefixLength = prefixLength;
    }

    //PUBLIC METHODS
    /**
     * @param limit
     * @throws Exception
     */
    public void limitBucketSize(int limit) throws Exception {
        this.bucketSizeLimit = limit;
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @param PrefixLength
     * @param CultureId
     * @param nodesize
     * @param buffersize
     * @return
     * @throws Exception
     */
    public static XBplusTreeBytes initialize(String treefileName,
            String blockfileName, int PrefixLength, int CultureId,
            int nodesize, int buffersize) throws Exception {
        return new XBplusTreeBytes(BplusTreeBytes.initialize(treefileName,
                blockfileName, PrefixLength, CultureId, nodesize, buffersize),
                PrefixLength);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @param PrefixLength
     * @param CultureId
     * @return
     * @throws Exception
     */
    public static XBplusTreeBytes initialize(String treefileName,
            String blockfileName, int PrefixLength, int CultureId)
            throws Exception {
        return new XBplusTreeBytes(BplusTreeBytes.initialize(treefileName,
                blockfileName, PrefixLength, CultureId), PrefixLength);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @param PrefixLength
     * @return
     * @throws Exception
     */
    public static XBplusTreeBytes initialize(String treefileName,
            String blockfileName, int PrefixLength) throws Exception {
        return new XBplusTreeBytes(BplusTreeBytes.initialize(treefileName,
                blockfileName, PrefixLength), PrefixLength);
    }

    /**
     * @param treefile
     * @param blockfile
     * @param PrefixLength
     * @param CultureId
     * @param nodesize
     * @param buffersize
     * @return
     * @throws Exception
     */
    public static XBplusTreeBytes initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int PrefixLength,
            int CultureId, int nodesize, int buffersize) throws Exception {
        return new XBplusTreeBytes(BplusTreeBytes.initialize(treefile,
                blockfile, PrefixLength, CultureId, nodesize, buffersize),
                PrefixLength);
    }

    /**
     * @param treefile
     * @param blockfile
     * @param PrefixLength
     * @param CultureId
     * @return
     * @throws Exception
     */
    public static XBplusTreeBytes initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId)
            throws Exception {
        return new XBplusTreeBytes(BplusTreeBytes.initialize(treefile,
                blockfile, PrefixLength, CultureId), PrefixLength);
    }

    /**
     * @param treefile
     * @param blockfile
     * @param PrefixLength
     * @return
     * @throws Exception
     */
    public static XBplusTreeBytes initialize(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile, int PrefixLength)
            throws Exception {
        return new XBplusTreeBytes(BplusTreeBytes.initialize(treefile,
                blockfile, PrefixLength), PrefixLength);
    }

    /**
     * @param treefile
     * @param blockfile
     * @return
     * @throws Exception
     */
    public static XBplusTreeBytes reOpen(java.io.RandomAccessFile treefile,
            java.io.RandomAccessFile blockfile) throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.reOpen(treefile, blockfile);
        int prefixLength = tree.MaxKeyLength();
        return new XBplusTreeBytes(tree, prefixLength);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @return
     * @throws Exception
     */
    public static XBplusTreeBytes reOpen(String treefileName,
            String blockfileName) throws Exception {
        BplusTreeBytes tree = BplusTreeBytes
                .reOpen(treefileName, blockfileName);
        int prefixLength = tree.MaxKeyLength();
        return new XBplusTreeBytes(tree, prefixLength);
    }

    /**
     * @param treefileName
     * @param blockfileName
     * @return
     * @throws Exception
     */
    public static XBplusTreeBytes readOnly(String treefileName,
            String blockfileName) throws Exception {
        BplusTreeBytes tree = BplusTreeBytes.readOnly(treefileName,
                blockfileName);
        int prefixLength = tree.MaxKeyLength();
        return new XBplusTreeBytes(tree, prefixLength);
    }

    /**
     * @param s
     * @param maxbytecount
     * @return
     * @throws Exception
     */
    public String prefixForByteCount(String s, int maxbytecount)
            throws Exception {
        if (s.length() < 1) {
            return "";
        }
        int prefixcharcount = maxbytecount;
        if (prefixcharcount > s.length()) {
            prefixcharcount = s.length();
        }
        String result = s.substring(0, prefixcharcount);
        while (result.getBytes("UTF-8").length > maxbytecount) {
            prefixcharcount--;
            result = s.substring(0, prefixcharcount);
        }
        return result;
    }

    /**
     * @param key
     * @param keyIsPrefix
     * @return
     * @throws Exception
     */
    public xBucket findBucketForPrefix(String key, boolean keyIsPrefix)
            throws Exception {
        xBucket bucket = null;
        String prefix = key;
        if (!keyIsPrefix) {
            prefix = prefixForByteCount(key, this.prefixLength);
        }
        Object datathing = this.tree.get(prefix, null);
        if (datathing != null) {
            byte[] databytes = (byte[]) datathing;
            bucket = new xBucket(this);
            bucket.LastPrefix = prefix;
            bucket.Load(databytes);
            if (bucket.size() < 1) {
                throw new BplusTreeException("empty bucket loaded");
            }
            return bucket;
        }
        return null; // default
    }


    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#compare(java.lang.String, java.lang.String)
     */
    public int compare(String left, String right) throws Exception {
        return this.tree.compare(left, right);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#recover(boolean)
     */
    public void recover(boolean CorrectErrors) throws Exception {
        this.tree.recover(CorrectErrors);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#removeKey(java.lang.String)
     */
    public void removeKey(String key) throws Exception {
        xBucket bucket;
        String prefix;
        bucket = findBucketForPrefix(key, false); //, out bucket, out prefix,
                                                  // false);
        prefix = bucket.LastPrefix;
        if (bucket == null) {
            throw new BplusTreeKeyMissing("no such key to delete");
        }
        bucket.Remove(key);
        if (bucket.size() < 1) {
            this.tree.removeKey(prefix);
        } else {
            //this.tree[prefix] = bucket.dump();
            this.tree.set(prefix, bucket.dump());
        }
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#firstKey()
     */
    public String firstKey() throws Exception {
        xBucket bucket;
        String prefix = this.tree.firstKey();
        if (prefix == null) {
            return null;
        }
        bucket = findBucketForPrefix(prefix, true); //out bucket, out
                                                    // dummyprefix, true);
        if (bucket == null) {
            throw new BplusTreeException("internal tree gave bad first key");
        }
        return bucket.FirstKey();
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#nextKey(java.lang.String)
     */
    public String nextKey(String AfterThisKey) throws Exception {
        xBucket bucket;
        //String prefix;
        String result = null;
        bucket = findBucketForPrefix(AfterThisKey, false);//, out bucket, out
                                                          // prefix, false);
        if (bucket != null) {
            result = bucket.NextKey(AfterThisKey);
            if (result != null) {
                return result;
            }
        }
        // otherwise look in the next bucket
        String prefix = prefixForByteCount(AfterThisKey, this.prefixLength);
        String nextprefix = this.tree.nextKey(prefix);
        if (nextprefix == null) {
            return null;
        }
        byte[] databytes = this.tree.get(nextprefix);
        bucket = new xBucket(this);
        bucket.Load(databytes);
        if (bucket.size() < 1) {
            throw new BplusTreeException("empty bucket loaded");
        }
        return bucket.FirstKey();
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#containsKey(java.lang.String)
     */
    public boolean containsKey(String key) throws Exception {
        xBucket bucket;
        String prefix;
        bucket = findBucketForPrefix(key, false); //, out bucket, out prefix,
                                                  // false);
        if (bucket == null) {
            return false;
        }
        byte[] map;
        map = bucket.Find(key);
        return map != null;
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#get(java.lang.String, java.lang.Object)
     */
    public Object get(String key, Object defaultValue) throws Exception {
        xBucket bucket;
        String prefix;
        bucket = findBucketForPrefix(key, false); //, out bucket, out prefix,
                                                  // false);
        if (bucket == null) {
            return defaultValue;
        }
        byte[] map;
        map = bucket.Find(key);
        if (map != null) {
            return map;
        }
        return defaultValue;
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#set(java.lang.String, java.lang.Object)
     */
    public void set(String key, Object map) throws Exception {

        xBucket bucket;
        String prefix;
        bucket = findBucketForPrefix(key, false); //out bucket, out prefix,
                                                  // false);
        //prefix = bucket.LastPrefix;
        //System.out.println("prefix="+prefix);
        if (bucket == null) {
            bucket = new xBucket(this);
            prefix = prefixForByteCount(key, this.prefixLength);
        } else {
            prefix = bucket.LastPrefix;
        }
        bucket.add(key, (byte[]) map);
        this.tree.set(prefix, bucket.dump());
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.IByteTree#get(java.lang.String)
     */
    public byte[] get(String key) throws Exception {
        Object test = this.get(key, null);
        if (test != null) {
            return (byte[]) test;
        }
        throw new BplusTreeKeyMissing("no such key in tree");
    }

    public void set(String key, byte[] value) throws Exception {
        this.set(key, value);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#commit()
     */
    public void commit() throws Exception {
        this.tree.commit();
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#abort()
     */
    public void abort() throws Exception {
        this.tree.abort();
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#setFootPrintLimit(int)
     */
    public void setFootPrintLimit(int limit) throws Exception {
        this.tree.setFootPrintLimit(limit);
    }

    /* (non-Javadoc)
     * @see NET.sourceforge.BplusJ.BplusJ.ITreeIndex#shutDown()
     */
    public void shutDown() throws Exception {
        this.tree.shutDown();
    }

    /**
     * Bucket for elements with same prefix -- designed for small buckets.
     * 
     * @author Aaron Watters
     *  
     */
    public class xBucket {
        ArrayList keys;

        ArrayList values;

        XBplusTreeBytes owner;

        public String LastPrefix = null;

        /**
         * @param owner
         * @throws Exception
         */
        public xBucket(XBplusTreeBytes owner) throws Exception {
            this.keys = new ArrayList();
            this.values = new ArrayList();
            this.owner = owner;
        }

        /**
         * @return
         * @throws Exception
         */
        public int size() throws Exception {
            return this.keys.size();
        }

        /**
         * @param serialization
         * @throws Exception
         */
        public void Load(byte[] serialization) throws Exception {
            int index = 0;
            int byteCount = serialization.length;
            if (this.values.size() != 0 || this.keys.size() != 0) {
                throw new BplusTreeException(
                        "load into nonempty xBucket not permitted");
            }
            while (index < byteCount) {
                // get key prefix and key
                int keylength = BufferFile.Retrieve(serialization, index);
                index += BufferFile.INTSTORAGE;
                byte[] keybytes = new byte[keylength];
                //Array.Copy(serialization, index, keybytes, 0, keylength);
                for (int i = 0; i < keylength; i++) {
                    keybytes[i] = serialization[index + i];
                }
                String keyString = BplusTree.bytesToString(keybytes);
                index += keylength;
                // get value prefix and value
                int valuelength = BufferFile.Retrieve(serialization, index);
                index += BufferFile.INTSTORAGE;
                byte[] valuebytes = new byte[valuelength];
                //Array.Copy(serialization, index, valuebytes, 0, valuelength);
                for (int i = 0; i < valuelength; i++) {
                    valuebytes[i] = serialization[index + i];
                }
                // record new key and value
                this.keys.add(keyString);
                this.values.add(valuebytes);
                index += valuelength;
            }
            if (index != byteCount) {
                throw new BplusTreeException("bad byte count in serialization "
                        + byteCount);
            }
        }

        /**
         * @return
         * @throws Exception
         */
        public byte[] dump() throws Exception {
            ArrayList allbytes = new ArrayList();
            int byteCount = 0;
            for (int index = 0; index < this.keys.size(); index++) {
                String thisKey = (String) this.keys.get(index);
                byte[] thisValue = (byte[]) this.values.get(index);
                byte[] keyprefix = new byte[BufferFile.INTSTORAGE];
                byte[] keybytes = BplusTree.stringToBytes(thisKey);
                BufferFile.Store(keybytes.length, keyprefix, 0);
                allbytes.add(keyprefix);
                allbytes.add(keybytes);
                byte[] valueprefix = new byte[BufferFile.INTSTORAGE];
                BufferFile.Store(thisValue.length, valueprefix, 0);
                allbytes.add(valueprefix);
                allbytes.add(thisValue);
            }
            for (int i = 0; i < allbytes.size(); i++) {
                Object thing = allbytes.get(i);
                byte[] thebytes = (byte[]) thing;
                byteCount += thebytes.length;
            }
            int outindex = 0;
            byte[] result = new byte[byteCount];
            //foreach (Object thing in allbytes)
            for (int i = 0; i < allbytes.size(); i++) {
                Object thing = allbytes.get(i);
                byte[] thebytes = (byte[]) thing;
                int thelength = thebytes.length;
                //Array.Copy(thebytes, 0, result, outindex, thelength);
                for (int ii = 0; ii < thelength; ii++) {
                    result[outindex + ii] = thebytes[ii];
                }
                outindex += thelength;
            }
            if (outindex != byteCount) {
                throw new BplusTreeException("error counting bytes in dump "
                        + outindex + "!=" + byteCount);
            }
            return result;
        }

        /**
         * @param key
         * @param map
         * @throws Exception
         */
        public void add(String key, byte[] map) throws Exception {
            int index = 0;
            int limit = this.owner.bucketSizeLimit;
            while (index < this.keys.size()) {
                String thiskey = (String) this.keys.get(index);
                int comparison = this.owner.compare(thiskey, key);
                if (comparison == 0) {
                    this.values.set(index, map);
                    this.keys.set(index, key);
                    return;
                }
                if (comparison > 0) {
                    this.values.add(index, map);
                    this.keys.add(index, key);
                    if (limit > 0 && this.keys.size() > limit) {
                        throw new BplusTreeBadKeyValue(
                                "bucket size limit exceeded");
                    }
                    return;
                }
                index++;
            }
            this.keys.add(key);
            this.values.add(map);
            if (limit > 0 && this.keys.size() > limit) {
                throw new BplusTreeBadKeyValue("bucket size limit exceeded");
            }
        }

        /**
         * @param key
         * @throws Exception
         */
        public void Remove(String key) throws Exception {
            int index = 0;
            while (index < this.keys.size()) {
                String thiskey = (String) this.keys.get(index);
                if (this.owner.compare(thiskey, key) == 0) {
                    this.values.remove(index);
                    this.keys.remove(index);
                    return;
                }
                index++;
            }
            throw new BplusTreeBadKeyValue("cannot remove missing key: " + key);
        }

        /**
         * @param key
         * @return
         * @throws Exception
         */
        public byte[] Find(String key) throws Exception {
            byte[] map = null;
            int index = 0;
            while (index < this.keys.size()) {
                String thiskey = (String) this.keys.get(index);
                if (this.owner.compare(thiskey, key) == 0) {
                    map = (byte[]) this.values.get(index);
                    return map;
                }
                index++;
            }
            return null;
        }

        /**
         * @return
         * @throws Exception
         */
        public String FirstKey() throws Exception {
            if (this.keys.size() < 1) {
                return null;
            }
            return (String) this.keys.get(0);
        }

        /**
         * @param AfterThisKey
         * @return
         * @throws Exception
         */
        public String NextKey(String AfterThisKey) throws Exception {
            int index = 0;
            while (index < this.keys.size()) {
                String thiskey = (String) this.keys.get(index);
                if (this.owner.compare(thiskey, AfterThisKey) > 0) {
                    return thiskey;
                }
                index++;
            }
            return null;
        }
    }
}