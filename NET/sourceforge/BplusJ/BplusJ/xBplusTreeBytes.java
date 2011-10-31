package NET.sourceforge.BplusJ.BplusJ;
import java.util.*;

	/// <summary>
	/// Bplustree with unlimited length Strings (but only a fixed prefix is indexed in the tree directly).
	/// </summary>
public class xBplusTreeBytes implements IByteTree
{
	public BplusTreeBytes tree;
	public int prefixLength;
	public int BucketSizeLimit = -1;
	public xBplusTreeBytes(BplusTreeBytes tree, int prefixLength) throws Exception
	{
		if (prefixLength<3) 
		{
			throw new BplusTreeException("prefix cannot be smaller than 3 :: "+prefixLength); 
		}
		if (prefixLength>tree.MaxKeyLength()) 
		{
			throw new BplusTreeException("prefix length cannot exceed keylength for internal tree");
		}
		this.tree = tree;
		this.prefixLength = prefixLength;
	}
	public void LimitBucketSize(int limit)  throws Exception
	{
		this.BucketSizeLimit = limit;
	}
	public static xBplusTreeBytes Initialize(String treefileName, String blockfileName, int PrefixLength, int CultureId,
		int nodesize, int buffersize)  throws Exception
	{
		return new xBplusTreeBytes(
			BplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId, nodesize, buffersize),
			PrefixLength);
	}
	public static xBplusTreeBytes Initialize(String treefileName, String blockfileName, int PrefixLength, int CultureId) 
		throws Exception
	{
		return new xBplusTreeBytes(
			BplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId),
			PrefixLength);
	}
	public static xBplusTreeBytes Initialize(String treefileName, String blockfileName, int PrefixLength) 
		throws Exception
	{
		return new xBplusTreeBytes(
			BplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength),
			PrefixLength);
	}
	public static xBplusTreeBytes Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId,
		int nodesize, int buffersize)  throws Exception
	{
		return new xBplusTreeBytes(
			BplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId, nodesize, buffersize),
			PrefixLength);
	}
	public static xBplusTreeBytes Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId) 
		throws Exception
	{
		return new xBplusTreeBytes(
			BplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId),
			PrefixLength);
	}
	public static xBplusTreeBytes Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength) 
		throws Exception
	{
		return new xBplusTreeBytes(
			BplusTreeBytes.Initialize(treefile, blockfile, PrefixLength),
			PrefixLength);
	}

	public static xBplusTreeBytes ReOpen(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile) 
		throws Exception
	{
		BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefile, blockfile);
		int prefixLength = tree.MaxKeyLength();
		return new xBplusTreeBytes(tree, prefixLength);
	}
	public static xBplusTreeBytes ReOpen(String treefileName, String blockfileName) 
		throws Exception
	{
		BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefileName, blockfileName);
		int prefixLength = tree.MaxKeyLength();
		return new xBplusTreeBytes(tree, prefixLength);
	}
	public static xBplusTreeBytes ReadOnly(String treefileName, String blockfileName) 
		throws Exception
	{
		BplusTreeBytes tree = BplusTreeBytes.ReadOnly(treefileName, blockfileName);
		int prefixLength = tree.MaxKeyLength();
		return new xBplusTreeBytes(tree, prefixLength);
	}

	public String PrefixForByteCount(String s, int maxbytecount) 
		throws Exception
	{
		if (s.length()<1) 
		{
			return "";
		}
		int prefixcharcount = maxbytecount;
		if (prefixcharcount>s.length()) 
		{
			prefixcharcount = s.length();
		}
		String result = s.substring(0, prefixcharcount);
		while (result.getBytes("UTF-8").length>maxbytecount) 
		{
			prefixcharcount--;
			result = s.substring(0, prefixcharcount);
		}
		return result;
//		if (prefixcharcount>s.length) 
//		{
//			prefixcharcount = s.length;
//		}
//		System.Text.Encoder encode = System.Text.Encoding.UTF8.GetEncoder();
//		char[] chars = s.ToCharArray(0, prefixcharcount);
//		long length = encode.GetByteCount(chars, 0, prefixcharcount, true);
//		while (length>maxbytecount) 
//		{
//			prefixcharcount--;
//			length = encode.GetByteCount(chars, 0, prefixcharcount, true);
//		}
//		return s.substring(0, prefixcharcount);
	}
	public xBucket FindBucketForPrefix(String key, boolean keyIsPrefix)  throws Exception
	{
		xBucket bucket = null;
		String prefix = key;
		if (!keyIsPrefix) 
		{
			prefix = PrefixForByteCount(key, this.prefixLength);
		}
		//System.out.println("prefix="+prefix);
		Object datathing = this.tree.Get(prefix, null);
		if (datathing != null) 
		{
			byte[] databytes = (byte[]) datathing;
			bucket = new xBucket(this);
			bucket.LastPrefix = prefix;
			bucket.Load(databytes);
			if (bucket.size()<1) 
			{
				throw new BplusTreeException("empty bucket loaded");
			}
			return bucket;
		}
		return null; // default
	}

		
	//#region ITreeIndex Members

		
	public int Compare(String left, String right) throws Exception
	{
		return this.tree.Compare(left, right);
	}

	public void Recover(boolean CorrectErrors) throws Exception
	{
		this.tree.Recover(CorrectErrors);
	}

	public void RemoveKey(String key) throws Exception
	{
		xBucket bucket;
		String prefix;
		bucket = FindBucketForPrefix(key, false); //, out bucket, out prefix, false);
		prefix = bucket.LastPrefix;
		if (bucket==null) 
		{
			throw new BplusTreeKeyMissing("no such key to delete");
		}
		bucket.Remove(key);
		if (bucket.size()<1) 
		{
			this.tree.RemoveKey(prefix);
		} 
		else 
		{
			//this.tree[prefix] = bucket.dump();
			this.tree.set(prefix, bucket.dump());
		}
	}

	public String FirstKey() throws Exception
	{
		xBucket bucket;
		String prefix = this.tree.FirstKey();
		if (prefix==null) 
		{
			return null;
		}
		//String dummyprefix;
		bucket = FindBucketForPrefix(prefix, true); //out bucket, out dummyprefix, true);
		if (bucket==null) 
		{
			throw new BplusTreeException("internal tree gave bad first key");
		}
		return bucket.FirstKey();
	}

	public String NextKey(String AfterThisKey) throws Exception
	{
		xBucket bucket;
		//String prefix;
		String result = null;
		bucket = FindBucketForPrefix(AfterThisKey, false);//, out bucket, out prefix, false);
		if (bucket!=null) 
		{
			result = bucket.NextKey(AfterThisKey);
			if (result!=null) 
			{
				return result;
			}
		}
		// otherwise look in the next bucket
		String prefix = PrefixForByteCount(AfterThisKey, this.prefixLength);
		String nextprefix = this.tree.NextKey(prefix);
		if (nextprefix==null) 
		{
			return null;
		}
		byte[] databytes = this.tree.get(nextprefix);
		bucket = new xBucket(this);
		bucket.Load(databytes);
		if (bucket.size()<1) 
		{
			throw new BplusTreeException("empty bucket loaded");
		}
		return bucket.FirstKey();
	}

	public boolean ContainsKey(String key) throws Exception
	{
		xBucket bucket;
		String prefix;
		bucket = FindBucketForPrefix(key, false); //, out bucket, out prefix, false);
		if (bucket==null) 
		{
			return false;
		}
		byte[] map;
		map = bucket.Find(key);
		return map!=null;
	}

	public Object Get(String key, Object defaultValue) throws Exception
	{
		xBucket bucket;
		String prefix;
		bucket = FindBucketForPrefix(key, false); //, out bucket, out prefix, false);
		if (bucket==null) 
		{
			return defaultValue;
		}
		byte[] map;
		map = bucket.Find(key);
		if (map!=null) 
		{
			return map;
		}
		return defaultValue;
	}

	public void Set(String key, Object map) throws Exception
	{
			
		xBucket bucket;
		String prefix;
		bucket = FindBucketForPrefix(key, false); //out bucket, out prefix, false);
		//prefix = bucket.LastPrefix;
		//System.out.println("prefix="+prefix);
		if (bucket==null) 
		{
			bucket = new xBucket(this);
			prefix = PrefixForByteCount(key, this.prefixLength);
		} 
		else 
		{
			prefix = bucket.LastPrefix;
		}
		//		if (!(map is byte[])) 
		//		{
		//			throw new BplusTreeBadKeyValue("xBplus only accepts byte array values");
		//		}
		bucket.add(key, (byte[]) map);
		//this.tree[prefix] = bucket.dump();
		this.tree.set(prefix, bucket.dump());
	}
	public byte[] get (String key)  throws Exception
	{
		Object test = this.Get(key, null);
		if (test!=null) 
		{
			return (byte[]) test;
		}
		throw new BplusTreeKeyMissing("no such key in tree");
	}
	public void set (String key, byte[] value) throws Exception
	{
		this.Set(key, value);
	}

	public void Commit() throws Exception
	{
		this.tree.Commit();
	}

	public void Abort() throws Exception
	{
		this.tree.Abort();
	}

	public void SetFootPrintLimit(int limit) throws Exception
	{
		this.tree.SetFootPrintLimit(limit);
	}

	public void Shutdown() throws Exception
	{
		this.tree.Shutdown();
	}

	//#endregion

	/// <summary>
	/// Bucket for elements with same prefix -- designed for small buckets.
	/// </summary>
	public class xBucket 
	{
		ArrayList keys;
		ArrayList values;
		xBplusTreeBytes owner;
		public String LastPrefix=null;
		public xBucket(xBplusTreeBytes owner)  throws Exception
		{
			this.keys = new ArrayList();
			this.values = new ArrayList();
			this.owner = owner;
		}
		public int size()  throws Exception
		{
			return this.keys.size();
		}
		public void Load(byte[] serialization)  throws Exception
		{
			int index = 0;
			int byteCount = serialization.length;
			if (this.values.size()!=0 || this.keys.size()!=0) 
			{
				throw new BplusTreeException("load into nonempty xBucket not permitted");
			}
			while (index<byteCount) 
			{
				// get key prefix and key
				int keylength = BufferFile.Retrieve(serialization, index);
				index += BufferFile.INTSTORAGE;
				byte[] keybytes = new byte[keylength];
				//Array.Copy(serialization, index, keybytes, 0, keylength);
				for (int i=0; i<keylength; i++) 
				{
					keybytes[i] = serialization[index+i];
				}
				String keyString = BplusTree.BytesToString(keybytes);
				index+= keylength;
				// get value prefix and value
				int valuelength = BufferFile.Retrieve(serialization, index);
				index += BufferFile.INTSTORAGE;
				byte[] valuebytes = new byte[valuelength];
				//Array.Copy(serialization, index, valuebytes, 0, valuelength);
				for (int i=0; i<valuelength; i++) 
				{
					valuebytes[i] = serialization[index+i];
				}
				// record new key and value
				this.keys.add(keyString);
				this.values.add(valuebytes);
				index+= valuelength;
			}
			if (index!=byteCount) 
			{
				throw new BplusTreeException("bad byte count in serialization "+byteCount);
			}
		}
		public byte[] dump()  throws Exception
		{
			ArrayList allbytes = new ArrayList();
			int byteCount = 0;
			for (int index=0; index<this.keys.size(); index++) 
			{
				String thisKey = (String) this.keys.get(index);
				byte[] thisValue = (byte[]) this.values.get(index);
				byte[] keyprefix = new byte[BufferFile.INTSTORAGE];
				byte[] keybytes = BplusTree.StringToBytes(thisKey);
				BufferFile.Store(keybytes.length, keyprefix, 0);
				allbytes.add(keyprefix);
				allbytes.add(keybytes);
				byte[] valueprefix = new byte[BufferFile.INTSTORAGE];
				BufferFile.Store(thisValue.length, valueprefix, 0);
				allbytes.add(valueprefix);
				allbytes.add(thisValue);
			}
			for (int i=0; i<allbytes.size(); i++)
			{
				Object thing = allbytes.get(i);
				byte[] thebytes = (byte[]) thing;
				byteCount+= thebytes.length;
			}
			int outindex=0;
			byte[] result = new byte[byteCount];
			//foreach (Object thing in allbytes) 
			for (int i=0; i<allbytes.size(); i++)
			{
				Object thing = allbytes.get(i);
				byte[] thebytes = (byte[]) thing;
				int thelength = thebytes.length;
				//Array.Copy(thebytes, 0, result, outindex, thelength);
				for (int ii=0; ii<thelength; ii++) 
				{
					result[outindex+ii] = thebytes[ii];
				}
				outindex+= thelength;
			}
			if (outindex!=byteCount) 
			{
				throw new BplusTreeException("error counting bytes in dump "+outindex+"!="+byteCount);
			}
			return result;
		}
		public void add(String key, byte[] map) throws Exception
		{
			int index = 0;
			int limit = this.owner.BucketSizeLimit;
			while (index<this.keys.size()) 
			{
				String thiskey = (String) this.keys.get(index);
				int comparison = this.owner.Compare(thiskey, key);
				if (comparison==0) 
				{
					//this.values[index] = map;
					//this.keys[index] = key;
					this.values.set(index, map);
					this.keys.set(index, key);
					return;
				}
				if (comparison>0) 
				{
					this.values.add(index, map);
					this.keys.add(index, key);
					if (limit>0 && this.keys.size()>limit) 
					{
						throw new BplusTreeBadKeyValue("bucket size limit exceeded");
					}
					return;
				}
				index++;
			}
			this.keys.add(key);
			this.values.add(map);
			if (limit>0 && this.keys.size()>limit) 
			{
				throw new BplusTreeBadKeyValue("bucket size limit exceeded");
			}
		}
		public void Remove(String key)  throws Exception
		{
			int index = 0;
			while (index<this.keys.size()) 
			{
				String thiskey = (String) this.keys.get(index);
				if (this.owner.Compare(thiskey, key)==0) 
				{
					this.values.remove(index);
					this.keys.remove(index);
					return;
				}
				index++;
			}
			throw new BplusTreeBadKeyValue("cannot remove missing key: "+key);
		}
		public byte[] Find(String key)  throws Exception
		{
			byte[] map = null;
			int index = 0;
			while (index<this.keys.size()) 
			{
				String thiskey = (String) this.keys.get(index);
				if (this.owner.Compare(thiskey, key)==0) 
				{
					map = (byte[]) this.values.get(index);
					return map;
				}
				index++;
			}
			return null;
		}
		public String FirstKey()  throws Exception
		{
			if (this.keys.size()<1) 
			{
				return null;
			}
			return (String) this.keys.get(0);
		}
		public String NextKey(String AfterThisKey) throws Exception
		{
			int index = 0;
			while (index<this.keys.size()) 
			{
				String thiskey = (String) this.keys.get(index);
				if (this.owner.Compare(thiskey, AfterThisKey)>0) 
				{
					return thiskey;
				}
				index++;
			}
			return null;
		}
	}
}