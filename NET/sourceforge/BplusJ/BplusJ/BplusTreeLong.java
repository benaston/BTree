
package NET.sourceforge.BplusJ.BplusJ;
import java.util.*;

	/// <summary>
	/// Bplustree mapping fixed length Strings (byte sequences) to longs (seek positions in file indexed).
	/// "Next leaf pointer" is not used since it increases the chance of file corruption on failure.
	/// All modifications are "shadowed" until a flush of all modifications succeeds.  Modifications are
	/// "hardened" when the header record is rewritten with a new root.  This design trades a few "unneeded"
	/// buffer writes for lower likelihood of file corruption.
	/// </summary>
public class BplusTreeLong implements ITreeIndex
{
	public java.io.RandomAccessFile fromfile;
	// should be read only
	//public boolean DontUseCulture = false;
	//public System.Globalization.CultureInfo cultureContext;
	//System.Globalization.CompareInfo cmp = null;
	// should be read only
	public BufferFile buffers;
	// should be read only
	public int buffersize;
	// should be read only
	public int KeyLength;
	public long seekStart = 0;
	public static byte[] HEADERPREFIX = { 98, 112, 78, 98, 112 };
	// header consists of 
	// prefix | version | node size | key size | culture id | buffer number of root | buffer number of free list head
	int headersize = HEADERPREFIX.length + 1 + BufferFile.INTSTORAGE*3 + BufferFile.LONGSTORAGE*2;
	public static byte VERSION = 0;
	// for java, only allow the invariant culture.
	public static int INVARIANTCULTUREID = 127;
	// size of allocated key space in each node (should be a read only property)
	public int NodeSize;
	BplusNode root = null;
	long rootSeek; 
	long freeHeadSeek;
	public long LastValueFound;
	public Hashtable FreeBuffersOnCommit = new Hashtable();
	public Hashtable FreeBuffersOnAbort = new Hashtable();
	Hashtable IdToTerminalNode = new Hashtable();
	Hashtable TerminalNodeToId = new Hashtable();
	int TerminalNodeCount = 0;
	int LowerTerminalNodeCount = 0;
	int FifoLimit = 100;
	public static int NULLBUFFERNUMBER = -1;
	public static byte NONLEAF = 0, LEAF = 1, FREE = 2;

	public BplusTreeLong(java.io.RandomAccessFile fromfile, int NodeSize, int KeyLength, long StartSeek, int CultureId)
		throws Exception
	{
		//this.cultureContext = new System.Globalization.CultureInfo(CultureId);
		if (CultureId!=INVARIANTCULTUREID) 
		{
			throw new BplusTreeException("BplusJ only supports the invariant culture");
		}
		//this.cmp = this.cultureContext.CompareInfo;
		this.fromfile = fromfile;
		this.NodeSize = NodeSize;
		this.seekStart = StartSeek;
		// add in key prefix overhead
		this.KeyLength = KeyLength + BufferFile.SHORTSTORAGE;
		this.rootSeek = NULLBUFFERNUMBER;
		this.root = null;
		this.freeHeadSeek = NULLBUFFERNUMBER;
		this.SanityCheck();
	}
		
	public int MaxKeyLength() 
	{
		return this.KeyLength-BufferFile.SHORTSTORAGE;
	}
	public void Shutdown()
		throws Exception
	{
		//this.fromfile.Flush();
		this.fromfile.close();
	}
	public int Compare(String left, String right) 
		throws Exception
	{
		//System.Globalization.CompareInfo cmp = this.cultureContext.CompareInfo;
		return left.compareTo(right); // only lexicographic compare allowed for java
		//			if (this.cultureContext==null || this.DontUseCulture) 
		//			{
		//				// no culture context: use miscellaneous total ordering on unicode Strings
		//				int i = 0;
		//				while (i<left.length && i<right.length) 
		//				{
		//					int leftOrd = Convert.ToInt32(left[i]);
		//					int rightOrd = Convert.ToInt32(right[i]);
		//					if (leftOrd<rightOrd) 
		//					{
		//						return -1;
		//					}
		//					if (leftOrd>rightOrd)
		//					{
		//						return 1;
		//					}
		//					i++;
		//				}
		//				if (left.length<right.length) 
		//				{
		//					return -1;
		//				}
		//				if (left.length>right.length) 
		//				{
		//					return 1;
		//				}
		//				return 0;
		//			}
		//			if (this.cmp==null) 
		//			{
		//				this.cmp = this.cultureContext.CompareInfo;
		//			}
		//			return this.cmp.Compare(left, right);
	}
	public void SanityCheck(boolean strong) 
		throws Exception
	{
		this.SanityCheck();
		if (strong) 
		{
			this.Recover(false);
			// look at all deferred deallocations -- they should not be free
			byte[] buffer = new byte[1];
			//foreach (Object thing in this.FreeBuffersOnAbort) 
			//for (int i=0; i<this.FreeBuffersOnAbort.size(); i++)
			for (Enumeration e=this.FreeBuffersOnAbort.keys(); e.hasMoreElements(); )
			{
				//Object thing = this.FreeBuffersOnAbort.get(i);
				Object thing = e.nextElement();
				long buffernumber = ((Long) thing).longValue();
				this.buffers.getBuffer(buffernumber, buffer, 0, 1);
				if (buffer[0]==FREE) 
				{
					throw new BplusTreeException("free on abort buffer already marked free "+buffernumber);
				}
			}
			//foreach (Object thing in this.FreeBuffersOnCommit) 
			//for (int i=0; i<this.FreeBuffersOnCommit.size(); i++)
			for (Enumeration e=this.FreeBuffersOnCommit.keys(); e.hasMoreElements(); )
			{
				//Object thing = this.FreeBuffersOnCommit.get(i);
				Object thing = e.nextElement();
				long buffernumber = ((Long) thing).longValue();
				this.buffers.getBuffer(buffernumber, buffer, 0, 1);
				if (buffer[0]==FREE) 
				{
					throw new BplusTreeException("free on commit buffer already marked free "+buffernumber);
				}
			}
		}
	}
	public void Recover(boolean CorrectErrors) 
		throws Exception
	{
		Hashtable visited = new Hashtable();
		if (this.root!=null) 
		{
			// find all reachable nodes
			this.root.SanityCheck(visited);
		}
		// traverse the free list
		long freebuffernumber = this.freeHeadSeek;
		while (freebuffernumber!=NULLBUFFERNUMBER) 
		{
			if (visited.containsKey(new Long(freebuffernumber)) ) 
			{
				throw new BplusTreeException("free buffer visited twice "+freebuffernumber);
			}
			//visited[freebuffernumber] = FREE;
			visited.put(new Long(freebuffernumber), new Byte(FREE));
			freebuffernumber = this.parseFreeBuffer(freebuffernumber);
		}
		// find out what is missing
		Hashtable Missing = new Hashtable();
		long maxbuffer = this.buffers.nextBufferNumber();
		for (long i=0; i<maxbuffer; i++) 
		{
			if (!visited.containsKey(new Long(i))) 
			{
				//Missing[i] = i;
				Missing.put(new Long(i), new Long(i));
			}
		}
		// remove from missing any free-on-commit blocks
		//foreach (Object thing in this.FreeBuffersOnCommit) 
		//for (int i=0; i<this.FreeBuffersOnCommit.size(); i++)
		for (Enumeration e=this.FreeBuffersOnCommit.keys(); e.hasMoreElements(); )
		{
			//long tobefreed = (long) thing;
			//Missing.Remove(tobefreed);
			Missing.remove( e.nextElement() );
		}
		// add the missing values to the free list
		if (CorrectErrors) 
		{
			//				if (Missing.size()>0) 
			//				{
			//					System.Diagnostics.Debug.WriteLine("correcting "+Missing.Count+" unreachable buffers");
			//				}
			//				ArrayList missingL = new ArrayList();
			//				foreach (DictionaryEntry d in Missing) 
			//				{
			//					missingL.Add(d.Key);
			//				}
			//				missingL.Sort();
			//				missingL.Reverse();
			//				foreach (Object thing in missingL) 
			//				{
			//					long buffernumber = (long) thing;
			//					this.deallocateBuffer(buffernumber);
			//				}
			for (Enumeration e = Missing.keys(); e.hasMoreElements(); ) 
			{
				long buffernumber = ((Long) e.nextElement()).longValue();
				this.deallocateBuffer(buffernumber);
			}
			//this.ResetBookkeeping();
		} 
		else if (Missing.size()>0)
		{
			//				String buffers = "";
			//				foreach (DictionaryEntry thing in Missing) 
			//				{
			//					buffers += " "+thing.Key;
			//				}
			throw new BplusTreeException("found "+Missing.size()+" unreachable buffers.");
		}
	}
	public void SerializationCheck()  
		throws Exception
	{
		if (this.root==null) 
		{
			throw new BplusTreeException("serialization check requires initialized root, sorry");
		}
		this.root.SerializationCheck();
	}
	void SanityCheck() 
		throws Exception
	{
		if (this.NodeSize<2) 
		{
			throw new BplusTreeException("node size must be larger than 2");
		}
		if (this.KeyLength<5) 
		{
			throw new BplusTreeException("Key length must be larger than 5");
		}
		if (this.seekStart<0) 
		{
			throw new BplusTreeException("start seek may not be negative");
		}
		// compute the buffer size
		// indicator | seek position | [ key storage | seek position ]*
		int keystorage = this.KeyLength + BufferFile.SHORTSTORAGE;
		this.buffersize = 1 + BufferFile.LONGSTORAGE + (keystorage + BufferFile.LONGSTORAGE)*this.NodeSize;
	}
	public String toHtml() throws Exception
	{
		java.io.CharArrayWriter sb = new java.io.CharArrayWriter();
		sb.write("<h1>BplusTree</h1>\r\n");
		sb.write("\r\n<br> nodesize="+this.NodeSize);
		sb.write("\r\n<br> seekstart="+this.seekStart);
		sb.write("\r\n<br> rootseek="+this.rootSeek);
		sb.write("\r\n<br> free on commit "+this.FreeBuffersOnCommit.size()+" ::");
		//foreach (Object thing in this.FreeBuffersOnCommit) 
		//for (int i=0; i<this.FreeBuffersOnCommit.size(); i++)
		//{
		//	sb.write(" "+this.FreeBuffersOnCommit.get(i));
		//}
		sb.write("\r\n<br> Freebuffers : ");
		Hashtable freevisit = new Hashtable();
		long free = this.freeHeadSeek;
		String allfree = "freehead="+free+" :: ";
		while (free!=NULLBUFFERNUMBER) 
		{
			allfree = allfree+" "+free;
			Long Lfree = new Long(free);
			if (freevisit.containsKey(Lfree)) 
			{
				throw new BplusTreeException("cycle in freelist "+free);
			}
			//freevisit[free] = free;
			freevisit.put(Lfree, Lfree);
			free = this.parseFreeBuffer(free);
		}
		if (allfree.length()==0) 
		{
			sb.write("empty list");
		} 
		else 
		{
			sb.write(allfree);
		}
		sb.write("\r\n<br> free on abort "+this.FreeBuffersOnAbort.size()+" ::");
		//foreach (Object thing in this.FreeBuffersOnAbort) 
		//for (int i=0; i<this.FreeBuffersOnAbort.size(); i++)
		//{
		//	sb.write(" "+this.FreeBuffersOnAbort.get(i));
		//}
		sb.write("\r\n<br>\r\n");
	
		//... add more
		if (this.root==null) 
		{
			sb.write("<br><b>NULL ROOT</b>\r\n");
		} 
		else 
		{
			this.root.AsHtml(sb);
		}
		return sb.toString();
	}
	public BplusTreeLong(java.io.RandomAccessFile fromfile, int KeyLength, int NodeSize, int CultureId) //:
		//this(fromfile, NodeSize, KeyLength, (long)0, CultureId) 
		throws Exception
	{
		// just start seek at 0
		this(fromfile, NodeSize, KeyLength, (long)0, CultureId);
	}
	public static BplusTreeLong SetupFromExistingStream(java.io.RandomAccessFile fromfile) 
		throws Exception
	{
		return SetupFromExistingStream(fromfile, (long)0);
	}
	public static BplusTreeLong SetupFromExistingStream(java.io.RandomAccessFile fromfile, long StartSeek)
		throws Exception 
	{
		//int dummyId = System.Globalization.CultureInfo.InvariantCulture.LCID;
		BplusTreeLong result = new BplusTreeLong(fromfile, 7, 100, StartSeek, INVARIANTCULTUREID); // dummy values for nodesize, keysize
		result.readHeader();
		result.buffers = BufferFile.SetupFromExistingStream(fromfile, StartSeek+result.headersize);
		if (result.buffers.buffersize != result.buffersize) 
		{
			throw new BplusTreeException("inner and outer buffer sizes should match");
		}
		if (result.rootSeek!=NULLBUFFERNUMBER) 
		{
			result.root = new BplusNode(result, null, -1, true);
			result.root.LoadFromBuffer(result.rootSeek);
		}
		return result;
	}
	public static BplusTreeLong InitializeInStream(java.io.RandomAccessFile fromfile, int KeyLength, int NodeSize) 
		throws Exception
	{
		//int dummyId = System.Globalization.CultureInfo.InvariantCulture.LCID;
		return InitializeInStream(fromfile, KeyLength, NodeSize, INVARIANTCULTUREID);
	}
	public static BplusTreeLong InitializeInStream(java.io.RandomAccessFile fromfile, int KeyLength, int NodeSize, int CultureId) 
		throws Exception
	{
		return InitializeInStream(fromfile, KeyLength, NodeSize, CultureId, (long)0);
	}
	public static BplusTreeLong InitializeInStream(java.io.RandomAccessFile fromfile, int KeyLength, int NodeSize, int CultureId, long StartSeek) 
		throws Exception
	{
		if (fromfile.length()>StartSeek) 
		{
			throw new BplusTreeException("can't initialize bplus tree inside written area of stream");
		}
		BplusTreeLong result = new BplusTreeLong(fromfile, NodeSize, KeyLength, StartSeek, CultureId);
		result.setHeader();
		result.buffers = BufferFile.InitializeBufferFileInStream(fromfile, result.buffersize, StartSeek+result.headersize);
		return result;
	}
	public void SetFootPrintLimit(int limit) 
		throws Exception
	{
		if (limit<5) 
		{
			throw new BplusTreeException("foot print limit less than 5 is too small");
		}
		this.FifoLimit = limit;
	}
	public void RemoveKey(String key) 
		throws Exception
	{
		if (this.root==null) 
		{
			throw new BplusTreeKeyMissing("tree is empty: cannot delete");
		}
		boolean MergeMe;
		BplusNode theroot = this.root;
		BplusNode.Delete deletion = theroot.delete(key);
		MergeMe = deletion.MergeMe;
		// if the root is not a leaf and contains only one child (no key), reroot
		if (MergeMe && !this.root.isLeaf && this.root.SizeInUse()==0) 
		{
			this.root = this.root.FirstChild();
			this.rootSeek = this.root.makeRoot();
			theroot.Free();
		}
	}
	public long get(String key)
		throws Exception
	{
		//long valueFound;
		boolean test = this.ContainsKey(key);
		if (!test) 
		{
			throw new BplusTreeKeyMissing("no such key found: "+key);
		}
		return this.LastValueFound;
	}
	public void set(String key, long value)
		throws Exception
	{
		if (!BplusNode.KeyOK(key, this)) 
		{
			String data = "null";
			if (key!=null) 
			{
				data = "key "+key+" length "+key.length();
			}
			throw new BplusTreeBadKeyValue("null or too large key cannot be inserted into tree: "+data);
		}
		boolean rootinit = false;
		if (this.root==null) 
		{
			// allocate root
			this.root = new BplusNode(this, null, -1, true);
			rootinit = true;
			//this.rootSeek = root.DumpToFreshBuffer();
		}
		// insert into root...
		//root.Insert(key, value, out splitString, out splitNode);
		root.Insert(key, value);
		String splitString = root.splitString;
		BplusNode splitNode = root.splitNode;
		// clear split info
		root.splitString = null;
		root.splitNode = null;
		if (splitNode!=null) 
		{
			// split of root: make a new root.
			rootinit = true;
			BplusNode oldRoot = this.root;
			this.root = BplusNode.BinaryRoot(oldRoot, splitString, splitNode, this);
		}
		if (rootinit) 
		{
			this.rootSeek = root.DumpToFreshBuffer();
		}
		// check size in memory
		this.ShrinkFootprint();
	}
		
	public String FirstKey() 
		throws Exception
	{
		String result = null;
		if (this.root!=null) 
		{
			// empty String is smallest possible tree
			if (this.ContainsKey("")) 
			{
				result = "";
			} 
			else 
			{
				return this.root.FindNextKey("");
			}
			this.ShrinkFootprint();
		}
		return result;
	}
	public String NextKey(String AfterThisKey) 
		throws Exception
	{
		if (AfterThisKey==null) 
		{
			throw new BplusTreeBadKeyValue("cannot search for null String");
		}
		String result = this.root.FindNextKey(AfterThisKey);
		this.ShrinkFootprint();
		return result;
	}
	//		public boolean containsKey(String key) 
	//		{
	//			long valueFound;
	//			return this.containsKey(key, out valueFound);
	//		} 
	public boolean ContainsKey(String key) 
		throws Exception
	{
		if (key==null)
		{
			throw new BplusTreeBadKeyValue("cannot search for null String");
		}
		boolean result = false;
		//valueFound = (long) 0;
		if (this.root!=null) 
		{
			result = this.root.FindMatch(key);
			this.LastValueFound = this.root.LastValueFound;
		}
		this.ShrinkFootprint();
		return result;
	}
	//		public long Get(String key, long defaultValue) 
	//		{
	//			long result = defaultValue;
	//			long valueFound;
	//			if (this.containsKey(key))
	//			{
	//				result = this.LastValueFound;
	//			}
	//			return result;
	//		}
	public void Set(String key, Object map) 
		throws Exception
	{
		//if (!(map is long)) 
		if (map.getClass()!=Long.class)
		{
			throw new BplusTreeBadKeyValue("only longs may be used as values in a BplusTreeLong: "+map);
		}
		//this[key] = ((Long) map).longValue();
		this.set(key, ((Long) map).longValue());
	}
	public Object Get(String key, Object defaultValue) 
		throws Exception
	{
		long valueFound;
		if (this.ContainsKey(key)) 
		{
			return new Long(this.LastValueFound);
		}
		return defaultValue;
	}
	/// <summary>
	/// Store off any changed buffers, clear the fifo, free invalid buffers
	/// </summary>
	public void Commit() 
		throws Exception
	{
		// store all modifications
		if (this.root!=null) 
		{
			this.rootSeek = this.root.Invalidate(false);
		}
		//this.fromfile.Flush();
		// commit the new root
		this.setHeader();
		//this.fromfile.Flush();
		// at this point the changes are committed, but some space is unreachable.
		// now free all unfreed buffers no longer in use
		//this.FreeBuffersOnCommit.Sort();
		//this.FreeBuffersOnCommit.Reverse();
		//foreach (Object thing in this.FreeBuffersOnCommit) 
		//for (int i=0; i<this.FreeBuffersOnCommit.size(); i++)
		for (Enumeration e=this.FreeBuffersOnCommit.keys(); e.hasMoreElements(); )
		{
			Long thing = (Long) e.nextElement();
			long buffernumber = thing.longValue();
			this.deallocateBuffer(buffernumber);
		}
		// store the free list head
		this.setHeader();
		//this.fromfile.Flush();
		this.ResetBookkeeping();
	}
	/// <summary>
	/// Forget all changes since last commit
	/// </summary>
	public void Abort() 
		throws Exception
	{
		// deallocate allocated blocks
		//this.FreeBuffersOnAbort.Sort();
		//this.FreeBuffersOnAbort.Reverse();
		//foreach (Object thing in this.FreeBuffersOnAbort) 
		//for (int i=0; i<this.FreeBuffersOnAbort.size(); i++)
		for (Enumeration e=this.FreeBuffersOnAbort.keys(); e.hasMoreElements(); )
		{
			Long thing = (Long) e.nextElement();
			long buffernumber = thing.longValue();
			this.deallocateBuffer(buffernumber);
		}
		long freehead = this.freeHeadSeek;
		// reread the header (except for freelist head)
		this.readHeader();
		// restore the root
		if (this.rootSeek==NULLBUFFERNUMBER) 
		{
			this.root = null; // nothing was committed
		} 
		else 
		{
			this.root.LoadFromBuffer(this.rootSeek);
		}
		this.ResetBookkeeping();
		this.freeHeadSeek = freehead;
		this.setHeader(); // store new freelist head
		//this.fromfile.Flush();
	}
	void ResetBookkeeping() 
		throws Exception
	{
		this.FreeBuffersOnCommit.clear();
		this.FreeBuffersOnAbort.clear();
		this.IdToTerminalNode.clear();
		this.TerminalNodeToId.clear();
	}
	public long allocateBuffer() 
		throws Exception
	{
		long allocated = -1;
		if (this.freeHeadSeek==NULLBUFFERNUMBER) 
		{
			// should be written immediately after allocation
			allocated = this.buffers.nextBufferNumber();
			//System.Diagnostics.Debug.WriteLine("<br> allocating fresh buffer "+allocated);
			return allocated;
		}
		// get the free head data
		allocated = this.freeHeadSeek;
		this.freeHeadSeek = this.parseFreeBuffer(allocated);
		//System.Diagnostics.Debug.WriteLine("<br> recycling free buffer "+allocated);
		return allocated;
	}
	long parseFreeBuffer(long buffernumber) 
		throws Exception
	{
		int freesize = 1+BufferFile.LONGSTORAGE;
		byte[] buffer = new byte[freesize];
		this.buffers.getBuffer(buffernumber, buffer, 0, freesize);
		if (buffer[0]!=FREE) 
		{
			throw new BplusTreeException("free buffer not marked free");
		}
		long result = BufferFile.RetrieveLong(buffer, 1);
		return result;
	}
	public void deallocateBuffer(long buffernumber) 
		throws Exception
	{
		//System.Diagnostics.Debug.WriteLine("<br> deallocating "+buffernumber);
		int freesize = 1+BufferFile.LONGSTORAGE;
		byte[] buffer = new byte[freesize];
		// it better not already be marked free
		this.buffers.getBuffer(buffernumber, buffer, 0, 1);
		if (buffer[0]==FREE) 
		{
			throw new BplusTreeException("attempt to re-free free buffer not allowed");
		}
		buffer[0] = FREE;
		BufferFile.Store(this.freeHeadSeek, buffer, 1);
		this.buffers.setBuffer(buffernumber, buffer, 0, freesize);
		this.freeHeadSeek = buffernumber;
	}
	void setHeader() 
		throws Exception
	{
		byte[] header = this.makeHeader();
		this.fromfile.seek(this.seekStart);//, System.IO.SeekOrigin.Begin);
		this.fromfile.write(header, 0, header.length);
	}
	public void RecordTerminalNode(BplusNode terminalNode) 
		throws Exception
	{
		if (terminalNode==this.root) 
		{
			return; // never record the root node
		}
		if (this.TerminalNodeToId.containsKey(terminalNode) )
		{
			return; // don't record it again
		}
		Integer id = new Integer(this.TerminalNodeCount);
		this.TerminalNodeCount++;
		//this.TerminalNodeToId[terminalNode] = id;
		this.TerminalNodeToId.put(terminalNode, id);
		//this.IdToTerminalNode[id] = terminalNode;
		this.IdToTerminalNode.put(id, terminalNode);
	}
	public void ForgetTerminalNode(BplusNode nonterminalNode) 
		throws Exception
	{
		if (!this.TerminalNodeToId.containsKey(nonterminalNode)) 
		{
			// silently ignore (?)
			return;
		}
		Integer id = (Integer) this.TerminalNodeToId.get(nonterminalNode);
		if (id.intValue() == this.LowerTerminalNodeCount) 
		{
			this.LowerTerminalNodeCount++;
		}
		this.IdToTerminalNode.remove(id);
		this.TerminalNodeToId.remove(nonterminalNode);
	}
	public void ShrinkFootprint() 
		throws Exception
	{
		this.InvalidateTerminalNodes(this.FifoLimit);
	}
	public void InvalidateTerminalNodes(int toLimit) 
		throws Exception
	{
		while (this.TerminalNodeToId.size()>toLimit) 
		{
			// choose oldest nonterminal and deallocate it
			Integer id = new Integer(this.LowerTerminalNodeCount);
			while (!this.IdToTerminalNode.containsKey(id)) 
			{
				this.LowerTerminalNodeCount++; // since most nodes are terminal this should usually be a short walk
				id = new Integer(this.LowerTerminalNodeCount);
				//System.Diagnostics.Debug.WriteLine("<BR>WALKING "+this.LowerTerminalNodeCount);
				//System.Console.WriteLine("<BR>WALKING "+this.LowerTerminalNodeCount);
				if (this.LowerTerminalNodeCount>this.TerminalNodeCount) 
				{
					throw new BplusTreeException("internal error counting nodes, lower limit went too large");
				}
			}
			//System.Console.WriteLine("<br> done walking");
			//int id = this.LowerTerminalNodeCount;
			BplusNode victim = (BplusNode) this.IdToTerminalNode.get(id);
			//System.Diagnostics.Debug.WriteLine("\r\n<br>selecting "+victim.myBufferNumber+" for deallocation from fifo");
			this.IdToTerminalNode.remove(id);
			this.TerminalNodeToId.remove(victim);
			if (victim.myBufferNumber!=NULLBUFFERNUMBER) 
			{
				victim.Invalidate(true);
			}
		}
	}
	void readHeader() 
		throws Exception
	{
		// prefix | version | node size | key size | culture id | buffer number of root | buffer number of free list head
		byte[] header = new byte[this.headersize];
		this.fromfile.seek(this.seekStart); //, System.IO.SeekOrigin.Begin);
		this.fromfile.read(header, 0, this.headersize);
		int index = 0;
		// check prefix
		//foreach (byte b in HEADERPREFIX) 
		for (index=0; index<HEADERPREFIX.length; index++)
		{
			if (header[index]!=HEADERPREFIX[index]) 
			{
				throw new BplusTreeException("invalid header prefix");
			}
			index++;
		}
		index = HEADERPREFIX.length;
		// skip version (for now)
		index++;
		this.NodeSize = BufferFile.Retrieve(header, index);
		index+= BufferFile.INTSTORAGE;
		this.KeyLength = BufferFile.Retrieve(header, index);
		index+= BufferFile.INTSTORAGE;
		int CultureId = BufferFile.Retrieve(header, index);
		//this.cultureContext = new System.Globalization.CultureInfo(CultureId);
		if (CultureId!=INVARIANTCULTUREID) 
		{
			throw new BplusTreeException("BplusJ only supports the invariant culture");
		}
		index+= BufferFile.INTSTORAGE;
		this.rootSeek = BufferFile.RetrieveLong(header, index);
		index+= BufferFile.LONGSTORAGE;
		this.freeHeadSeek = BufferFile.RetrieveLong(header, index);
		this.SanityCheck();
		//this.header = header;
	}
	public byte[] makeHeader() 
		throws Exception
	{
		// prefix | version | node size | key size | culture id | buffer number of root | buffer number of free list head
		byte[] result = new byte[this.headersize];
		//HEADERPREFIX.CopyTo(result, 0);
		for (int i=0; i<HEADERPREFIX.length; i++) 
		{
			result[i] = HEADERPREFIX[i];
		}
		result[HEADERPREFIX.length] = VERSION;
		int index = HEADERPREFIX.length+1;
		BufferFile.Store(this.NodeSize, result, index);
		index+= BufferFile.INTSTORAGE;
		BufferFile.Store(this.KeyLength, result, index);
		index+= BufferFile.INTSTORAGE;
		BufferFile.Store(INVARIANTCULTUREID, result, index);
		index+= BufferFile.INTSTORAGE;
		BufferFile.Store(this.rootSeek, result, index);
		index+= BufferFile.LONGSTORAGE;
		BufferFile.Store(this.freeHeadSeek, result, index);
		return result;
	}
	
	public static class BplusNode 
	{
		public boolean isLeaf = true;
		// the maximum number of children to each node.
		int Size;
		// false if the node is no longer active and should not be used.
		boolean isValid = true;
		// true if the materialized node needs to be persisted.
		boolean Dirty = true;
		// if non-root reference to the parent node containing this node
		BplusNode parent = null;
		// tree containing this node
		BplusTreeLong owner = null;
		// buffer number of this node
		public long myBufferNumber = BplusTreeLong.NULLBUFFERNUMBER;
		// number of children used by this node
		//int NumberOfValidKids = 0;
		long[] ChildBufferNumbers;
		String[] ChildKeys;
		BplusNode[] MaterializedChildNodes;
		int indexInParent = -1;
		/// Temporary slots for use in splitting, fetching
		public BplusNode splitNode = null;
		public String splitString = null;
		public long LastValueFound = 0;
		/// <summary>
		/// Create a new BplusNode and install in parent if parent is not null.
		/// </summary>
		/// <param name="owner">tree containing the node</param>
		/// <param name="parent">parent node (if provided)</param>
		/// <param name="indexInParent">location in parent if provided</param>
		public BplusNode(BplusTreeLong owner, BplusNode parent, int indexInParent, boolean isLeaf) 
			throws Exception
		{
			this.isLeaf = isLeaf;
			this.owner = owner;
			this.parent = parent;
			this.Size = owner.NodeSize;
			this.isValid = true;
			this.Dirty = true;
			//			this.ChildBufferNumbers = new long[this.Size+1];
			//			this.ChildKeys = new String[this.Size];
			//			this.MaterializedChildNodes = new BplusNode[this.Size+1];
			this.Clear();
			if (parent!=null && indexInParent>=0) 
			{
				if (indexInParent>this.Size) 
				{
					throw new BplusTreeException("parent index too large");
				}
				// key info, etc, set elsewhere
				this.parent.MaterializedChildNodes[indexInParent] = this;
				this.myBufferNumber = this.parent.ChildBufferNumbers[indexInParent];
				this.indexInParent = indexInParent;
			}
		}
		public BplusNode FirstChild() 
			throws Exception
		{
			BplusNode result = this.MaterializeNodeAtIndex(0);
			if (result==null) 
			{
				throw new BplusTreeException("no first child");
			}
			return result;
		}
		public long makeRoot() 
			throws Exception
		{
			this.parent = null;
			this.indexInParent = -1;
			if (this.myBufferNumber==BplusTreeLong.NULLBUFFERNUMBER) 
			{
				throw new BplusTreeException("no root seek allocated to new root");
			}
			return this.myBufferNumber;
		}
		public void Free() throws Exception
		{
			if (this.myBufferNumber!=BplusTreeLong.NULLBUFFERNUMBER) 
			{
				Long L = new Long(this.myBufferNumber);
				if (this.owner.FreeBuffersOnAbort.containsKey(L)) 
				{
					// free it now
					this.owner.FreeBuffersOnAbort.remove(L);
					this.owner.deallocateBuffer(this.myBufferNumber);
				} 
				else 
				{
					// free on commit
					//this.owner.FreeBuffersOnCommit.Add(this.myBufferNumber);
					this.owner.FreeBuffersOnCommit.put(L,L);
				}
			}
			this.myBufferNumber = BplusTreeLong.NULLBUFFERNUMBER; // don't do it twice...
		}
		public void SerializationCheck() 
			throws Exception
		{ 
			BplusNode A = new BplusNode(this.owner, null, -1, false);
			for (int i=0; i<this.Size; i++) 
			{
				long j = i*((long)0xf0f0f0f0f0f0f01L);
				A.ChildBufferNumbers[i] = j;
				A.ChildKeys[i] = "k"+i;
			}
			A.ChildBufferNumbers[this.Size] = 7;
			A.TestRebuffer();
			A.isLeaf = true;
			for (int i=0; i<this.Size; i++) 
			{
				long j = -i*((long)0x3e3e3e3e3e3e666L);
				A.ChildBufferNumbers[i] = j;
				A.ChildKeys[i] = "key"+i;
			}
			A.ChildBufferNumbers[this.Size] = -9097;
			A.TestRebuffer();
		}
		void TestRebuffer() 
			throws Exception
		{
			boolean IL = this.isLeaf;
			long[] Ns = this.ChildBufferNumbers;
			String[] Ks = this.ChildKeys;
			byte[] buffer = new byte[this.owner.buffersize];
			this.Dump(buffer);
			this.Clear();
			this.Load(buffer);
			for (int i=0; i<this.Size; i++) 
			{
				if (this.ChildBufferNumbers[i]!=Ns[i]) 
				{
					throw new BplusTreeException("didn't get back buffernumber "+i+" got "+this.ChildBufferNumbers[i]+" not "+Ns[i]);
				}
				if (!this.ChildKeys[i].equals(Ks[i])) 
				{
					throw new BplusTreeException("didn't get back key "+i+" got "+this.ChildKeys[i]+" not "+Ks[i]);
				}
			}
			if (this.ChildBufferNumbers[this.Size]!=Ns[this.Size]) 
			{
				throw new BplusTreeException("didn't get back buffernumber "+this.Size+" got "+this.ChildBufferNumbers[this.Size]+" not "+Ns[this.Size]);
			}
			if (this.isLeaf!=IL) 
			{
				throw new BplusTreeException("isLeaf should be "+IL+" got "+this.isLeaf);
			}
		}
		public String SanityCheck(Hashtable visited) 
			throws Exception
		{
			String result = null;
			if (visited==null) 
			{
				visited = new Hashtable();
			}
			if (visited.containsKey(this)) 
			{
				throw new BplusTreeException("node visited twice "+this.myBufferNumber);
			}
			//visited[this] = this.myBufferNumber;
			visited.put(this, new Long(this.myBufferNumber));
			if (this.myBufferNumber!=BplusTreeLong.NULLBUFFERNUMBER) 
			{
				Long bf = new Long(this.myBufferNumber);
				if (visited.containsKey(bf))
				{
					throw new BplusTreeException("buffer number seen twice "+this.myBufferNumber);
				}
				//visited[bf] = this;
				visited.put(bf, this);
			}
			if (this.parent!=null) 
			{
				if (this.parent.isLeaf) 
				{
					throw new BplusTreeException("parent is leaf");
				}
				this.parent.MaterializeNodeAtIndex(this.indexInParent);
				if (this.parent.MaterializedChildNodes[this.indexInParent]!=this) 
				{
					throw new BplusTreeException("incorrect index in parent");
				}
				// since not at root there should be at least size/2 keys
				int limit = this.Size/2;
				if (this.isLeaf) 
				{
					limit--;
				}
				for (int i=0; i<limit; i++) 
				{
					if (this.ChildKeys[i]==null) 
					{
						throw new BplusTreeException("null child in first half");
					}
				}
			}
			result = this.ChildKeys[0]; // for leaf
			if (!this.isLeaf) 
			{
				this.MaterializeNodeAtIndex(0);
				result = this.MaterializedChildNodes[0].SanityCheck(visited);
				for (int i=0; i<this.Size; i++) 
				{
					if (this.ChildKeys[i]==null) 
					{
						break;
					}
					this.MaterializeNodeAtIndex(i+1);
					String least = this.MaterializedChildNodes[i+1].SanityCheck(visited);
					if (least==null) 
					{
						throw new BplusTreeException("null least in child doesn't match node entry "+this.ChildKeys[i]);
					}
					if (!least.equals(this.ChildKeys[i])) 
					{
						throw new BplusTreeException("least in child "+least+" doesn't match node entry "+this.ChildKeys[i]);
					}
				}
			}
			// look for duplicate keys
			String lastkey = this.ChildKeys[0];
			for (int i=1; i<this.Size; i++) 
			{
				if (this.ChildKeys[i]==null) 
				{
					break;
				}
				if (lastkey.equals(this.ChildKeys[i]) ) 
				{
					throw new BplusTreeException("duplicate key in node "+lastkey);
				}
				lastkey = this.ChildKeys[i];
			}
			return result;
		}
		void Destroy() 
		{
			// make sure the structure is useless, it should no longer be used.
			this.owner = null;
			this.parent = null;
			this.Size = -100;
			this.ChildBufferNumbers = null;
			this.ChildKeys = null;
			this.MaterializedChildNodes = null;
			this.myBufferNumber = BplusTreeLong.NULLBUFFERNUMBER;
			this.indexInParent = -100;
			this.Dirty = false;
		}
		public int SizeInUse() 
		{
			int result = 0;
			for (int i=0; i<this.Size; i++) 
			{
				if (this.ChildKeys[i]==null) 
				{
					break;
				}
				result++;
			}
			return result;
		}
		public static BplusNode BinaryRoot(BplusNode LeftNode, String key, BplusNode RightNode, BplusTreeLong owner) 
			throws Exception
		{
			BplusNode newRoot = new BplusNode(owner, null, -1, false);
			//newRoot.Clear(); // redundant
			newRoot.ChildKeys[0] = key;
			LeftNode.Reparent(newRoot, 0);
			RightNode.Reparent(newRoot, 1);
			// new root is stored elsewhere
			return newRoot;
		}
		void Reparent(BplusNode newParent, int ParentIndex) 
			throws Exception
		{
			// keys and existing parent structure must be updated elsewhere.
			this.parent = newParent;
			this.indexInParent = ParentIndex;
			newParent.ChildBufferNumbers[ParentIndex] = this.myBufferNumber;
			newParent.MaterializedChildNodes[ParentIndex] = this;
			// parent is no longer terminal
			this.owner.ForgetTerminalNode(parent);
		}
		void Clear() 
			throws Exception
		{
			this.ChildBufferNumbers = new long[this.Size+1];
			this.ChildKeys = new String[this.Size];
			this.MaterializedChildNodes = new BplusNode[this.Size+1];
			for (int i=0; i<this.Size; i++) 
			{
				this.ChildBufferNumbers[i] = BplusTreeLong.NULLBUFFERNUMBER;
				this.MaterializedChildNodes[i] = null;
				this.ChildKeys[i] = null;
			}
			this.ChildBufferNumbers[this.Size] = BplusTreeLong.NULLBUFFERNUMBER;
			this.MaterializedChildNodes[this.Size] = null;
			// this is now a terminal node
			this.owner.RecordTerminalNode(this);
		}
		/// <summary>
		/// Find first index in self associated with a key same or greater than CompareKey
		/// </summary>
		/// <param name="CompareKey">CompareKey</param>
		/// <param name="LookPastOnly">if true and this is a leaf then look for a greater value</param>
		/// <returns>lowest index of same or greater key or this.Size if no greater key.</returns>
		int FindAtOrNextPosition(String CompareKey, boolean LookPastOnly) 
			throws Exception
		{
			int insertposition = 0;
			//System.Globalization.CultureInfo culture = this.owner.cultureContext;
			//System.Globalization.CompareInfo cmp = culture.CompareInfo;
			if (this.isLeaf && !LookPastOnly) 
			{
				// look for exact match or greater or null
				while (insertposition<this.Size && this.ChildKeys[insertposition]!=null &&
					//cmp.Compare(this.ChildKeys[insertposition], CompareKey)<0) 
					this.owner.Compare(this.ChildKeys[insertposition], CompareKey)<0)
				{
					insertposition++;
				}
			} 
			else 
			{
				// look for greater or null only
				while (insertposition<this.Size && this.ChildKeys[insertposition]!=null &&
					this.owner.Compare(this.ChildKeys[insertposition], CompareKey)<=0) 
				{
					insertposition++;
				}
			}
			return insertposition;
		}
		/// <summary>
		/// Find the first key below atIndex, or if no such node traverse to the next key to the right.
		/// If no such key exists, return nulls.
		/// </summary>
		/// <param name="atIndex">where to look in this node</param>
		/// <param name="FoundInLeaf">leaf where found</param>
		/// <param name="KeyFound">key value found</param>
		TraverseToFollowingKey traverseToFollowingKey(int atIndex) 
			throws Exception
		{
			return new TraverseToFollowingKey(atIndex);
		}
		class TraverseToFollowingKey 
		{
			public BplusNode FoundInLeaf = null;
			public String KeyFound = null;
			public TraverseToFollowingKey(int atIndex) 
				throws Exception
			{
				this.FoundInLeaf = null;
				this.KeyFound = null;
				boolean LookInParent = false;
				if (BplusNode.this.isLeaf) 
				{
					LookInParent = (atIndex>=BplusNode.this.Size) || (BplusNode.this.ChildKeys[atIndex]==null);
				} 
				else 
				{
					LookInParent = (atIndex>BplusNode.this.Size) ||
						(atIndex>0 && BplusNode.this.ChildKeys[atIndex-1]==null);
				}
				if (LookInParent) 
				{
					// if it's anywhere it's in the next child of parent
					if (BplusNode.this.parent!=null && BplusNode.this.indexInParent>=0) 
					{
						TraverseToFollowingKey t = BplusNode.this.parent.traverseToFollowingKey(BplusNode.this.indexInParent+1);//, out FoundInLeaf, out KeyFound);
						this.FoundInLeaf = t.FoundInLeaf;
						this.KeyFound = t.KeyFound;
						return;
					} 
					else 
					{
						return; // no such following key
					}
				}
				if (BplusNode.this.isLeaf) 
				{
					// leaf, we found it.
					FoundInLeaf = BplusNode.this;
					KeyFound = BplusNode.this.ChildKeys[atIndex];
					return;
				} 
				else 
				{
					// nonleaf, look in child (if there is one)
					if (atIndex==0 || BplusNode.this.ChildKeys[atIndex-1]!=null) 
					{
						BplusNode thechild = BplusNode.this.MaterializeNodeAtIndex(atIndex);
						TraverseToFollowingKey t = thechild.traverseToFollowingKey(0); //, out FoundInLeaf, out KeyFound);this.FoundInLeaf = t.FoundInLeaf;
						this.KeyFound = t.KeyFound;
						this.FoundInLeaf = t.FoundInLeaf;
					}
				}
			}
		}
		public boolean FindMatch(String CompareKey) //, out long ValueFound) 
			throws Exception
		{
			this.LastValueFound = 0; // dummy value on failure
			BplusNode leaf;
			//int position = this.FindAtOrNextPositionInLeaf(CompareKey, out leaf, false);
			FindAtOrNextPositionInLeaf f = new FindAtOrNextPositionInLeaf(CompareKey, false);
			leaf = f.inLeaf;
			int position = f.atPosition;
			if (position<leaf.Size) 
			{
				String key = leaf.ChildKeys[position];
				if ((key!=null) && this.owner.Compare(key, CompareKey)==0) //(key.equals(CompareKey)
				{
					this.LastValueFound = leaf.ChildBufferNumbers[position];
					return true;
				}
			}
			return false;
		}
		public String FindNextKey(String CompareKey) 
			throws Exception
		{
			String result = null;
			BplusNode leaf;
			//int position = this.FindAtOrNextPositionInLeaf(CompareKey, out leaf, true);
			FindAtOrNextPositionInLeaf f = new FindAtOrNextPositionInLeaf(CompareKey, true);
			leaf = f.inLeaf;
			int position = f.atPosition;
			if (position>=leaf.Size || leaf.ChildKeys[position]==null) 
			{
				// try to traverse to the right.
				BplusNode newleaf;
				TraverseToFollowingKey t = leaf.traverseToFollowingKey(leaf.Size); //, out newleaf, out result);
				newleaf = t.FoundInLeaf;
				result = t.KeyFound;
			} 
			else 
			{
				result = leaf.ChildKeys[position];
			}
			return result;
		}
		/// <summary>
		/// Find near-index of comparekey in leaf under this node. 
		/// </summary>
		/// <param name="CompareKey">the key to look for</param>
		/// <param name="inLeaf">the leaf where found</param>
		/// <param name="LookPastOnly">If true then only look for a greater value, not an exact match.</param>
		/// <returns>index of match in leaf</returns>
		FindAtOrNextPositionInLeaf findAtOrNextPositionInLeaf(String CompareKey, boolean LookPastOnly) 
			throws Exception
		{
			return new FindAtOrNextPositionInLeaf(CompareKey, LookPastOnly);
		}
		class FindAtOrNextPositionInLeaf 
		{
			public BplusNode inLeaf;
			public int atPosition;
			public FindAtOrNextPositionInLeaf(String CompareKey, boolean LookPastOnly) 
				throws Exception
			{
				int myposition = BplusNode.this.FindAtOrNextPosition(CompareKey, LookPastOnly);
				if (BplusNode.this.isLeaf) 
				{
					this.inLeaf = BplusNode.this;
					this.atPosition = myposition;
					return;
				}
				long childBufferNumber = BplusNode.this.ChildBufferNumbers[myposition];
				if (childBufferNumber==BplusTreeLong.NULLBUFFERNUMBER) 
				{
					throw new BplusTreeException("can't search null subtree");
				}
				BplusNode child = BplusNode.this.MaterializeNodeAtIndex(myposition);
				FindAtOrNextPositionInLeaf f = child.findAtOrNextPositionInLeaf(CompareKey, LookPastOnly);
				this.inLeaf = f.inLeaf;
				this.atPosition = f.atPosition;
			}
		}
		BplusNode MaterializeNodeAtIndex(int myposition) 
			throws Exception
		{
			if (this.isLeaf) 
			{
				throw new BplusTreeException("cannot materialize child for leaf");
			}
			long childBufferNumber = this.ChildBufferNumbers[myposition];
			if (childBufferNumber==BplusTreeLong.NULLBUFFERNUMBER) 
			{
				throw new BplusTreeException("can't search null subtree at position "+myposition+" in "+this.myBufferNumber);
			}
			// is it already materialized?
			BplusNode result = this.MaterializedChildNodes[myposition];
			if (result!=null) 
			{
				return result;
			}
			// otherwise read it in...
			result = new BplusNode(this.owner, this, myposition, true); // dummy isLeaf value
			result.LoadFromBuffer(childBufferNumber);
			this.MaterializedChildNodes[myposition] = result;
			// no longer terminal
			this.owner.ForgetTerminalNode(this);
			return result;
		}
		public void LoadFromBuffer(long bufferNumber) 
			throws Exception
		{
			// freelist bookkeeping done elsewhere
			String parentinfo = "no parent"; // debug
			if (this.parent!=null) 
			{
				parentinfo = "parent="+parent.myBufferNumber; // debug
			}
			//System.Diagnostics.Debug.WriteLine("\r\n<br> loading "+this.indexInParent+" from "+bufferNumber+" for "+parentinfo);
			byte[] rawdata = new byte[this.owner.buffersize];
			this.owner.buffers.getBuffer(bufferNumber, rawdata, 0, rawdata.length);
			this.Load(rawdata);
			this.Dirty = false;
			this.myBufferNumber = bufferNumber;
			// it's terminal until a child is materialized
			this.owner.RecordTerminalNode(this);
		}
		public long DumpToFreshBuffer() throws Exception
		{
			long oldbuffernumber = this.myBufferNumber;
			long freshBufferNumber = this.owner.allocateBuffer();
			//System.Diagnostics.Debug.WriteLine("\r\n<br> dumping "+this.indexInParent+" from "+oldbuffernumber+" to "+freshBufferNumber);
			this.DumpToBuffer(freshBufferNumber);
			if (oldbuffernumber!=BplusTreeLong.NULLBUFFERNUMBER) 
			{
				Long L = new Long(oldbuffernumber);
				//this.owner.FreeBuffersOnCommit.Add(oldbuffernumber);
				if (this.owner.FreeBuffersOnAbort.containsKey(L)) 
				{
					// free it now
					this.owner.FreeBuffersOnAbort.remove(L);
					this.owner.deallocateBuffer(oldbuffernumber);
				} 
				else 
				{
					// free on commit
					this.owner.FreeBuffersOnCommit.put(L,L);
				}
			}
			//this.owner.FreeBuffersOnAbort.Add(freshBufferNumber);
			Long F = new Long(freshBufferNumber);
			this.owner.FreeBuffersOnAbort.put(F, F);
			return freshBufferNumber;
		}
		void DumpToBuffer(long buffernumber) 
			throws Exception
		{
			byte[] rawdata = new byte[this.owner.buffersize];
			this.Dump(rawdata);
			this.owner.buffers.setBuffer(buffernumber, rawdata, 0, rawdata.length);
			this.Dirty = false;
			this.myBufferNumber = buffernumber;
			if (this.parent!=null && this.indexInParent>=0 &&
				this.parent.ChildBufferNumbers[this.indexInParent]!=buffernumber) 
			{
				if (this.parent.MaterializedChildNodes[this.indexInParent]!=this) 
				{
					throw new BplusTreeException("invalid parent connection "+this.parent.myBufferNumber+" at "+this.indexInParent);
				}
				this.parent.ChildBufferNumbers[this.indexInParent] = buffernumber;
				this.parent.Soil();
			}
		}
		void reParentAllChildren() 
			throws Exception
		{
			for (int i=0; i<=this.Size; i++) 
			{
				BplusNode thisnode = this.MaterializedChildNodes[i];
				if (thisnode!=null) 
				{
					thisnode.Reparent(this, i);
				}
			}
		}
		/// <summary>
		/// Delete entry for key
		/// </summary>
		/// <param name="key">key to delete</param>
		/// <param name="MergeMe">true if the node is less than half full after deletion</param>
		/// <returns>null unless the smallest key under this node has changed in which case it returns the smallest key.</returns>
		Delete delete(String Key) throws Exception
		{
			return new Delete(Key);
		}
		public class Delete
		{
			public String smallestKey;
			public boolean MergeMe;
			public Delete(String key) 
				throws Exception
			{
				this.MergeMe = false; // assumption
				this.smallestKey = null;
				if (BplusNode.this.isLeaf) 
				{
					this.DeleteLeaf(key);
					return;
				}
				int deleteposition = BplusNode.this.FindAtOrNextPosition(key, false);
				long deleteBufferNumber = BplusNode.this.ChildBufferNumbers[deleteposition];
				if (deleteBufferNumber==BplusTreeLong.NULLBUFFERNUMBER) 
				{
					throw new BplusTreeException("key not followed by buffer number in non-leaf (del)");
				}
				// del in subtree
				BplusNode DeleteChild = BplusNode.this.MaterializeNodeAtIndex(deleteposition);
				boolean MergeKid;
				//String delresult = DeleteChild.Delete(key, out MergeKid);
				Delete deletion = DeleteChild.delete(key);
				MergeKid = deletion.MergeMe;
				String delresult = deletion.smallestKey;
				// delete succeeded... now fix up the child node if needed.
				BplusNode.this.Soil(); // redundant ?
				// bizarre special case for 2-3  or 3-4 trees -- empty leaf
				if (delresult!=null && BplusNode.this.owner.Compare(delresult, key)==0) // delresult.equals(key)
				{
					if (BplusNode.this.Size>3) 
					{
						throw new BplusTreeException("assertion error: delete returned delete key for too large node size: "+BplusNode.this.Size);
					}
					// junk this leaf and shift everything over
					if (deleteposition==0) 
					{
						this.smallestKey = BplusNode.this.ChildKeys[deleteposition];
					} 
					else if (deleteposition==BplusNode.this.Size) 
					{
						BplusNode.this.ChildKeys[deleteposition-1] = null;
					}
					else
					{
						BplusNode.this.ChildKeys[deleteposition-1] = BplusNode.this.ChildKeys[deleteposition];
					}
					if (this.smallestKey!=null && BplusNode.this.owner.Compare(this.smallestKey, key)==0) // result.equals(key)
					{
						// I'm not sure this ever happens
						BplusNode.this.MaterializeNodeAtIndex(1);
						this.smallestKey = BplusNode.this.MaterializedChildNodes[1].LeastKey();
					}
					DeleteChild.Free();
					for (int i=deleteposition; i<BplusNode.this.Size-1; i++) 
					{
						BplusNode.this.ChildKeys[i] = BplusNode.this.ChildKeys[i+1];
						BplusNode.this.MaterializedChildNodes[i] = BplusNode.this.MaterializedChildNodes[i+1];
						BplusNode.this.ChildBufferNumbers[i] = BplusNode.this.ChildBufferNumbers[i+1];
					}
					BplusNode.this.ChildKeys[BplusNode.this.Size-1] = null;
					if (deleteposition<BplusNode.this.Size) 
					{
						BplusNode.this.MaterializedChildNodes[BplusNode.this.Size-1] = BplusNode.this.MaterializedChildNodes[BplusNode.this.Size];
						BplusNode.this.ChildBufferNumbers[BplusNode.this.Size-1] = BplusNode.this.ChildBufferNumbers[BplusNode.this.Size];
					}
					BplusNode.this.MaterializedChildNodes[BplusNode.this.Size] = null;
					BplusNode.this.ChildBufferNumbers[BplusNode.this.Size] = BplusTreeLong.NULLBUFFERNUMBER;
					MergeMe = (BplusNode.this.SizeInUse()<BplusNode.this.Size/2);
					BplusNode.this.reParentAllChildren();
					//return result;
					return;
				}
				if (deleteposition==0) 
				{
					// smallest key may have changed.
					this.smallestKey = delresult;
				}
					// update key array if needed
				else if (delresult!=null && deleteposition>0) 
				{
					if (BplusNode.this.owner.Compare(delresult,key)!=0) // !delresult.equals(key)
					{
						BplusNode.this.ChildKeys[deleteposition-1] = delresult;
					} 
				}
				// if the child needs merging... do it
				if (MergeKid) 
				{
					int leftindex, rightindex;
					BplusNode leftNode;
					BplusNode rightNode;
					String keyBetween;
					if (deleteposition==0) 
					{
						// merge with next
						leftindex = deleteposition;
						rightindex = deleteposition+1;
						leftNode = DeleteChild;
						//keyBetween = this.ChildKeys[deleteposition];
						rightNode = BplusNode.this.MaterializeNodeAtIndex(rightindex);
					} 
					else 
					{
						// merge with previous
						leftindex = deleteposition-1;
						rightindex = deleteposition;
						leftNode = BplusNode.this.MaterializeNodeAtIndex(leftindex);
						//keyBetween = this.ChildKeys[deleteBufferNumber-1];
						rightNode = DeleteChild;
					}
					keyBetween = BplusNode.this.ChildKeys[leftindex];
					String rightLeastKey;
					boolean DeleteRight;
					rightLeastKey = Merge(leftNode, keyBetween, rightNode);//, out rightLeastKey, out DeleteRight);
					DeleteRight = !rightNode.isValid;
					// delete the right node if needed.
					if (DeleteRight) 
					{
						for (int i=rightindex; i<BplusNode.this.Size; i++) 
						{
							BplusNode.this.ChildKeys[i-1] = BplusNode.this.ChildKeys[i];
							BplusNode.this.ChildBufferNumbers[i] = BplusNode.this.ChildBufferNumbers[i+1];
							BplusNode.this.MaterializedChildNodes[i] = BplusNode.this.MaterializedChildNodes[i+1];
						}
						BplusNode.this.ChildKeys[BplusNode.this.Size-1] = null;
						BplusNode.this.MaterializedChildNodes[BplusNode.this.Size] = null;
						BplusNode.this.ChildBufferNumbers[BplusNode.this.Size] = BplusTreeLong.NULLBUFFERNUMBER;
						BplusNode.this.reParentAllChildren();
						rightNode.Free();
						// does this node need merging?
						if (BplusNode.this.SizeInUse()<BplusNode.this.Size/2) 
						{
							MergeMe = true;
						}
					} 
					else 
					{
						// update the key entry
						BplusNode.this.ChildKeys[rightindex-1] = rightLeastKey;
					}
				}
				//return result;
			}
			
			public String DeleteLeaf(String key) 
				throws Exception
			{
				this.smallestKey  = null;
				this.MergeMe = false;
				boolean found = false;
				int deletelocation = 0;
				//foreach (String thiskey in this.ChildKeys) 
				for (int i=0; i<BplusNode.this.ChildKeys.length; i++)
				{
					String thiskey = BplusNode.this.ChildKeys[i];
					// use comparison, not equals, in case different Strings sometimes compare same
					if (thiskey!=null && BplusNode.this.owner.Compare(thiskey, key)==0) // thiskey.equals(key)
					{
						found = true;
						deletelocation = i;
						break;
					}
					//deletelocation++;
				}
				if (!found) 
				{
					throw new BplusTreeKeyMissing("cannot delete missing key: "+key);
				}
				BplusNode.this.Soil();
				// only keys are important...
				for (int i=deletelocation; i<BplusNode.this.Size-1; i++) 
				{
					BplusNode.this.ChildKeys[i] = BplusNode.this.ChildKeys[i+1];
					BplusNode.this.ChildBufferNumbers[i] = BplusNode.this.ChildBufferNumbers[i+1];
				}
				BplusNode.this.ChildKeys[BplusNode.this.Size-1] = null;
				//this.MaterializedChildNodes[endlocation+1] = null;
				//this.ChildBufferNumbers[endlocation+1] = BplusTreeLong.NULLBUFFERNUMBER;
				if (BplusNode.this.SizeInUse()<BplusNode.this.Size/2)
				{
					this.MergeMe = true;
				}
				if (deletelocation==0) 
				{
					this.smallestKey  = BplusNode.this.ChildKeys[0];
					// this is only relevant for the case of 2-3 trees (empty leaf after deletion)
					if (this.smallestKey ==null) 
					{
						this.smallestKey = key; // deleted value
					}
				}
				return this.smallestKey;
			}
		}
		String LeastKey() 
			throws Exception
		{
			String result = null;
			if (this.isLeaf) 
			{
				result = this.ChildKeys[0];
			} 
			else 
			{
				this.MaterializeNodeAtIndex(0);
				result = this.MaterializedChildNodes[0].LeastKey();
			}
			if (result==null) 
			{
				throw new BplusTreeException("no key found");
			}
			return result;
		}
		public static String Merge(BplusNode left, String KeyBetween, BplusNode right) //, out String rightLeastKey, 
			//out boolean DeleteRight) 
			throws Exception
		{
			//System.Diagnostics.Debug.WriteLine("\r\n<br> merging "+right.myBufferNumber+" ("+KeyBetween+") "+left.myBufferNumber);
			//System.Diagnostics.Debug.WriteLine(left.owner.toHtml());
			//rightLeastKey = null; // only if DeleteRight
			if (left.isLeaf || right.isLeaf) 
			{
				if (!(left.isLeaf&&right.isLeaf)) 
				{
					throw new BplusTreeException("can't merge leaf with non-leaf");
				}
				MergeLeaves(left, right); //, out DeleteRight);
				String rightLeastKey = right.ChildKeys[0];
				return rightLeastKey;
			}
			// merge non-leaves
			//DeleteRight = false;
			String[] allkeys = new String[left.Size*2+1];
			long[] allseeks = new long[left.Size*2+2];
			BplusNode[] allMaterialized = new BplusNode[left.Size*2+2];
			if (left.ChildBufferNumbers[0]==BplusTreeLong.NULLBUFFERNUMBER ||
				right.ChildBufferNumbers[0]==BplusTreeLong.NULLBUFFERNUMBER) 
			{
				throw new BplusTreeException("cannot merge empty non-leaf with non-leaf");
			}
			int index = 0;
			allseeks[0] = left.ChildBufferNumbers[0];
			allMaterialized[0] = left.MaterializedChildNodes[0];
			for (int i=0; i<left.Size; i++) 
			{
				if (left.ChildKeys[i]==null) 
				{
					break;
				}
				allkeys[index] = left.ChildKeys[i];
				allseeks[index+1] = left.ChildBufferNumbers[i+1];
				allMaterialized[index+1] = left.MaterializedChildNodes[i+1];
				index++;
			}
			allkeys[index] = KeyBetween;
			index++;
			allseeks[index] = right.ChildBufferNumbers[0];
			allMaterialized[index] = right.MaterializedChildNodes[0];
			int rightcount = 0;
			for (int i=0; i<right.Size; i++) 
			{
				if (right.ChildKeys[i]==null) 
				{
					break;
				}
				allkeys[index] = right.ChildKeys[i];
				allseeks[index+1] = right.ChildBufferNumbers[i+1];
				allMaterialized[index+1] = right.MaterializedChildNodes[i+1];
				index++;
				rightcount++;
			}
			if (index<=left.Size) 
			{
				// it will all fit in one node
				//System.Diagnostics.Debug.WriteLine("deciding to forget "+right.myBufferNumber+" into "+left.myBufferNumber);
				//DeleteRight = true;
				right.isValid = false;
				for (int i=0; i<index; i++) 
				{
					left.ChildKeys[i] = allkeys[i];
					left.ChildBufferNumbers[i] = allseeks[i];
					left.MaterializedChildNodes[i] = allMaterialized[i];
				}
				left.ChildBufferNumbers[index] = allseeks[index];
				left.MaterializedChildNodes[index] = allMaterialized[index];
				left.reParentAllChildren();
				left.Soil();
				right.Free();
				return null;
			}
			// otherwise split the content between the nodes
			left.Clear();
			right.Clear();
			left.Soil();
			right.Soil();
			int leftcontent = index/2;
			int rightcontent = index-leftcontent-1;
			String rightLeastKey = allkeys[leftcontent];
			int outputindex = 0;
			for (int i=0; i<leftcontent; i++) 
			{
				left.ChildKeys[i] = allkeys[outputindex];
				left.ChildBufferNumbers[i] = allseeks[outputindex];
				left.MaterializedChildNodes[i] = allMaterialized[outputindex];
				outputindex++;
			}
			rightLeastKey = allkeys[outputindex];
			left.ChildBufferNumbers[outputindex] = allseeks[outputindex];
			left.MaterializedChildNodes[outputindex] = allMaterialized[outputindex];
			outputindex++;
			rightcount = 0;
			for (int i=0; i<rightcontent; i++) 
			{
				right.ChildKeys[i] = allkeys[outputindex];
				right.ChildBufferNumbers[i] = allseeks[outputindex];
				right.MaterializedChildNodes[i] = allMaterialized[outputindex];
				outputindex++;
				rightcount++;
			}
			right.ChildBufferNumbers[rightcount] = allseeks[outputindex];
			right.MaterializedChildNodes[rightcount] = allMaterialized[outputindex];
			left.reParentAllChildren();
			right.reParentAllChildren();
			return rightLeastKey;
		}
		public static void MergeLeaves(BplusNode left, BplusNode right) //, out boolean DeleteRight) 
			throws Exception
		{
			//DeleteRight = false;
			String[] allkeys = new String[left.Size*2];
			long[] allseeks = new long[left.Size*2];
			int index = 0;
			for (int i=0; i<left.Size; i++) 
			{
				if (left.ChildKeys[i]==null) 
				{
					break;
				}
				allkeys[index] = left.ChildKeys[i];
				allseeks[index] = left.ChildBufferNumbers[i];
				index++;
			}
			for (int i=0; i<right.Size; i++) 
			{
				if (right.ChildKeys[i]==null) 
				{
					break;
				}
				allkeys[index] = right.ChildKeys[i];
				allseeks[index] = right.ChildBufferNumbers[i];
				index++;
			}
			if (index<=left.Size) 
			{
				left.Clear();
				//DeleteRight = true;
				right.isValid = false;
				for (int i=0; i<index; i++) 
				{
					left.ChildKeys[i] = allkeys[i];
					left.ChildBufferNumbers[i] = allseeks[i];
				}
				right.Free();
				left.Soil();
				return;
			}
			left.Clear();
			right.Clear();
			left.Soil();
			right.Soil();
			int rightcontent = index/2;
			int leftcontent = index - rightcontent;
			int newindex = 0;
			for (int i=0; i<leftcontent; i++) 
			{
				left.ChildKeys[i] = allkeys[newindex];
				left.ChildBufferNumbers[i] = allseeks[newindex];
				newindex++;
			}
			for (int i=0; i<rightcontent; i++) 
			{
				right.ChildKeys[i] = allkeys[newindex];
				right.ChildBufferNumbers[i] = allseeks[newindex];
				newindex++;
			}
		}
		//		public String DeleteLeaf(String key, out boolean MergeMe) 
		//		{
		//			String result = null;
		//			MergeMe = false;
		//			boolean found = false;
		//			int deletelocation = 0;
		//			foreach (String thiskey in this.ChildKeys) 
		//													 {
		//														 // use comparison, not equals, in case different Strings sometimes compare same
		//														 if (thiskey!=null && this.owner.Compare(thiskey, key)==0) // thiskey.equals(key)
		//														 {
		//															 found = true;
		//															 break;
		//														 }
		//														 deletelocation++;
		//													 }
		//			if (!found) 
		//			{
		//				throw new BplusTreeKeyMissing("cannot delete missing key: "+key);
		//			}
		//			this.Soil();
		//			// only keys are important...
		//			for (int i=deletelocation; i<this.Size-1; i++) 
		//			{
		//				this.ChildKeys[i] = this.ChildKeys[i+1];
		//				this.ChildBufferNumbers[i] = this.ChildBufferNumbers[i+1];
		//			}
		//			this.ChildKeys[this.Size-1] = null;
		//			//this.MaterializedChildNodes[endlocation+1] = null;
		//			//this.ChildBufferNumbers[endlocation+1] = BplusTreeLong.NULLBUFFERNUMBER;
		//			if (this.SizeInUse()<this.Size/2)
		//			{
		//				MergeMe = true;
		//			}
		//			if (deletelocation==0) 
		//			{
		//				result = this.ChildKeys[0];
		//				// this is only relevant for the case of 2-3 trees (empty leaf after deletion)
		//				if (result==null) 
		//				{
		//					result = key; // deleted value
		//				}
		//			}
		//			return result;
		//		}
		/// <summary>
		/// insert key/position entry in self 
		/// </summary>
		/// <param name="key">Key to associate with the leaf</param>
		/// <param name="position">position associated with key in external structur</param>
		/// <param name="splitString">if not null then the smallest key in the new split leaf</param>
		/// <param name="splitNode">if not null then the node was split and this is the leaf to the right.</param>
		/// <returns>null unless the smallest key under this node has changed, in which case it returns the smallest key.</returns>
		public String Insert(String key, long position) //, out String splitString, out BplusNode splitNode) 
			throws Exception
		{
			this.splitString = null;
			this.splitNode = null;
			if (this.isLeaf) 
			{
				return this.InsertLeaf(key, position); //, out splitString, out splitNode);
			}
			int insertposition = this.FindAtOrNextPosition(key, false);
			long insertBufferNumber = this.ChildBufferNumbers[insertposition];
			if (insertBufferNumber==BplusTreeLong.NULLBUFFERNUMBER) 
			{
				throw new BplusTreeException("key not followed by buffer number in non-leaf");
			}
			// insert in subtree
			BplusNode InsertChild = this.MaterializeNodeAtIndex(insertposition);
			BplusNode childSplit;
			String childSplitString;
			String childInsert = InsertChild.Insert(key, position); //, out childSplitString, out childSplit);
			childSplit = InsertChild.splitNode;
			childSplitString = InsertChild.splitString;
			InsertChild.splitNode = null;
			InsertChild.splitString = null;
			// if there was a split the node must expand
			if (childSplit!=null) 
			{
				// insert the child
				this.Soil(); // redundant -- a child will have a change so this node will need to be copied
				int newChildPosition = insertposition+1;
				boolean dosplit = false;
				// if there is no free space we must do a split
				if (this.ChildBufferNumbers[this.Size]!=BplusTreeLong.NULLBUFFERNUMBER) 
				{
					dosplit = true;
					this.PrepareForSplit();
				}
				// bubble over the current values to make space for new child
				for (int i=this.ChildKeys.length-2; i>=newChildPosition-1; i--) 
				{
					int i1 = i+1;
					int i2 = i1+1;
					this.ChildKeys[i1] = this.ChildKeys[i];
					this.ChildBufferNumbers[i2] = this.ChildBufferNumbers[i1];
					BplusNode childNode = this.MaterializedChildNodes[i2] = this.MaterializedChildNodes[i1];
				}
				// record the new child
				this.ChildKeys[newChildPosition-1] = childSplitString;
				//this.MaterializedChildNodes[newChildPosition] = childSplit;
				//this.ChildBufferNumbers[newChildPosition] = childSplit.myBufferNumber;
				childSplit.Reparent(this, newChildPosition);
				// split, if needed
				if (dosplit) 
				{
					int splitpoint = this.MaterializedChildNodes.length/2-1;
					this.splitString = this.ChildKeys[splitpoint];
					this.splitNode = new BplusNode(this.owner, this.parent, -1, this.isLeaf);
					// make copy of expanded node structure
					BplusNode[] materialized = this.MaterializedChildNodes;
					long[] buffernumbers = this.ChildBufferNumbers;
					String[] keys = this.ChildKeys;
					// repair the expanded node
					this.ChildKeys = new String[this.Size];
					this.MaterializedChildNodes = new BplusNode[this.Size+1];
					this.ChildBufferNumbers = new long[this.Size+1];
					this.Clear();
					//Array.Copy(materialized, 0, this.MaterializedChildNodes, 0, splitpoint+1);
					//Array.Copy(buffernumbers, 0, this.ChildBufferNumbers, 0, splitpoint+1);
					for (int i=0; i<splitpoint+1; i++) 
					{
						this.MaterializedChildNodes[i] = materialized[i];
						this.ChildBufferNumbers[i] = buffernumbers[i];
					}
					//Array.Copy(keys, 0, this.ChildKeys, 0, splitpoint);
					for (int i=0; i<splitpoint; i++) 
					{
						this.ChildKeys[i] = keys[i];
					}
					// initialize the new node
					splitNode.Clear(); // redundant.
					int remainingKeys = this.Size-splitpoint;
					//Array.Copy(materialized, splitpoint+1, splitNode.MaterializedChildNodes, 0, remainingKeys+1);
					//Array.Copy(buffernumbers, splitpoint+1, splitNode.ChildBufferNumbers, 0, remainingKeys+1);
					for (int i=0; i<remainingKeys+1; i++) 
					{
						splitNode.MaterializedChildNodes[i] = materialized[i+splitpoint+1];
						splitNode.ChildBufferNumbers[i] = buffernumbers[i+splitpoint+1];
					}
					//Array.Copy(keys, splitpoint+1, splitNode.ChildKeys, 0, remainingKeys);
					for (int i=0; i<remainingKeys; i++) 
					{
						splitNode.ChildKeys[i] = keys[i+splitpoint+1];
					}
					// fix pointers in materialized children of splitnode
					splitNode.reParentAllChildren();
					// store the new node
					splitNode.DumpToFreshBuffer();
					splitNode.CheckIfTerminal();
					splitNode.Soil();
					this.CheckIfTerminal();
				}
				// fix pointers in children
				this.reParentAllChildren();
			}
			if (insertposition==0) 
			{
				// the smallest key may have changed
				return childInsert;
			}
			return null;  // no change in smallest key
		}
		/// <summary>
		/// Check to see if this is a terminal node, if so record it, otherwise forget it
		/// </summary>
		void CheckIfTerminal() 
			throws Exception
		{
			if (!this.isLeaf) 
			{
				for (int i=0; i<this.Size+1; i++) 
				{
					if (this.MaterializedChildNodes[i]!=null) 
					{
						this.owner.ForgetTerminalNode(this);
						return;
					}
				}
			}
			this.owner.RecordTerminalNode(this);
		}
		/// <summary>
		/// insert key/position entry in self (as leaf)
		/// </summary>
		/// <param name="key">Key to associate with the leaf</param>
		/// <param name="position">position associated with key in external structure</param>
		/// <param name="splitString">if not null then the smallest key in the new split leaf</param>
		/// <param name="splitNode">if not null then the node was split and this is the leaf to the right.</param>
		/// <returns>smallest key value in keys, or null if no change</returns>
		public String InsertLeaf(String key, long position) //, out String splitString, out BplusNode splitNode) 
			throws Exception
		{
			this.splitString = null;
			this.splitNode = null;
			boolean dosplit = false;
			if (!this.isLeaf) 
			{
				throw new BplusTreeException("bad call to InsertLeaf: this is not a leaf");
			}
			this.Soil();
			int insertposition = this.FindAtOrNextPosition(key, false);
			if (insertposition>=this.Size) 
			{
				//throw new BplusTreeException("key too big and leaf is full");
				dosplit = true;
				this.PrepareForSplit();
			} 
			else 
			{
				// if it's already there then change the value at the current location (duplicate entries not supported).
				if (this.ChildKeys[insertposition]==null || this.owner.Compare(this.ChildKeys[insertposition], key)==0) // this.ChildKeys[insertposition].equals(key)
				{
					this.ChildBufferNumbers[insertposition] = position;
					this.ChildKeys[insertposition] = key;
					if (insertposition==0) 
					{
						return key;
					} 
					else 
					{
						return null;
					}
				}
			}
			// check for a null position
			int nullindex = insertposition;
			while (nullindex<this.ChildKeys.length && this.ChildKeys[nullindex]!=null) 
			{
				nullindex++;
			}
			if (nullindex>=this.ChildKeys.length) 
			{
				if (dosplit) 
				{
					throw new BplusTreeException("can't split twice!!");
				}
				//throw new BplusTreeException("no space in leaf");
				dosplit = true;
				this.PrepareForSplit();
			}
			// bubble in the new info XXXX THIS SHOULD BUBBLE BACKWARDS	
			String nextkey = this.ChildKeys[insertposition];
			long nextposition = this.ChildBufferNumbers[insertposition];
			this.ChildKeys[insertposition] = key;
			this.ChildBufferNumbers[insertposition] = position;
			while (nextkey!=null) 
			{
				key = nextkey;
				position = nextposition;
				insertposition++;
				nextkey = this.ChildKeys[insertposition];
				nextposition = this.ChildBufferNumbers[insertposition];
				this.ChildKeys[insertposition] = key;
				this.ChildBufferNumbers[insertposition] = position;
			}
			// split if needed
			if (dosplit) 
			{
				int splitpoint = this.ChildKeys.length/2;
				int splitlength = this.ChildKeys.length - splitpoint;
				splitNode = new BplusNode(this.owner, this.parent, -1, this.isLeaf);
				// copy the split info into the splitNode
				//Array.Copy(this.ChildBufferNumbers, splitpoint, splitNode.ChildBufferNumbers, 0, splitlength);
				//Array.Copy(this.ChildKeys, splitpoint, splitNode.ChildKeys, 0, splitlength);
				//Array.Copy(this.MaterializedChildNodes, splitpoint, splitNode.MaterializedChildNodes, 0, splitlength);
				for (int i=0; i<splitlength; i++) 
				{
					splitNode.ChildBufferNumbers[i] = this.ChildBufferNumbers[i+splitpoint];
					splitNode.ChildKeys[i] = this.ChildKeys[i+splitpoint];
					splitNode.MaterializedChildNodes[i] = this.MaterializedChildNodes[i+splitpoint];
				}
				splitString = splitNode.ChildKeys[0];
				// archive the new node
				splitNode.DumpToFreshBuffer();
				// store the node data temporarily
				long[] buffernumbers = this.ChildBufferNumbers;
				String[] keys = this.ChildKeys;
				BplusNode[] nodes = this.MaterializedChildNodes;
				// repair current node, copy in the other part of the split
				this.ChildBufferNumbers = new long[this.Size+1];
				this.ChildKeys = new String[this.Size];
				this.MaterializedChildNodes = new BplusNode[this.Size+1];
				//Array.Copy(buffernumbers, 0, this.ChildBufferNumbers, 0, splitpoint);
				//Array.Copy(keys, 0, this.ChildKeys, 0, splitpoint);
				//Array.Copy(nodes, 0, this.MaterializedChildNodes, 0, splitpoint);
				for (int i=0; i<splitpoint; i++) 
				{
					this.ChildBufferNumbers[i] = buffernumbers[i];
					this.ChildKeys[i] = keys[i];
					this.MaterializedChildNodes[i] = nodes[i];
				}
				for (int i=splitpoint; i<this.ChildKeys.length; i++) 
				{
					this.ChildKeys[i] = null;
					this.ChildBufferNumbers[i] = BplusTreeLong.NULLBUFFERNUMBER;
					this.MaterializedChildNodes[i] = null;
				}
				// store the new node
				//splitNode.DumpToFreshBuffer();
				this.owner.RecordTerminalNode(splitNode);
				splitNode.Soil();
			}
			//return this.ChildKeys[0];
			if (insertposition==0) 
			{
				return key; // smallest key changed.
			} 
			else 
			{
				return null; // no change in smallest key
			}
		}
		/// <summary>
		/// Grow to this.size+1 in preparation for insertion and split
		/// </summary>
		void PrepareForSplit() 
			throws Exception
		{
			int supersize = this.Size+1;
			long[] positions = new long[supersize+1];
			String[] keys = new String[supersize];
			BplusNode[] materialized = new BplusNode[supersize+1];
			for (int i=0; i<this.Size; i++) 
			{
				keys[i] = this.ChildKeys[i];
				positions[i] = this.ChildBufferNumbers[i];
				materialized[i] = this.MaterializedChildNodes[i];
			}
			//Array.Copy(this.ChildBufferNumbers, 0, positions, 0, this.Size+1);
			positions[this.Size] = this.ChildBufferNumbers[this.Size];
			positions[this.Size+1] = BplusTreeLong.NULLBUFFERNUMBER;
			//Array.Copy(this.ChildKeys, 0, keys, 0, this.Size);
			keys[this.Size] = null;
			//Array.Copy(this.MaterializedChildNodes, 0, materialized, 0, this.Size+1);
			materialized[this.Size] = this.MaterializedChildNodes[this.Size];
			materialized[this.Size+1] = null;
			this.ChildBufferNumbers = positions;
			this.ChildKeys = keys;
			this.MaterializedChildNodes = materialized;
		}
		public void Load(byte[] buffer) 
			throws Exception
		{
			// load serialized data
			// indicator | seek position | [ key storage | seek position ]*
			this.Clear();
			if (buffer.length!=owner.buffersize) 
			{
				throw new BplusTreeException("bad buffer size "+buffer.length+" should be "+owner.buffersize);
			}
			byte indicator = buffer[0];
			this.isLeaf = false;
			if (indicator==BplusTreeLong.LEAF) 
			{
				this.isLeaf = true;
			} 
			else if (indicator!=BplusTreeLong.NONLEAF) 
			{
				throw new BplusTreeException("bad indicator, not leaf or nonleaf in tree "+indicator);
			}
			int index = 1;
			// get the first seek position
			this.ChildBufferNumbers[0] = BufferFile.RetrieveLong(buffer, index);
			//System.Text.Decoder decode = System.Text.Encoding.UTF8.GetDecoder();
			index+= BufferFile.LONGSTORAGE;
			int maxKeyLength = this.owner.KeyLength;
			int maxKeyPayload = maxKeyLength - BufferFile.SHORTSTORAGE;
			//this.NumberOfValidKids = 0;
			// get remaining key storages and seek positions
			String lastkey = "";
			for (int KeyIndex=0; KeyIndex<this.Size; KeyIndex++) 
			{
				// decode and store a key
				short keylength = BufferFile.RetrieveShort(buffer, index);
				if (keylength<-1 || keylength>maxKeyPayload) 
				{
					throw new BplusTreeException("invalid keylength decoded");
				}
				index+= BufferFile.SHORTSTORAGE;
				String key = null;
				if (keylength==0) 
				{
					key = "";
				}
				if (keylength>0) 
				{
					//int charCount = decode.GetCharCount(buffer, index, keylength);
					//char[] ca = new char[charCount];
					//decode.GetChars(buffer, index, keylength, ca, 0);
					//this.NumberOfValidKids++;
					key = new String(buffer, index, keylength, "UTF-8");
				}
				this.ChildKeys[KeyIndex] = key;
				index+= maxKeyPayload;
				// decode and store a seek position
				long seekPosition = BufferFile.RetrieveLong(buffer, index);
				if (!this.isLeaf) 
				{
					if (key==null & seekPosition!=BplusTreeLong.NULLBUFFERNUMBER) 
					{
						throw new BplusTreeException("key is null but position is not "+KeyIndex);
					} 
					else if (lastkey==null && key!=null) 
					{
						throw new BplusTreeException("null key followed by non-null key "+KeyIndex);
					}
				}
				lastkey = key;
				this.ChildBufferNumbers[KeyIndex+1] = seekPosition;
				index+= BufferFile.LONGSTORAGE;
			}
		}
		/// <summary>
		/// check that key is ok for node of this size (put here for locality of relevant code).
		/// </summary>
		/// <param name="key">key to check</param>
		/// <param name="owner">tree to contain node containing the key</param>
		/// <returns>true if key is ok</returns>
		public static boolean KeyOK(String key, BplusTreeLong owner) 
			throws Exception
		{
			if (key==null) 
			{ 
				return false;
			}
			//System.Text.Encoder encode = System.Text.Encoding.UTF8.GetEncoder();
			int maxKeyLength = owner.KeyLength;
			int maxKeyPayload = maxKeyLength - BufferFile.SHORTSTORAGE;
			//char[] keyChars = key.ToCharArray();
			//int charCount = encode.GetByteCount(keyChars, 0, keyChars.length, true);
			byte[] keyBytes = key.getBytes("UTF-8");
			int charCount = keyBytes.length;
			if (charCount>maxKeyPayload) 
			{
				return false;
			}
			return true;
		}
		public void Dump(byte[] buffer) 
			throws Exception
		{
			// indicator | seek position | [ key storage | seek position ]*
			if (buffer.length!=owner.buffersize) 
			{
				throw new BplusTreeException("bad buffer size "+buffer.length+" should be "+owner.buffersize);
			}
			buffer[0] = BplusTreeLong.NONLEAF;
			if (this.isLeaf) { buffer[0] = BplusTreeLong.LEAF; }
			int index = 1;
			// store first seek position
			BufferFile.Store(this.ChildBufferNumbers[0], buffer, index);
			index+= BufferFile.LONGSTORAGE;
			//System.Text.Encoder encode = System.Text.Encoding.UTF8.GetEncoder();
			// store remaining keys and seeks
			int maxKeyLength = this.owner.KeyLength;
			int maxKeyPayload = maxKeyLength - BufferFile.SHORTSTORAGE;
			String lastkey = "";
			for (int KeyIndex=0; KeyIndex<this.Size; KeyIndex++) 
			{
				// store a key
				String theKey = this.ChildKeys[KeyIndex];
				short charCount = -1;
				if (theKey!=null) 
				{
					byte[] keyBytes = theKey.getBytes("UTF-8");
					//charCount = (short) encode.GetByteCount(keyChars, 0, keyChars.length, true);
					charCount = (short) keyBytes.length;
					if (charCount>maxKeyPayload) 
					{
						throw new BplusTreeException("String bytes to large for use as key "+charCount+">"+maxKeyPayload);
					}
					BufferFile.Store(charCount, buffer, index);
					index+= BufferFile.SHORTSTORAGE;
					//encode.GetBytes(keyChars, 0, keyChars.length, buffer, index, true);
					for (int i=0; i<keyBytes.length; i++) 
					{
						buffer[index+i] = keyBytes[i];
					}
				} 
				else 
				{
					// null case (no String to read)
					BufferFile.Store(charCount, buffer, index);
					index+= BufferFile.SHORTSTORAGE;
				}
				index+= maxKeyPayload;
				// store a seek
				long seekPosition = this.ChildBufferNumbers[KeyIndex+1];
				if (theKey==null && seekPosition!=BplusTreeLong.NULLBUFFERNUMBER && !this.isLeaf) 
				{
					throw new BplusTreeException("null key paired with non-null location "+KeyIndex);
				}
				if (lastkey==null && theKey!=null) 
				{
					throw new BplusTreeException("null key followed by non-null key "+KeyIndex);
				}
				lastkey = theKey;
				BufferFile.Store(seekPosition, buffer, index);
				index+= BufferFile.LONGSTORAGE;
			}
		}
		/// <summary>
		/// Close the node:
		/// invalidate all children, store state if needed, remove materialized self from parent.
		/// </summary>
		public long Invalidate(boolean destroyRoot) 
			
			throws Exception
		{
			long result = this.myBufferNumber;
			if (!this.isLeaf) 
			{
				// need to invalidate kids
				for (int i=0; i<this.Size+1; i++) 
				{
					if (this.MaterializedChildNodes[i]!=null) 
					{
						// new buffer numbers are recorded automatically
						this.ChildBufferNumbers[i] = this.MaterializedChildNodes[i].Invalidate(true);
					}
				}
			} 
			// store if dirty
			if (this.Dirty) 
			{
				result = this.DumpToFreshBuffer();
				//				result = this.myBufferNumber;
			}
			// remove from owner archives if present
			this.owner.ForgetTerminalNode(this);
			// remove from parent
			if (this.parent!=null && this.indexInParent>=0) 
			{
				this.parent.MaterializedChildNodes[this.indexInParent] = null;
				this.parent.ChildBufferNumbers[this.indexInParent] = result; // should be redundant
				this.parent.CheckIfTerminal();
				this.indexInParent = -1;
			}
			// render all structures useless, just in case...
			if (destroyRoot) 
			{
				this.Destroy();
			}
			return result;
		}
		/// <summary>
		/// Mark this as dirty and all ancestors too.
		/// </summary>
		void Soil() 
			throws Exception
		{
			if (this.Dirty) 
			{
				return; // don't need to do it again
			} 
			else 
			{
				this.Dirty = true;
				if (this.parent!=null) 
				{
					this.parent.Soil();
				}
			}
		}
		public void AsHtml(java.io.CharArrayWriter sb) throws Exception
		{
			String hygeine = "clean";
			if (this.Dirty) { hygeine = "dirty"; }
			int keycount = 0;
			if (this.isLeaf) 
			{
				for (int i=0; i<this.Size; i++) 
				{
					String key = this.ChildKeys[i];
					long seek = this.ChildBufferNumbers[i];
					if (key!=null) 
					{
						key = PrintableString(key);
						sb.write("'"+key+"' : "+seek+"<br>\r\n");
						keycount++;
					}
				}
				sb.write("leaf "+this.indexInParent+" at "+this.myBufferNumber+" #keys=="+keycount+" "+hygeine+"\r\n");
			} 
			else 
			{
				sb.write("<table border>\r\n");
				sb.write("<tr><td colspan=2>nonleaf "+this.indexInParent+" at "+this.myBufferNumber+" "+hygeine+"</td></tr>\r\n");
				if (this.ChildBufferNumbers[0]!=BplusTreeLong.NULLBUFFERNUMBER) 
				{
					this.MaterializeNodeAtIndex(0);
					sb.write("<tr><td></td><td>"+this.ChildBufferNumbers[0]+"</td><td>\r\n");
					this.MaterializedChildNodes[0].AsHtml(sb);
					sb.write("</td></tr>\r\n");
				}
				for (int i=0; i<this.Size; i++) 
				{
					String key =  this.ChildKeys[i];
					if (key==null) 
					{
						break;
					}
					key = PrintableString(key);
					sb.write("<tr><th>'"+key+"'</th><td></td><td></td></tr>\r\n");
					try 
					{
						this.MaterializeNodeAtIndex(i+1);
						sb.write("<tr><td></td><td>"+this.ChildBufferNumbers[i+1]+"</td><td>\r\n");
						this.MaterializedChildNodes[i+1].AsHtml(sb);
						sb.write("</td></tr>\r\n");
					} 
					catch (BplusTreeException e) 
					{
						sb.write("<tr><td></td><th>COULDN'T MATERIALIZE NODE "+(i+1)+"</th></tr>");
					}
					keycount++;
				}
				sb.write("<tr><td colspan=2> #keys=="+keycount+"</td></tr>\r\n");
				sb.write("</table>\r\n");
			}
		}
		public static String PrintableString(String s) throws Exception
		{
			if (s==null) { return "[NULL]"; }
			//System.Text.StringBuilder sb = new System.Text.StringBuilder();
			String result = "";
			//foreach (char c in s) 
			for (int i=0; i<s.length(); i++)
			{
				char c = s.charAt(i);
				if (Character.isLetterOrDigit(c) || c=='-' || c=='.') 
				{
					//sb.write(c);
					result += c;
				} 
				else 
				{
					//sb.write("["+Convert.ToInt32(c)+"]");
					result += "["+Character.getNumericValue(c)+"]";
				}
			}
			return result;
		}
	}

	//	/// <summary>
	//	/// Generic error including programming errors.
	//	/// </summary>
	//	public class BplusTreeException: ApplicationException 
	//	{
	//		public BplusTreeException(String message): base(message) 
	//		{
	//			// do nothing extra
	//		}
	//	}
	//	/// <summary>
	//	/// No such key found for attempted retrieval.
	//	/// </summary>
	//	public class BplusTreeKeyMissing: ApplicationException 
	//	{
	//		public BplusTreeKeyMissing(String message): base(message) 
	//		{
	//			// do nothing extra
	//		}
	//	}
	//	/// <summary>
	//	/// Key cannot be null or too large.
	//	/// </summary>
	//	public class BplusTreeBadKeyValue: ApplicationException 
	//	{
	//		public BplusTreeBadKeyValue(String message): base(message) 
	//		{
	//			// do nothing extra
	//		}
	//	}
}
