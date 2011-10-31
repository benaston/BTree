package NET.sourceforge.BplusJ.BplusJ;

import java.util.*;

	/// <summary>
	/// Summary description for BplusTreeBytes.
	/// </summary>
	public class BplusTreeBytes implements IByteTree
	{
		BplusTreeLong tree;
		LinkedFile archive;
		Hashtable FreeChunksOnCommit = new Hashtable();
		Hashtable FreeChunksOnAbort = new Hashtable();
		static int DEFAULTBLOCKSIZE = 1024;
		static int DEFAULTNODESIZE = 32;
		public BplusTreeBytes(BplusTreeLong tree, LinkedFile archive)
		{
			this.tree = tree;
			this.archive = archive;
		}

		public static java.io.RandomAccessFile makeFile(String path) throws Exception
		{
			java.io.File f = new java.io.File(path);
			if (f.exists()) 
			{
				//System.out.println("<br>				DELETING FILE "+path);
				f.delete();
			}
			return new java.io.RandomAccessFile(path, "rw");
		}

		public static BplusTreeBytes Initialize(String treefileName, String blockfileName, int KeyLength, int CultureId,
			int nodesize, int buffersize) 
			throws Exception
		{
			//java.io.RandomAccessFile treefile = new System.IO.FileStream(treefileName, System.IO.FileMode.CreateNew, 
			//	System.IO.FileAccess.ReadWrite);
			//java.io.RandomAccessFile blockfile = new System.IO.FileStream(blockfileName, System.IO.FileMode.CreateNew, 
			//	System.IO.FileAccess.ReadWrite);
			java.io.RandomAccessFile treefile = makeFile(treefileName);
			java.io.RandomAccessFile blockfile = makeFile(blockfileName);
			return Initialize(treefile, blockfile, KeyLength, CultureId, nodesize, buffersize);
		}
		public static BplusTreeBytes Initialize(String treefileName, String blockfileName, int KeyLength, int CultureId) 
			throws Exception
		{
			java.io.RandomAccessFile treefile = makeFile(treefileName);
			java.io.RandomAccessFile blockfile = makeFile(blockfileName);
			return Initialize(treefile, blockfile, KeyLength, CultureId);
		}
		public static BplusTreeBytes Initialize(String treefileName, String blockfileName, int KeyLength) 
			throws Exception
		{
			java.io.RandomAccessFile treefile = makeFile(treefileName);
			java.io.RandomAccessFile blockfile = makeFile(blockfileName);
			return Initialize(treefile, blockfile, KeyLength);
		}
		public static BplusTreeBytes Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int KeyLength, int CultureId,
			int nodesize, int buffersize) 
			throws Exception
		{
			BplusTreeLong tree = BplusTreeLong.InitializeInStream(treefile, KeyLength, nodesize, CultureId);
			LinkedFile archive = LinkedFile.InitializeLinkedFileInStream(blockfile, buffersize);
			return new BplusTreeBytes(tree, archive);
		}
		public static BplusTreeBytes Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int KeyLength, int CultureId) 
			throws Exception
		{
			return Initialize(treefile, blockfile, KeyLength, CultureId, DEFAULTNODESIZE, DEFAULTBLOCKSIZE);
		}
		public static BplusTreeBytes Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int KeyLength) 
			throws Exception
		{
			int CultureId = BplusTreeLong.INVARIANTCULTUREID;
			return Initialize(treefile, blockfile, KeyLength, CultureId, DEFAULTNODESIZE, DEFAULTBLOCKSIZE);
		}
		public static BplusTreeBytes ReOpen(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile) throws Exception
		{
			BplusTreeLong tree = BplusTreeLong.SetupFromExistingStream(treefile);
			LinkedFile archive = LinkedFile.SetupFromExistingStream(blockfile);
			return new BplusTreeBytes(tree, archive);
		}
		public static BplusTreeBytes ReOpen(String treefileName, String blockfileName, String access) throws Exception
		{
			//java.io.RandomAccessFile treefile = new System.IO.FileStream(treefileName, System.IO.FileMode.Open, 
			//	access);
			//java.io.RandomAccessFile blockfile = new System.IO.FileStream(blockfileName, System.IO.FileMode.Open, 
			//	access);
			java.io.RandomAccessFile treefile = new java.io.RandomAccessFile(treefileName, access);
			java.io.RandomAccessFile blockfile = new java.io.RandomAccessFile(blockfileName, access);
			return ReOpen(treefile, blockfile);
		}
		public static BplusTreeBytes ReOpen(String treefileName, String blockfileName) throws Exception
		{
			return ReOpen(treefileName, blockfileName, "rw");
		}
		public static BplusTreeBytes ReadOnly(String treefileName, String blockfileName) throws Exception
		{
			return ReOpen(treefileName, blockfileName, "r");
		}

		/// <summary>
		/// Use non-culture sensitive total order on binary Strings.
		/// </summary>
		public void NoCulture() 
		{
			// not relevant to java implementation currently.
			//this.tree.DontUseCulture = true;
			//this.tree.cultureContext = null;
		}
		public int MaxKeyLength() throws Exception
		{
			return this.tree.MaxKeyLength();
		}
//		#region ITreeIndex Members

		
		public int Compare(String left, String right) throws Exception
		{
			return this.tree.Compare(left, right);
		}
		public void Shutdown() throws Exception
		{
			this.tree.Shutdown();
			this.archive.Shutdown();
		}

		public void Recover(boolean CorrectErrors) throws Exception
		{
			this.tree.Recover(CorrectErrors);
			Hashtable ChunksInUse = new Hashtable();
			String key = this.tree.FirstKey();
			while (key!=null) 
			{
				Long buffernumber = new Long(this.tree.get(key));
				if (ChunksInUse.containsKey(buffernumber)) 
				{
					throw new BplusTreeException("buffer number "+buffernumber+" associated with more than one key '"
						+key+"' and '"+ChunksInUse.get(buffernumber)+"'");
				}
				//ChunksInUse[buffernumber] = key;
				ChunksInUse.put(buffernumber, key);
				key = this.tree.NextKey(key);
			}
			// also consider the un-deallocated chunks to be in use
			//foreach (Object thing in this.FreeChunksOnCommit)
			//for (int i=0; i<this.FreeChunksOnCommit.size(); i++)
			for (Enumeration e=this.FreeChunksOnCommit.keys(); e.hasMoreElements(); )
			{
				Long buffernumber = (Long) e.nextElement();
				//ChunksInUse[buffernumber] = "awaiting commit";
				ChunksInUse.put(buffernumber, "awaiting commit");
			}
			this.archive.Recover(ChunksInUse, CorrectErrors);
		}

		public void RemoveKey(String key) throws Exception
		{
			long map = this.tree.get(key);
			//this.archive.ReleaseBuffers(map);
			//this.FreeChunksOnCommit.Add(map);
			Long M = new Long(map);
			if (this.FreeChunksOnAbort.containsKey(M)) 
			{
				// free it now
				this.FreeChunksOnAbort.remove(M);
				this.archive.ReleaseBuffers(map);
			} 
			else 
			{
				// free when committed
				this.FreeChunksOnCommit.put(M,M);
			}
			this.tree.RemoveKey(key);
		}

		public String FirstKey() throws Exception
		{
			return this.tree.FirstKey();
		}

		public String NextKey(String AfterThisKey) throws Exception
		{
			return this.tree.NextKey(AfterThisKey);
		}

		public boolean ContainsKey(String key) throws Exception
		{
			return this.tree.ContainsKey(key);
		}

		public Object Get(String key, Object defaultValue) throws Exception
		{
			long map;
			if (this.tree.ContainsKey(key)) 
			{
				map = this.tree.LastValueFound;
				return (Object) this.archive.GetChunk(map);
			}
			return defaultValue;
		}

		public void Set(String key, Object map) throws Exception
		{
//			if (!(map is byte[]) )
//			{
//				throw new BplusTreeBadKeyValue("BplusTreeBytes can only archive byte array as value");
//			}
			byte[] thebytes = (byte[]) map;
			//this[key] = thebytes;
			this.set(key, thebytes);
		}
		public void set (String key, byte[] value) throws Exception
		{
			long storage = this.archive.StoreNewChunk(value, 0, value.length);
			//this.FreeChunksOnAbort.add(new Long(storage));
			Long S = new Long(storage);
			this.FreeChunksOnAbort.put(S, S);
			long valueFound;
			if (this.tree.ContainsKey(key)) 
			{
				valueFound = this.tree.LastValueFound;
				//this.archive.ReleaseBuffers(valueFound);
				Long F = new Long(valueFound);
				if (this.FreeChunksOnAbort.containsKey(F))
				{
					// free it now
					this.FreeChunksOnAbort.remove(F);
					this.archive.ReleaseBuffers(valueFound);
				}
				else
				{
					this.FreeChunksOnCommit.put(F, F);
				}
			}
			//this.tree[key] = storage;
			this.tree.set(key, storage);
		}
		public byte[] get(String key) throws Exception
		{
			long map = this.tree.get(key);
			return this.archive.GetChunk(map);
		}

		public void Commit() throws Exception
		{
			// store all new bufferrs
			this.archive.Flush();
			// commit the tree
			this.tree.Commit();
			// at this point the new buffers have been committed, now free the old ones
			//this.FreeChunksOnCommit.Sort();
			//this.OnCommit.Reverse();
			//foreach (Object thing in this.FreeChunksOnCommit) 
			//for (int i=0; i<this.FreeChunksOnCommit.size(); i++)
			for (Enumeration e=this.FreeChunksOnCommit.keys(); e.hasMoreElements(); )
			{
				long chunknumber = ((Long) e.nextElement()).longValue();
				this.archive.ReleaseBuffers(chunknumber);
			}
			this.archive.Flush();
			this.ClearBookKeeping();
		}

		public void Abort() throws Exception
		{
			//this.FreeChunksOnAbort.Sort();
			//this.FreeChunksOnAbort.Reverse();
			//foreach (Object thing in this.FreeChunksOnAbort) 
			//for (int i=0; i<this.FreeChunksOnAbort.size(); i++)
			for (Enumeration e=this.FreeChunksOnAbort.keys(); e.hasMoreElements(); )
			{
				long chunknumber = ((Long) e.nextElement()).longValue();
				this.archive.ReleaseBuffers(chunknumber);
			}
			this.tree.Abort();
			this.archive.Flush();
			this.ClearBookKeeping();
		}
		
		public void SetFootPrintLimit(int limit) throws Exception
		{
			this.tree.SetFootPrintLimit(limit);
		}

		void ClearBookKeeping() throws Exception
		{
			this.FreeChunksOnCommit.clear();
			this.FreeChunksOnAbort.clear();
		}

//		#endregion

//		public String toHtml() throws Exception
//		{
//			String treehtml = this.tree.toHtml();
//			System.Text.StringBuilder sb = new System.Text.StringBuilder();
//			sb.Append(treehtml);
//			sb.Append("\r\n<br> free on commit "+this.FreeChunksOnCommit.Count+" ::");
//			foreach (Object thing in this.FreeChunksOnCommit) 
//			{
//				sb.Append(" "+thing);
//			}
//			sb.Append("\r\n<br> free on abort "+this.FreeChunksOnAbort.Count+" ::");
//			foreach (Object thing in this.FreeChunksOnAbort) 
//			{
//				sb.Append(" "+thing);
//			}
//			return sb.ToString(); // archive info not included
//		}
	}

