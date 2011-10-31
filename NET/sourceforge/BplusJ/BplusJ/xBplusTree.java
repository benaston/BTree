
package NET.sourceforge.BplusJ.BplusJ;
	/// <summary>
	/// Tree index mapping Strings to Strings with unlimited key length
	/// </summary>
	public class xBplusTree extends BplusTree
	{
		xBplusTreeBytes xtree;
		public xBplusTree(xBplusTreeBytes tree) //: base(tree)
			throws Exception
		{
			super(tree);
			this.xtree = tree;
		}
//		protected override bool checkTree()
//		{
//			return false;
//		}
		public void LimitBucketSize(int limit) 
		{
			this.xtree.BucketSizeLimit = limit;
		}
		public static BplusTree Initialize(String treefileName, String blockfileName, int PrefixLength, int CultureId,
			int nodesize, int buffersize) 
			throws Exception
		{
			xBplusTreeBytes tree = xBplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId, nodesize, buffersize);
			return new xBplusTree(tree);
		}
		public static BplusTree Initialize(String treefileName, String blockfileName, int PrefixLength, int CultureId) 
			throws Exception
		{
			xBplusTreeBytes tree = xBplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId);
			return new xBplusTree(tree);
		}
		public static BplusTree Initialize(String treefileName, String blockfileName, int PrefixLength) 
			throws Exception
		{
			xBplusTreeBytes tree = xBplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength);
			return new xBplusTree(tree);
		}
		
		public static BplusTree Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId,
			int nodesize, int buffersize) 
			throws Exception
		{
			xBplusTreeBytes tree = xBplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId, nodesize, buffersize);
			return new xBplusTree(tree);
		}
		public static BplusTree Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId) 
			throws Exception
		{
			xBplusTreeBytes tree = xBplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId);
			return new xBplusTree(tree);
		}
		public static BplusTree Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int KeyLength) 
			throws Exception
		{
			xBplusTreeBytes tree = xBplusTreeBytes.Initialize(treefile, blockfile, KeyLength);
			return new xBplusTree(tree);
		}
		
		public static BplusTree ReOpen(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile) 
			throws Exception
		{
			xBplusTreeBytes tree = xBplusTreeBytes.ReOpen(treefile, blockfile);
			return new xBplusTree(tree);
		}
		public static BplusTree ReOpen(String treefileName, String blockfileName) 
			throws Exception
		{
			xBplusTreeBytes tree = xBplusTreeBytes.ReOpen(treefileName, blockfileName);
			return new xBplusTree(tree);
		}
		public static BplusTree ReadOnly(String treefileName, String blockfileName) 
			throws Exception
		{
			xBplusTreeBytes tree = xBplusTreeBytes.ReadOnly(treefileName, blockfileName);
			return new xBplusTree(tree);
		}
	}

