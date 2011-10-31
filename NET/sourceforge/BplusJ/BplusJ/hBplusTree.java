
package NET.sourceforge.BplusJ.BplusJ;
	/// <summary>
	/// Tree index mapping Strings to Strings with unlimited key length
	/// </summary>
	public class hBplusTree extends BplusTree
	{
		hBplusTreeBytes xtree;
		public hBplusTree(hBplusTreeBytes tree) //: base(tree)
		{
			super(tree);
			this.xtree = tree;
		}
//		protected override boolean checkTree()
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
			hBplusTreeBytes tree = (hBplusTreeBytes) hBplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId, nodesize, buffersize);
			return new hBplusTree(tree);
		}
		public static BplusTree Initialize(String treefileName, String blockfileName, int PrefixLength, int CultureId) 
			throws Exception
		{
			hBplusTreeBytes tree = (hBplusTreeBytes) hBplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId);
			return new hBplusTree(tree);
		}
		public static BplusTree Initialize(String treefileName, String blockfileName, int PrefixLength) 
			throws Exception
		{
			hBplusTreeBytes tree = (hBplusTreeBytes) hBplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength);
			return new hBplusTree(tree);
		}
		
		public static BplusTree Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId,
			int nodesize, int buffersize) 
			throws Exception
		{
			hBplusTreeBytes tree = (hBplusTreeBytes) hBplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId, nodesize, buffersize);
			return new hBplusTree(tree);
		}
		public static BplusTree Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId) 
			throws Exception
		{
			hBplusTreeBytes tree = (hBplusTreeBytes) hBplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId);
			return new hBplusTree(tree);
		}
		public static BplusTree Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int KeyLength) 
			throws Exception
		{
			hBplusTreeBytes tree = (hBplusTreeBytes) hBplusTreeBytes.Initialize(treefile, blockfile, KeyLength);
			return new hBplusTree(tree);
		}
		
		public static BplusTree ReOpen(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile) 
			throws Exception
		{
			hBplusTreeBytes tree = (hBplusTreeBytes) hBplusTreeBytes.ReOpen(treefile, blockfile);
			return new hBplusTree(tree);
		}
		public static BplusTree ReadOnly(String treefileName, String blockfileName) 
			throws Exception
		{
			hBplusTreeBytes tree = (hBplusTreeBytes) hBplusTreeBytes.ReadOnly(treefileName, blockfileName);
			return new hBplusTree(tree);
		}
//		public override String toHtml() 
//		{
//			return ((hBplusTreeBytes) this.tree).toHtml();
//		}
	}

