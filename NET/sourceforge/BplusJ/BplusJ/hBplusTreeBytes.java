
package NET.sourceforge.BplusJ.BplusJ;
	/// <summary>
	/// Btree mapping unlimited length key Strings to fixed length hash values
	/// </summary>
	public class hBplusTreeBytes extends xBplusTreeBytes
	{
		public hBplusTreeBytes(BplusTreeBytes tree, int hashLength) //: base(tree, hashLength)
		 throws Exception
		{
			// null out the culture context to use the naive comparison
			super(tree, hashLength);
			this.tree.NoCulture();
		}
		
		public static xBplusTreeBytes Initialize(String treefileName, String blockfileName, int PrefixLength, int CultureId,
			int nodesize, int buffersize) 
			 throws Exception
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId, nodesize, buffersize),
				PrefixLength);
		}
		public static xBplusTreeBytes Initialize(String treefileName, String blockfileName, int PrefixLength, int CultureId) 
			 throws Exception
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, CultureId),
				PrefixLength);
		}
		public static xBplusTreeBytes Initialize(String treefileName, String blockfileName, int PrefixLength) 
			 throws Exception
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength),
				PrefixLength);
		}
		public static xBplusTreeBytes Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId,
			int nodesize, int buffersize) 
			 throws Exception
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId, nodesize, buffersize),
				PrefixLength);
		}
		public static xBplusTreeBytes Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength, int CultureId)
			 throws Exception
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, CultureId),
				PrefixLength);
		}
		public static xBplusTreeBytes Initialize(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile, int PrefixLength) 
			 throws Exception
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefile, blockfile, PrefixLength),
				PrefixLength);
		}

		public static xBplusTreeBytes ReOpen(java.io.RandomAccessFile treefile, java.io.RandomAccessFile blockfile)  throws Exception
		{
			BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefile, blockfile);
			int prefixLength = tree.MaxKeyLength();
			return new hBplusTreeBytes(tree, prefixLength);
		}
		public static xBplusTreeBytes ReOpen(String treefileName, String blockfileName)  throws Exception
		{
			BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefileName, blockfileName);
			int prefixLength = tree.MaxKeyLength();
			return new hBplusTreeBytes(tree, prefixLength);
		}
		public static xBplusTreeBytes ReadOnly(String treefileName, String blockfileName)  throws Exception
		{
			BplusTreeBytes tree = BplusTreeBytes.ReadOnly(treefileName, blockfileName);
			int prefixLength = tree.MaxKeyLength();
			return new hBplusTreeBytes(tree, prefixLength);
		}

		public String PrefixForByteCount(String s, int maxbytecount) throws Exception
		{
			byte[] inputbytes = BplusTree.StringToBytes(s);
			java.security.MessageDigest D = java.security.MessageDigest.getInstance("MD5");	
			byte[] digest = D.digest(inputbytes);
			byte[] resultbytes = new byte[maxbytecount];
			for (int i=0; i<maxbytecount; i++) {
				int r = digest[ i % digest.length ];
				if (r<0) {
					r = -r;
				}
				r = r%79 + 40; // printable ascii
				resultbytes[i] = (byte)r;
			}
			String result = BplusTree.BytesToString(resultbytes);
			return result;
		}

//		public String PrefixForByteCount(String s, int maxbytecount) throws Exception OLD VERSION
//		{
//			// compute a hash code as a String which has maxbytecount size as a byte sequence
//			byte[] resultbytes = new byte[maxbytecount];
//			byte[] inputbytes = BplusTree.StringToBytes(s);
//			int sevenbits = 127;
//			int eighthbit = 128;
//			boolean invert = false;
//			for (int i=0; i<maxbytecount; i++) 
//			{
//				resultbytes[i] = (byte) (i & sevenbits);
//			}
//			System.out.println("\n\n\n prefixing "+s+" "+maxbytecount);
//			for (int i=0; i<inputbytes.length; i++) 
//			{
//				int inputbyte = (inputbytes[i]+256)%256;
//				System.out.println("inputbyte = "+inputbyte);
//				int outputindex = i % maxbytecount;
//				int outputbyte = (resultbytes[outputindex]+256)%256;
//				System.out.println("outputbyte = "+outputbyte);
//				int rotator = (i/maxbytecount) % 8;
//				if (rotator!=0) 
//				{
//					int hipart = inputbyte << rotator;
//					System.out.println("hipart = "+hipart);
//					int lowpart = inputbyte >> (8-rotator);
//					System.out.println("lowpart = "+lowpart);
//					inputbyte = (hipart | lowpart);
//					System.out.println("new inputbyte = "+inputbyte);
//				}
//				outputbyte = (((inputbyte ^ outputbyte)) % sevenbits);
//				System.out.println("uninverted outputbyte = "+outputbyte);
//				if ( (inputbyte&eighthbit)!=0 ) 
//				{
//					invert = !invert;
//				}
//				if (invert) 
//				{
//					outputbyte = ((outputbyte ^ sevenbits)&sevenbits) % eighthbit;
//					System.out.println("inverted outputbyte = "+outputbyte);
//				}
//				resultbytes[outputindex] = (byte) outputbyte;
//				System.out.println("result "+outputindex+" = "+resultbytes[outputindex]);
//			}
//			String result = BplusTree.BytesToString(resultbytes);
//			if (result.length()!=maxbytecount) 
//			{
//				throw new BplusTreeException("bad hash value generated with length: "+result.length()+" not "+maxbytecount);
//			}
//			return result;
//		}
//		public String toHtml() 
//		{
//			System.Text.StringBuilder sb = new System.Text.StringBuilder();
//			sb.Append(((BplusTreeBytes) this.tree).toHtml());
//			sb.Append("\r\n<br><b>key / hash / value dump</b><br>");
//			String currentkey = this.FirstKey();
//			while (currentkey!=null) 
//			{
//				sb.Append("\r\n<br>"+currentkey);
//				sb.Append(" / "+BplusNode.PrintableString(this.PrefixForByteCount(currentkey, this.prefixLength)));
//				try 
//				{
//					sb.Append( " / value found " );
//				}
//				catch (Exception) 
//				{
//					sb.Append( " !!!!!!! FAILED TO GET VALUE");
//				}
//				currentkey = this.NextKey(currentkey);
//			}
//			return sb.ToString();
//		}
	}

