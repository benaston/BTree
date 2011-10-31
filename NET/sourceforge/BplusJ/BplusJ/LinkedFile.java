

package NET.sourceforge.BplusJ.BplusJ;
import java.util.*;


	/// <summary>
	/// Chunked singly linked file with garbage collection.
	/// </summary>
	public class LinkedFile 
	{
		static long NULLBUFFERPOINTER = -1;
		//java.io.RandomAccessFile fromfile;
		java.io.RandomAccessFile fromfile;
		BufferFile buffers;
		int buffersize;
		int headersize;
		long seekStart = 0;
		long FreeListHead = NULLBUFFERPOINTER;
		long RecentNewBufferNumber = NULLBUFFERPOINTER;
		boolean headerDirty = true;
		byte FREE = 0;
		byte HEAD = 1;
		byte BODY = 2;
		public static byte[] HEADERPREFIX = { 98, 112, 78, 108, 102 };
		public static byte VERSION = 0;
		public static int MINBUFFERSIZE = 20;
		// next pointer and indicator flag
		public static int BUFFEROVERHEAD = BufferFile.LONGSTORAGE + 1;
		public LinkedFile(int buffersize, long seekStart)
			throws Exception
		{
			this.seekStart = seekStart;
			//this.buffers = buffers;
			this.buffersize = buffersize;
			// markers+version byte+buffersize+freelisthead
			this.headersize = HEADERPREFIX.length + 1 + BufferFile.INTSTORAGE + BufferFile.LONGSTORAGE; 
			this.sanityCheck();
		}
		public static LinkedFile SetupFromExistingStream(java.io.RandomAccessFile fromfile) 
			throws Exception
		{
			return SetupFromExistingStream(fromfile, (long)0);
		}
		public static LinkedFile SetupFromExistingStream(java.io.RandomAccessFile fromfile, long StartSeek) 
			throws Exception
		{
			LinkedFile result = new LinkedFile(100, StartSeek); // dummy buffer size for now
			result.fromfile = fromfile;
			result.readHeader();
			result.buffers = BufferFile.SetupFromExistingStream(fromfile, StartSeek+result.headersize);
			return result;
		}
		void readHeader() 
			throws Exception
		{
			byte[] header = new byte[this.headersize];
			//this.fromfile.Seek(this.seekStart, System.IO.SeekOrigin.Begin);
			this.fromfile.seek(this.seekStart);
			//this.fromfile.Read(header, 0, this.headersize);
			this.fromfile.read(header, 0, this.headersize);
			int index = 0;
			// check prefix
			//foreach (byte b in HEADERPREFIX) 
			for (int i=0; i<HEADERPREFIX.length; i++)
			{
				byte b = HEADERPREFIX[i];
				if (header[index]!=b) 
				{
					throw new LinkedFileException("invalid header prefix");
				}
				index++;
			}
			// skip version (for now)
			index++;
			// read buffersize
			this.buffersize = BufferFile.Retrieve(header, index);
			index += BufferFile.INTSTORAGE;
			this.FreeListHead = BufferFile.RetrieveLong(header, index);
			this.sanityCheck();
			this.headerDirty = false;
		}
		public static LinkedFile InitializeLinkedFileInStream(java.io.RandomAccessFile fromfile, int buffersize) 
			throws Exception
		{
			return InitializeLinkedFileInStream(fromfile, buffersize, (long)0);
		}
		public static LinkedFile InitializeLinkedFileInStream(java.io.RandomAccessFile fromfile, int buffersize, long StartSeek)
			throws Exception 
		{
			LinkedFile result = new LinkedFile(buffersize, StartSeek);
			result.fromfile = fromfile;
			result.setHeader();
			// buffersize should be increased by overhead...
			result.buffers = BufferFile.InitializeBufferFileInStream(fromfile, buffersize+BUFFEROVERHEAD, StartSeek+result.headersize);
			return result;
		}
		public void setHeader() 
			throws Exception
		{
			byte[] header = this.makeHeader();
			//this.fromfile.Seek(this.seekStart, System.IO.SeekOrigin.Begin);
			this.fromfile.seek(this.seekStart);
			//this.fromfile.Write(header, 0, header.length);
			this.fromfile.write(header, 0, header.length);
			this.headerDirty = false;
		}
		public byte[] makeHeader() 
			throws Exception
		{
			byte[] result = new byte[this.headersize];
			//HEADERPREFIX.CopyTo(result, 0);
			for (int i=0; i<HEADERPREFIX.length; i++) 
			{
				result[i] = HEADERPREFIX[i];
			}
			result[HEADERPREFIX.length] = VERSION;
			int index = HEADERPREFIX.length+1;
			BufferFile.Store(this.buffersize, result, index);
			index += BufferFile.INTSTORAGE;
			BufferFile.Store(this.FreeListHead, result, index);
			return result;
		}
		public void Recover(Hashtable ChunksInUse, boolean FixErrors) 
			throws Exception
		{
			// find missing space and recover it
			this.checkStructure(ChunksInUse, FixErrors);
		}
		void sanityCheck() 
			throws Exception
		{
			if (this.seekStart<0) 
			{
				throw new LinkedFileException("cannot seek negative "+this.seekStart);
			}
			if (this.buffersize<MINBUFFERSIZE) 
			{
				throw new LinkedFileException("buffer size too small "+this.buffersize);
			}
		}
		public void Shutdown()
			throws Exception
		{
			// flushing not needed in java
			//this.fromfile.Flush();
			this.fromfile.close();
		}
//		byte[] ParseBuffer(long bufferNumber, out byte type, out long NextBufferNumber) 
//		{
//			byte[] thebuffer = new byte[this.buffersize];
//			byte[] fullbuffer = new byte[this.buffersize+BUFFEROVERHEAD];
//			this.buffers.getBuffer(bufferNumber, fullbuffer, 0, fullbuffer.length);
//			type = fullbuffer[0];
//			NextBufferNumber = BufferFile.RetrieveLong(fullbuffer, 1);
//			Array.Copy(fullbuffer, BUFFEROVERHEAD, thebuffer, 0, this.buffersize);
//			return thebuffer;
//		}
		public class ParseBuffer 
		{
			public byte[] payload;
			public byte type;
			public long NextBufferNumber;
			public ParseBuffer(long bufferNumber) 
				throws Exception
			{
				byte[] thebuffer = new byte[LinkedFile.this.buffersize];
				byte[] fullbuffer = new byte[LinkedFile.this.buffersize+LinkedFile.BUFFEROVERHEAD];
				LinkedFile.this.buffers.getBuffer(bufferNumber, fullbuffer, 0, fullbuffer.length);
				this.type = fullbuffer[0];
				this.NextBufferNumber = BufferFile.RetrieveLong(fullbuffer, 1);
				for (int i=0; i<LinkedFile.this.buffersize; i++) 
				{
					thebuffer[i] = fullbuffer[i+LinkedFile.BUFFEROVERHEAD];
				}
				this.payload = thebuffer;
			}
		}
		void SetBuffer(long buffernumber, byte type, byte[] thebuffer, int start, int length, long NextBufferNumber)
			throws Exception
		{
			//System.Diagnostics.Debug.WriteLine(" storing chunk type "+type+" at "+buffernumber);
			if (this.buffersize<length) 
			{
				throw new LinkedFileException("buffer size too small "+this.buffersize+"<"+length);
			}
			byte[] fullbuffer = new byte[length+BUFFEROVERHEAD];
			fullbuffer[0] = type;
			BufferFile.Store(NextBufferNumber, fullbuffer, 1);
			if (thebuffer!=null) 
			{
				//Array.Copy(thebuffer, start, fullbuffer, BUFFEROVERHEAD, length);
				for (int i=0; i<length; i++) 
				{
					fullbuffer[BUFFEROVERHEAD+i] = thebuffer[i];
				}
			}
			this.buffers.setBuffer(buffernumber, fullbuffer, 0, fullbuffer.length);
		}

		void DeallocateBuffer(long buffernumber) 
			throws Exception
		{
			
			//System.Diagnostics.Debug.WriteLine(" deallocating "+buffernumber);
			// should be followed by resetting the header eventually.
			this.SetBuffer(buffernumber, FREE, null, 0, 0, this.FreeListHead);
			this.FreeListHead = buffernumber;
			this.headerDirty = true;
		}
		long AllocateBuffer() 
			throws Exception
		{
			if (this.FreeListHead!=NULLBUFFERPOINTER) 
			{
				// reallocate a freed buffer
				long result = this.FreeListHead;
				//byte buffertype;
				//long NextFree;
				//byte[] dummy = this.ParseBuffer(result, out buffertype, out NextFree);
				ParseBuffer P = new ParseBuffer(result);
				byte buffertype = P.type;
				long NextFree = P.NextBufferNumber;
				if (buffertype!=FREE) 
				{
					throw new LinkedFileException("free head buffer not marked free");
				}
				this.FreeListHead = NextFree;
				this.headerDirty = true;
				this.RecentNewBufferNumber = NULLBUFFERPOINTER;
				return result;
			} 
			else 
			{
				// allocate a new buffer
				long NextBufferNumber = this.buffers.nextBufferNumber();
				if (this.RecentNewBufferNumber==NextBufferNumber) 
				{
					// the previous buffer has been allocated but not yet written.  It must be written before the following one...
					NextBufferNumber++;
				}
				this.RecentNewBufferNumber = NextBufferNumber;
				return NextBufferNumber;
			}
		}
		public void checkStructure() 
			throws Exception
		{
			checkStructure(null, false);
		}
		public void checkStructure(Hashtable ChunksInUse, boolean FixErrors) 
			throws Exception
		{
			Hashtable buffernumberToType = new Hashtable();
			Hashtable buffernumberToNext = new Hashtable();
			Hashtable visited = new Hashtable();
			long LastBufferNumber = this.buffers.nextBufferNumber();
			for (long buffernumber=0; buffernumber<LastBufferNumber; buffernumber++) 
			{
				//byte buffertype;
				//long NextBufferNumber;
				//this.ParseBuffer(buffernumber, out buffertype, out NextBufferNumber);
				ParseBuffer P = new ParseBuffer(buffernumber);
				byte buffertype = P.type;
				long NextBufferNumber = P.NextBufferNumber;
				//buffernumberToType[buffernumber] = buffertype;
				buffernumberToType.put(new Long(buffernumber), new Byte(buffertype));
				//buffernumberToNext[buffernumber] = NextBufferNumber;
				buffernumberToNext.put(new Long(buffernumber), new Long(NextBufferNumber));
			}
			// traverse the freelist
			long thisFreeBuffer = this.FreeListHead;
			while (thisFreeBuffer!=NULLBUFFERPOINTER) 
			{
				if (visited.containsKey(new Long(thisFreeBuffer))) 
				{
					throw new LinkedFileException("cycle in freelist "+thisFreeBuffer);
				}
				//visited[thisFreeBuffer] = thisFreeBuffer;
				visited.put(new Long(thisFreeBuffer), new Long(thisFreeBuffer));
				byte thetype = ((Byte) buffernumberToType.get(new Long(thisFreeBuffer))).byteValue();
				//long NextBufferNumber = (long) buffernumberToNext[thisFreeBuffer];
				long NextBufferNumber = ((Long) buffernumberToNext.get(new Long(thisFreeBuffer))).longValue();
				if (thetype!=FREE) 
				{
					throw new LinkedFileException("free list element not marked free "+thisFreeBuffer);
				}
				thisFreeBuffer = NextBufferNumber;
			}
			// traverse all nodes marked head
			Hashtable allchunks = new Hashtable();
			for (long buffernumber=0; buffernumber<LastBufferNumber; buffernumber++) 
			{
				//byte thetype = (byte) buffernumberToType[buffernumber];
				byte thetype = ((Byte) buffernumberToType.get(new Long(buffernumber))).byteValue();
				if (thetype==HEAD) 
				{
					if (visited.containsKey(new Long(buffernumber))) 
					{
						throw new LinkedFileException("head buffer already visited "+buffernumber);
					}
					//allchunks[buffernumber] = buffernumber;
					allchunks.put(new Long(buffernumber), new Long(buffernumber));
					//visited[buffernumber] = buffernumber;
					visited.put(new Long(buffernumber), new Long(buffernumber));
					//long bodybuffernumber = (long) buffernumberToNext[buffernumber];
					long bodybuffernumber = ((Long) buffernumberToNext.get(new Long(buffernumber))).longValue();
					while (bodybuffernumber!=NULLBUFFERPOINTER) 
					{
						//byte bodytype = (byte) buffernumberToType[bodybuffernumber];
						byte bodytype = ((Byte) buffernumberToType.get(new Long(bodybuffernumber))).byteValue();
						//long NextBufferNumber = (long) buffernumberToNext[bodybuffernumber];
						long NextBufferNumber = ((Long) buffernumberToNext.get(new Long(bodybuffernumber))).longValue();
						if (visited.containsKey(new Long(bodybuffernumber))) 
						{
							throw new LinkedFileException("body buffer visited twice "+bodybuffernumber);
						}
						//visited[bodybuffernumber] = bodytype;
						visited.put(new Long(bodybuffernumber), new Byte(bodytype));
						if (bodytype!=BODY) 
						{
							throw new LinkedFileException("body buffer not marked body "+thetype);
						}
						bodybuffernumber = NextBufferNumber;
					}
					// check retrieval
					this.GetChunk(buffernumber);
				}
			}
			// make sure all were visited
			for (long buffernumber=0; buffernumber<LastBufferNumber; buffernumber++) 
			{
				if (!visited.containsKey(new Long(buffernumber))) 
				{
					throw new LinkedFileException("buffer not found either as data or free "+buffernumber);
				}
			}
			// check against in use list
			if (ChunksInUse!=null) 
			{
				ArrayList notInUse = new ArrayList();
				//foreach (DictionaryEntry d in ChunksInUse)
				for (Enumeration e=ChunksInUse.keys(); e.hasMoreElements(); )
				{
					//long buffernumber = (long)d.Key;
					long buffernumber = ( (Long) e.nextElement() ).longValue();
					if (!allchunks.containsKey(new Long(buffernumber))) 
					{
						//System.Diagnostics.Debug.WriteLine("\r\n<br>allocated chunks "+allchunks.Count);
						//foreach (DictionaryEntry d1 in allchunks) 
						//{
						//	System.Diagnostics.Debug.WriteLine("\r\n<br>found "+d1.Key);
						//}
						throw new LinkedFileException("buffer in used list not found in linked file "+buffernumber);
					}
				}
				//foreach (DictionaryEntry d in allchunks)
				for (Enumeration e=allchunks.keys(); e.hasMoreElements(); )
				{
					//long buffernumber = (long)d.Key;
					long buffernumber = ( (Long) e.nextElement() ).longValue();
					if (!ChunksInUse.containsKey(new Long(buffernumber))) 
					{
						if (!FixErrors) 
						{
							throw new LinkedFileException("buffer in linked file not in used list "+buffernumber);
						}
						notInUse.add(new Long(buffernumber));
					}
				}
				//notInUse.Sort();
				//notInUse.Reverse();
				//foreach (object thing in notInUse) 
				for (int iii=0; iii<notInUse.size(); iii++)
				{
					//long buffernumber = (long)thing;
					long buffernumber = ( (Long) notInUse.get(iii) ).longValue();
					this.ReleaseBuffers(buffernumber);
				}
			}
		}
		public byte[] GetChunk(long HeadBufferNumber) 
			throws Exception
		{
			// get the head, interpret the length
			//byte buffertype;
			//long NextBufferNumber;
			//byte[] buffer = this.ParseBuffer(HeadBufferNumber, out buffertype, out NextBufferNumber);
			ParseBuffer P = new ParseBuffer(HeadBufferNumber);
			byte buffertype = P.type;
			long NextBufferNumber = P.NextBufferNumber;
			byte[] buffer = P.payload;
			int length = BufferFile.Retrieve(buffer, 0);
			if (length<0) 
			{
				throw new LinkedFileException("negative length block? must be garbage: "+length);
			}
			if (buffertype!=HEAD) 
			{
				throw new LinkedFileException("first buffer not marked HEAD");
			}
			byte[] result = new byte[length];
			// read in the data from the first buffer
			int firstLength = this.buffersize-BufferFile.INTSTORAGE;
			if (firstLength>length) 
			{
				firstLength = length;
			}
			//Array.Copy(buffer, BufferFile.INTSTORAGE, result, 0, firstLength);
			for (int i=0; i<firstLength; i++) 
			{
				result[i] = buffer[BufferFile.INTSTORAGE+i];
			}
			int stored = firstLength;
			while (stored<length) 
			{
				// get the next buffer
				long thisBufferNumber = NextBufferNumber;
				//buffer = this.ParseBuffer(thisBufferNumber, out buffertype, out NextBufferNumber);
				P = new ParseBuffer(thisBufferNumber);
				buffer = P.payload;
				buffertype = P.type;
				NextBufferNumber = P.NextBufferNumber;
				int nextLength = this.buffersize;
				if (length-stored<nextLength) 
				{
					nextLength = length-stored;
				}
				//Array.Copy(buffer, 0, result, stored, nextLength);
				for (int i=0; i<nextLength; i++) 
				{
					result[stored+i] = buffer[i];
				}
				stored += nextLength;
			}
			return result;
		}
		public long StoreNewChunk(byte[] fromArray, int startingAt, int length) 
			throws Exception
		{
			// get the first buffer as result value
			long currentBufferNumber = this.AllocateBuffer();
			//System.Diagnostics.Debug.WriteLine(" allocating chunk starting at "+currentBufferNumber);
			long result = currentBufferNumber;
			if (length<0 || startingAt<0) 
			{
				throw new LinkedFileException("cannot store negative length chunk ("+startingAt+","+length+")");
			}
			int endingAt = startingAt+length;
			// special case: zero length chunk
			if (endingAt>fromArray.length) 
			{
				throw new LinkedFileException("array doesn't have this much data: "+endingAt);
			}
			int index = startingAt;
			// store header with length information
			byte[] buffer = new byte[this.buffersize];
			BufferFile.Store(length, buffer, 0);
			int fromIndex = startingAt;
			int firstLength = this.buffersize-BufferFile.INTSTORAGE;
			int stored = 0;
			if (firstLength>length) 
			{
				firstLength=length;
			}
			//Array.Copy(fromArray, fromIndex, buffer, BufferFile.INTSTORAGE, firstLength);
			for (int i=0; i<firstLength; i++) 
			{
				buffer[BufferFile.INTSTORAGE+i] = fromArray[i];
			}
			stored += firstLength;
			fromIndex += firstLength;
			byte CurrentBufferType = HEAD;
			// store any remaining buffers (no length info)
			while (stored<length) 
			{
				// store current buffer and get next block number
				long NextBufferNumber = this.AllocateBuffer();
				this.SetBuffer(currentBufferNumber, CurrentBufferType, buffer, 0, buffer.length, NextBufferNumber);
				currentBufferNumber = NextBufferNumber;
				CurrentBufferType = BODY;
				int nextLength = this.buffersize;
				if (stored+nextLength>length) 
				{
					nextLength = length-stored;
				}
				//Array.Copy(fromArray, fromIndex, buffer, 0, nextLength);
				for (int i=0; i<nextLength; i++) 
				{
					buffer[i] = fromArray[fromIndex+i];
				}
				stored += nextLength;
				fromIndex += nextLength;
			}
			// store final buffer
			this.SetBuffer(currentBufferNumber, CurrentBufferType, buffer, 0, buffer.length, NULLBUFFERPOINTER);
			return result;
		}
		public void Flush() 
			throws Exception
		{
			if (this.headerDirty) 
			{
				this.setHeader();
			}
			this.buffers.Flush();
		}
		public void ReleaseBuffers(long HeadBufferNumber) 
			throws Exception
		{
			// KISS
			//System.Diagnostics.Debug.WriteLine(" deallocating chunk starting at "+HeadBufferNumber);
			long thisbuffernumber = HeadBufferNumber;
			//long NextBufferNumber;
			//byte buffertype;
			//byte[] dummy = this.ParseBuffer(HeadBufferNumber, out buffertype, out NextBufferNumber);
			ParseBuffer P = new ParseBuffer(HeadBufferNumber);
			long NextBufferNumber = P.NextBufferNumber;
			byte buffertype = P.type;
			if (buffertype!=HEAD) 
			{
				throw new LinkedFileException("head buffer not marked HEAD");
			}
			this.DeallocateBuffer(HeadBufferNumber);
			while (NextBufferNumber!=NULLBUFFERPOINTER) 
			{
				thisbuffernumber = NextBufferNumber;
				//dummy = this.ParseBuffer(thisbuffernumber, out buffertype, out NextBufferNumber);
				P = new ParseBuffer(thisbuffernumber);
				NextBufferNumber = P.NextBufferNumber;
				buffertype = P.type;
				if (buffertype!=BODY) 
				{
					throw new LinkedFileException("body buffer not marked BODY");
				}
				this.DeallocateBuffer(thisbuffernumber);
			}
		}
	}
/*
	public class LinkedFileException: ApplicationException 
	{
		public LinkedFileException(String message): base(message) 
		{
			// do nothing extra
		}
	}
	*/

