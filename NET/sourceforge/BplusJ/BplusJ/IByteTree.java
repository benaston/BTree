
package NET.sourceforge.BplusJ.BplusJ;
	/// <summary>
	/// A tree which returns byte array values
	/// </summary>
	public interface IByteTree extends ITreeIndex 
	{
		//byte[] this[String key] { get; set; }
		byte[] get(String key) throws Exception;
		void set(String key, byte[] value) throws Exception;
	}