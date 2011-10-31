
package NET.sourceforge.BplusJ.BplusJ;
	/// <summary>
	/// A tree which returns byte array values
	/// </summary>
	public interface IStringTree extends ITreeIndex 
	{
		//string this[String key] { get; set; }
		String get(String key) throws Exception;
		void set(String key, String value) throws Exception;
	}