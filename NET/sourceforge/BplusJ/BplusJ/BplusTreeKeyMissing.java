package NET.sourceforge.BplusJ.BplusJ;

	/// <summary>
	/// No such key found for attempted retrieval.
	/// </summary>
	public class BplusTreeKeyMissing extends Exception
	{
		public BplusTreeKeyMissing(String message)//: base(message) 
		{
			// do nothing extra
			super(message);
		}
	}