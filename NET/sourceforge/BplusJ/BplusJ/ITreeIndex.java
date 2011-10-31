
package NET.sourceforge.BplusJ.BplusJ;
	/// <summary>
	/// This is the shared interface among the various tree index implementations.  Each
	/// implementation also supports an indexing notation this[key] which is not included
	/// here because of type incompatibilities.
	/// </summary>
	public interface ITreeIndex 
	{
		/// <summary>
		/// Examine the structure and optionally try to reclaim unreachable space.  A structure which was modified without a
		/// concluding commit or abort may contain unreachable space.
		/// </summary>
		/// <param name="CorrectErrors">if true try to correct errors detected, if false throw an exception on errors.</param>
		void Recover(boolean CorrectErrors) throws Exception;
		/// <summary>
		/// Dispose of the key and its associated value.  Throw an exception if the key is missing.
		/// </summary>
		/// <param name="key">Key to erase.</param>
		void RemoveKey(String key) throws Exception;
		/// <summary>
		/// Get the least key in the structure.
		/// </summary>
		/// <returns>least key value or null if the tree is empty.</returns>
		String FirstKey() throws Exception;
		/// <summary>
		/// Get the least key in the structure strictly "larger" than the argument.  Return null if there is no such key.
		/// </summary>
		/// <param name="AfterThisKey">The "lower limit" for the value to return</param>
		/// <returns>Least key greater than argument or null</returns>
		String NextKey(String AfterThisKey) throws Exception;
		/// <summary>
		/// Return true if the key is present in the structure.
		/// </summary>
		/// <param name="key">Key to test</param>
		/// <returns>true if present, otherwise false.</returns>
		boolean ContainsKey(String key) throws Exception;
		/// <summary>
		/// Get the Object associated with the key, or return the default if the key is not present.
		/// </summary>
		/// <param name="key">key to retrieve.</param>
		/// <param name="defaultValue">default value to use if absent.</param>
		/// <returns>the mapped value boxed as an Object</returns>
		Object Get(String key, Object defaultValue) throws Exception;
		/// <summary>
		/// map the key to the value in the structure.
		/// </summary>
		/// <param name="key">the key</param>
		/// <param name="map">the value	(must coerce to the appropriate value for the tree instance).</param>
		void Set(String key, Object map) throws Exception;
		/// <summary>
		/// Make changes since the last commit permanent.
		/// </summary>
		void Commit() throws Exception;
		/// <summary>
		/// Discard changes since the last commit and return to the state at the last commit point.
		/// </summary>
		void Abort() throws Exception;
		/// <summary>
		/// Set a parameter used to decide when to release memory mapped buffers.
		/// Larger values mean that more memory is used but accesses may be faster
		/// especially if there is locality of reference.  5 is too small and 1000
		/// may be too big.
		/// </summary>
		/// <param name="limit">maximum number of leaves with no materialized children to keep in memory.</param>
		void SetFootPrintLimit(int limit) throws Exception;
		/// <summary>
		/// Close and flush the streams without committing or aborting.
		/// (This is equivalent to abort, except unused space in the streams may be left unreachable).
		/// </summary>
		void Shutdown() throws Exception;
		/// <summary>
		/// Use the culture context for this tree to compare two Strings.
		/// </summary>
		/// <param name="left"></param>
		/// <param name="right"></param>
		/// <returns></returns>
		int Compare(String left, String right) throws Exception;
	}

/*
	/// <summary>
	/// A tree which returns byte array values
	/// </summary>
	public interface IByteTree : ITreeIndex 
	{
		byte[] this[String key] { get; set; }
	}
	/// <summary>
	/// A tree which returns byte String
	/// </summary>
	public interface IStringTree : ITreeIndex 
	{
		String this[String key] { get; set; }
	}
	/// <summary>
	/// A tree which returns Object values
	/// </summary>
	public interface IObjectTree : ITreeIndex 
	{
		Object this[String key] { get; set; }
	}
	*/
