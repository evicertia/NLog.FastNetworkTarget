namespace NLog.Targets
{
	/// <summary>
	/// Specifies the action to take when the network sender queue is full.
	/// </summary>
	public enum NetworkTargetQueueOverflowAction
	{
		/// <summary>
		/// Discard the log message if the queue is full.
		/// Does not block caller threads.
		/// </summary>
		Discard = 0,

		/// <summary>
		/// Block caller thread until queue space becomes available.
		/// </summary>
		Block = 1
	}
}
