#if NETFRAMEWORK
using System;

namespace NLog.FastNetworkTarget
{
	public static class Program
	{
		public static int Main(string[] args)
		{
			var result = new NUnitLite.AutoRun().Execute(args);
			Environment.Exit(result);
			return result;
		}
	}
}
#endif
