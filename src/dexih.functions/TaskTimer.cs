using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace dexih.functions
{
	/// <summary>
	/// Runs a task with a timer attached.  
	/// </summary>
	public static class TaskTimer<T>
	{
		public async static Task<(TimeSpan, T)>  Start(Func<Task<T>> func)
		{
			var timer = Stopwatch.StartNew();
			var returnValue = await func();
			timer.Stop();
			return (timer.Elapsed, returnValue);
		}
	}

	/// <summary>
	/// Runs a task with a timer attached.  
	/// </summary>
	public static class TaskTimer
	{
		public async static Task<TimeSpan> Start(Func<Task> func)
		{
			var timer = Stopwatch.StartNew();
			await func();
			timer.Stop();
			return timer.Elapsed;
		}
	}
}
