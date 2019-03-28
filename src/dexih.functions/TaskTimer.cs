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
		public static async Task<(TimeSpan, T)>  Start(Func<Task<T>> func)
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
		public static async Task<TimeSpan> StartAsync(Func<Task> func)
		{
			var timer = Stopwatch.StartNew();
			await func();
			timer.Stop();
			return timer.Elapsed;
		}
		
		public static TimeSpan Start(Action action)
		{
			var timer = Stopwatch.StartNew();
			action();
			timer.Stop();
			return timer.Elapsed;
		}


		
		
	}
}
