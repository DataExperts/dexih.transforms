using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace dexih.functions.tests
{
    public class dexih_functions_managedTasks
    {
        [Fact]
        public async Task Test_1ManagedTask()
        {
            var managedTasks = new ManagedTasks();

            // add a series of tasks with various delays to ensure the task manager is running.
            async void Action(IProgress<int> progress, CancellationToken cancellationToken)
            {
                for (var i = 0; i < 5; i++)
                {
                    progress.Report(i * 20);
                    await Task.Delay(10, cancellationToken);
                }
            }

            var task1 = managedTasks.Add("123", "task3", Action);

			int percentage = 0;
			managedTasks.OnProgress += Progress;
            void Progress(ManagedTask task)
            {
                Assert.Equal(percentage, task.Percentage);
                percentage += 20;
            }

            await managedTasks.WhenAll();
            Assert.Equal(100, percentage);

        }
    }
}