using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace dexih.functions.tests
{
    public class dexih_functions_managedTasks
    {
        private readonly ITestOutputHelper output;

        int progressCounter = 0;
        int completedCounter = 0;
        int startedCounter = 0;

        public dexih_functions_managedTasks(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public async Task Test_1ManagedTask()
        {
            var managedTasks = new ManagedTasks();

            // add a series of tasks with various delays to ensure the task manager is running.
            async Task Action(IProgress<int> progress, CancellationToken cancellationToken)
            {
                for (var i = 0; i < 5; i++)
                {
                    await Task.Delay(20, cancellationToken);
                    progress.Report(i * 20);
                }
            }

            progressCounter = 0;
            var task1 = managedTasks.Add("123", "task3", Action);

            managedTasks.OnProgress += Progress;
            await managedTasks.WhenAll();

            // ensure the progress was called at least once. 
            // This doesn't get called for every progress event as when they stack up they get dropped
            // which is expected bahaviour.
            Assert.True(progressCounter > 0);

        }

        void Progress(Object sender, EventArgs args)
        {
            var task = (ManagedTask)sender;
            Assert.Equal(progressCounter, task.Percentage);
            Assert.Equal(EManagedTaskStatus.Running, task.Status);
            progressCounter += 20;
        }

        [Theory]
        [InlineData(500)]
        public async Task Test_MultipleManagedTasks(int TaskCount)
        {
            var managedTasks = new ManagedTasks();
            managedTasks.OnCompleted += CompletedCounter;

            // simple task reports progress 10 times.
            async Task Action(IProgress<int> progress, CancellationToken cancellationToken)
            {
                for (var i = 0; i < 10; i++)
                {
                    await Task.Delay(20, cancellationToken);
                    progress.Report(i * 10);
                }
            }

            completedCounter = 0;

            // add the simple task 100 times.
            for (int i = 0; i < TaskCount; i++)
            {
                var task1 = managedTasks.Add("123", "task3", Action);
                task1.OnStarted += StartedCounter;
            }

            await managedTasks.WhenAll();

            // counter should eqaul the number of tasks
            Assert.Equal(TaskCount, completedCounter);
            Assert.Equal(0, managedTasks.Count());
        }

        object startedLock = 1;

        void StartedCounter(object sender, EventArgs args)
        {
            lock(startedLock)
            {
                startedCounter++;
            }
        }

        object completedLock = 1;

        void CompletedCounter(Object sender, EventArgs args)
        {
            var task = (ManagedTask)sender;
            //if (task.Status == EManagedTaskStatus.Success)
            lock (completedLock)
            {
                completedCounter++;

            }

            //else
            //    throw new Exception("should be success");
        }

        int errorCounter = 0;
        [Fact]
        public async Task Test_ManagedTask_Error()
        {
            var managedTasks = new ManagedTasks();

            // simple task reports progress 10 times.
            async Task Action(IProgress<int> progress, CancellationToken cancellationToken)
            {
                throw new Exception("An error");
            }

            // add the simple task 500 times.
            errorCounter = 0;
            managedTasks.OnCompleted += ErrorResult;

            for (int i = 0; i < 500; i++)
            {
                var task1 = managedTasks.Add("123", "task3", Action);
            }

            await managedTasks.WhenAll();

            // counter should eqaul the number of tasks
            Assert.Equal(500, errorCounter);
            Assert.Equal(0, managedTasks.Count());
        }

        void ErrorResult(Object sender, EventArgs args)
        {
            var task = (ManagedTask)sender;
            if (task.Status == EManagedTaskStatus.Error)
                errorCounter++;
        }


        int cancelCounter = 0;
        [Fact]
        public async Task Test_ManagedTask_Cancel()
        {
            var managedTasks = new ManagedTasks();

            // simple task that can be cancelled
            async Task Action(IProgress<int> progress, CancellationToken cancellationToken)
            {
                await Task.Delay(100000, cancellationToken);
                output.WriteLine("cancelled");
            }

            // add the simple task 500 times.
            cancelCounter = 0;
            managedTasks.OnCancelled += CancelResult;

            var tasks = new ManagedTask[500];
            for (int i = 0; i < 500; i++)
            {
                tasks[i] = managedTasks.Add("123", "task3", Action);
            }

            for (int i = 0; i < 500; i++)
            {
                tasks[i].Cancel();
            }

            await managedTasks.WhenAll();

            // counter should eqaul the number of tasks
            Assert.Equal(500, cancelCounter);
            Assert.Equal(0, managedTasks.Count());
        }

       void CancelResult(Object sender, EventArgs args)
        {
            var task = (ManagedTask)sender;
            if(task.Status == EManagedTaskStatus.Canceled)
                cancelCounter++;
        }
    }
}