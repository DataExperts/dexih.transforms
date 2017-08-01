using dexih.functions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace dexih.functions.tests
{
    public class RealTimeQueueTests
    {
        [Fact]
        public async Task Test_QueuePushPop()
        {
            var queue = new RealTimeQueue<int>(2, 5000);

            await queue.Push(1);
            await queue.Push(2);

            var pop = await queue.Pop();
            Assert.Equal(pop.Package, 1);
            Assert.Equal<ERealTimeQueueStatus>(pop.Status, ERealTimeQueueStatus.NotComplete);

            await queue.Push(3, true);

            pop = await queue.Pop();
            Assert.Equal(pop.Package, 2);
            Assert.Equal<ERealTimeQueueStatus>(pop.Status, ERealTimeQueueStatus.NotComplete);

            pop = await queue.Pop();
            Assert.Equal(pop.Package, 3);
            Assert.Equal<ERealTimeQueueStatus>(pop.Status, ERealTimeQueueStatus.Complete);
        }

        [Fact]
        public async Task Test_QueueWaitWhenEmpty()
        {
            var queue = new RealTimeQueue<int>(2, 5000);

            //queue is empty so this should wait until something enters queue.
            var popTask = queue.Pop();
            
            await Task.Delay(50);
            Assert.Equal<TaskStatus>(popTask.Status, TaskStatus.WaitingForActivation);

            await queue.Push(1);

            var pop = await popTask;
            Assert.Equal(pop.Package, 1);
            Assert.Equal<ERealTimeQueueStatus>(pop.Status, ERealTimeQueueStatus.NotComplete);
        }

        [Fact]
        public async Task Test_QueueWaitWhenFull()
        {
            var queue = new RealTimeQueue<int>(2, 5000);

            await queue.Push(1);
            await queue.Push(2);

            // queue is full, so should wait until queue becomes less than max.
            var pushTask = queue.Push(3, true);
            await Task.Delay(50); //short simulated delay
            Assert.Equal<TaskStatus>(pushTask.Status, TaskStatus.WaitingForActivation);

            var pop = await queue.Pop();
            Assert.Equal(pop.Package, 1);
            Assert.Equal<ERealTimeQueueStatus>(pop.Status, ERealTimeQueueStatus.NotComplete);

            // queue should be available now, so allow push task to complete
            await pushTask;

            pop = await queue.Pop();
            Assert.Equal(pop.Package, 2);
            Assert.Equal<ERealTimeQueueStatus>(pop.Status, ERealTimeQueueStatus.NotComplete);

            pop = await queue.Pop();
            Assert.Equal(pop.Package, 3);
            Assert.Equal<ERealTimeQueueStatus>(pop.Status, ERealTimeQueueStatus.Complete);
        }

        [Fact]
        public async Task Test_QueueTimeout()
        {
            var queue = new RealTimeQueue<int>(2, 5000);

            await queue.Push(1);
            await queue.Push(2);
            await Assert.ThrowsAsync(typeof(RealTimeQueueTimeOutException), () => queue.Push(3, false, CancellationToken.None, 100));
        }

        [Fact]
        public async Task Test_QueuePushAfterFinished()
        {
            var queue = new RealTimeQueue<int>(2, 5000);

            await queue.Push(1);
            await queue.Push(2, true);
            await Assert.ThrowsAsync(typeof(RealTimeQueueFinishedException), () => queue.Push(3, false, CancellationToken.None, 100));
        }

        [Fact]
        public async Task Test_QueuePushExceeded()
        {
            var queue = new RealTimeQueue<int>(2, 5000);

            await queue.Push(1);
            await queue.Push(2);
            var push3 = queue.Push(3);
            await Task.Delay(50);
            var push4 = queue.Push(4);

            await Assert.ThrowsAsync(typeof(RealTimeQueuePushExceededException), () => queue.Push(4));
        }

        [Fact]
        public async Task Test_QueuePushCancelled()
        {
            var queue = new RealTimeQueue<int>(2, 5000);

            await queue.Push(1);
            await queue.Push(2);
            
            var cancelToken = new CancellationTokenSource();
            var pushTask = queue.Push(3, cancelToken.Token);
            cancelToken.Cancel();

            Assert.True(pushTask.Exception.InnerException is RealTimeQueueCancelledException);
        }

    }
}
