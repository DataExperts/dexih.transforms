using dexih.functions;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.functions {

    /// <summary>
    /// RealTimeQueue
    /// Details: 
    /// This class allows data to be sent from disonnected objects via a data buffer, through a push/pull mechanism.
    /// </summary>
	public class RealTimeQueue<T>
	{
		private ConcurrentQueue<T> _realtimeQueue;
        private int _maxSize = 2;
        private int _defaulttimeOutMilliseconds = 5000; //default timeout 5seconds

        private AsyncAutoResetEvent _popEvent = new AsyncAutoResetEvent();
        private AsyncAutoResetEvent _pushEvent = new AsyncAutoResetEvent();
        private AsyncAutoResetEvent _cancelEvent = new AsyncAutoResetEvent();

        public bool IsCancelled { get; set; } = false;
        public bool IsFinished { get; set; } = false;

        private bool awaitingPush = false;

        public RealTimeQueue()
        {
        }

        public RealTimeQueue(int maxSize)
		{
			_realtimeQueue = new ConcurrentQueue<T>();
            _maxSize = maxSize;
		}

        public RealTimeQueue(int maxSize, int defaultTimeOutMilliseconds)
        {
            _realtimeQueue = new ConcurrentQueue<T>();
            _maxSize = maxSize;
            _defaulttimeOutMilliseconds = defaultTimeOutMilliseconds;
        }
        /// <summary>
        /// Issues  a cancel event that will stop any push/pull operations.
        /// </summary>
        public void Cancel()
        {
            IsCancelled = true;
            _cancelEvent.Set();
        }

        public Task Push(T buffer)
        {
            return Push(buffer, false, CancellationToken.None, _defaulttimeOutMilliseconds);
        }

        public Task Push(T buffer, bool isFinalBuffer)
        {
            return Push(buffer, isFinalBuffer, CancellationToken.None, _defaulttimeOutMilliseconds);
        }

        public Task Push(T buffer, CancellationToken cancellationToken)
        {
            return Push(buffer, false, cancellationToken, _defaulttimeOutMilliseconds);
        }

        public Task Push(T buffer, bool isFinalBuffer, CancellationToken cancellationToken)
        {
            return Push(buffer, isFinalBuffer, cancellationToken, _defaulttimeOutMilliseconds);
        }

        /// <summary>
        /// Push data to the buffer.  If the buffer queue is great than the max buffers the function will wait until a buffer has been cleared before acepting the new buffer.
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="TimeOutMilliseconds"></param>
        /// <returns></returns>
        public async Task Push(T buffer, bool isFinalBuffer, CancellationToken cancellationToken, int TimeOutMilliseconds)
        {
            try
            {
                cancellationToken.Register(() => Cancel());

                if(IsFinished)
                {
                    throw new RealTimeQueueFinishedException("The push operation was attempted after the queue was marked as finished.");
                }

                // if the buffer is full, wait until something is popped
                while (_realtimeQueue.Count >= _maxSize)
                {
                    if(awaitingPush)
                    {
                        throw new RealTimeQueuePushExceededException("The push operation failed, as the buffer is at max capacity, and another task is waiting to push a buffer.");
                    }

                    awaitingPush = true;

                    var popEvent = _popEvent.WaitAsync();
                    var cancelEvent = _cancelEvent.WaitAsync();
                    var timeoutEvent = Task.Delay(TimeOutMilliseconds);


                    var completedTask = await Task.WhenAny(popEvent, cancelEvent, timeoutEvent);

                    awaitingPush = false;

                    if (completedTask == cancelEvent)
                    {
                        throw new RealTimeQueueCancelledException("The push operation was cancelled");
                    }

                    if (completedTask == timeoutEvent)
                    {
                        throw new RealTimeQueueTimeOutException($"The push operation timed out after {TimeOutMilliseconds.ToString()} milliseconds.");
                    }
                }

                _realtimeQueue.Enqueue(buffer);
                IsFinished = isFinalBuffer;

                _pushEvent.Set();

                return;
            }
            catch (Exception ex)
            when(!(ex is RealTimeQueueCancelledException || ex is RealTimeQueueFinishedException || ex is RealTimeQueueTimeOutException || ex is RealTimeQueuePushExceededException))
            {
                throw new RealTimeQueueException("The push operation failed.  See inner exception for details.", ex);
            }
        }

        public Task<RealTimeQueuePackage<T>> Pop()
        {
            return Pop(CancellationToken.None, _defaulttimeOutMilliseconds);
        }

        public Task<RealTimeQueuePackage<T>> Pop(CancellationToken cancellationToken)
        {
            return Pop(cancellationToken, _defaulttimeOutMilliseconds);
        }

        public async Task<RealTimeQueuePackage<T>> Pop(CancellationToken cancellationToken, int TimeOutMilliseconds)
        {
            try
            {
                cancellationToken.Register(() => Cancel());

                while (_realtimeQueue.Count == 0)
                {
                    if (IsFinished)
                    {
                        return new RealTimeQueuePackage<T>(ERealTimeQueueStatus.Complete);
                    }

                    var pushEvent = _pushEvent.WaitAsync();
                    var cancelEvent = _cancelEvent.WaitAsync();
                    var timeoutEvent = Task.Delay(TimeOutMilliseconds);

                    var completedTask = await Task.WhenAny(pushEvent, cancelEvent, timeoutEvent);

                    if (completedTask == cancelEvent)
                    {
                        return new RealTimeQueuePackage<T>(ERealTimeQueueStatus.Cancalled);
                    }

                    if (completedTask == timeoutEvent)
                    {
                        throw new RealTimeQueueTimeOutException($"The pull operation timed out after {TimeOutMilliseconds.ToString()} milliseconds.");
                    }
                }

                while (true)
                {
                    var canDequeue = _realtimeQueue.TryDequeue(out T result);
                    if (canDequeue)
                    {
                        ERealTimeQueueStatus status;
                        if (IsFinished && _realtimeQueue.Count == 0)
                        {
                            status = ERealTimeQueueStatus.Complete;
                        }
                        else
                        {
                            status = ERealTimeQueueStatus.NotComplete;
                        }
                        var package = new RealTimeQueuePackage<T>(result, status);
                        _popEvent.Set();
                        return package;
                    }
                    await Task.Delay(100);
                }
            } catch(Exception ex)
            when (!(ex is RealTimeQueueCancelledException || ex is RealTimeQueueFinishedException || ex is RealTimeQueueTimeOutException))
            {
                throw new RealTimeQueueException("The pull operation failed.  See inner exception for details.", ex);
            }
        }
    }
}
