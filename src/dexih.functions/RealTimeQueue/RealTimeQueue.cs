﻿using System;
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
		private readonly ConcurrentQueue<T> _realtimeQueue;
        private readonly int _maxSize = 2;
        private readonly int _defaulttimeOutMilliseconds = 5000; //default timeout 5seconds

        private readonly AutoResetEventAsync _popEvent = new AutoResetEventAsync();
        private readonly AutoResetEventAsync _pushEvent = new AutoResetEventAsync();
        // private readonly AsyncAutoResetEvent _cancelEvent = new AsyncAutoResetEvent();

        public bool IsCancelled { get; set; } = false;
        public bool IsFailed { get; set; } = false;
        public bool IsFinished { get; set; }

        public string Message { get; set; }
        public Exception Exception { get; set; }

        private bool _awaitingPush;

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
	    /// <param name="isFinalBuffer"></param>
	    /// <param name="cancellationToken"></param>
	    /// <param name="timeOutMilliseconds"></param>
	    /// <returns></returns>
	    public async Task Push(T buffer, bool isFinalBuffer, CancellationToken cancellationToken, int timeOutMilliseconds)
        {
            try
            {
                if(IsFinished)
                {
                    throw new RealTimeQueueFinishedException("The push operation was attempted after the queue was marked as finished.");
                }

                // if the buffer is full, wait until something is popped
                while (_realtimeQueue.Count >= _maxSize)
                {
                    if(_awaitingPush)
                    {
                        throw new RealTimeQueuePushExceededException("The push operation failed, as the buffer is at max capacity, and another task is waiting to push a buffer.");
                    }

                    _awaitingPush = true;

                    var popEvent = _popEvent.WaitAsync();
                    var timeoutEvent = Task.Delay(timeOutMilliseconds, cancellationToken);

                    var completedTask = await Task.WhenAny(popEvent, timeoutEvent);

                    _awaitingPush = false;

                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new RealTimeQueueCancelledException("The push operation was cancelled");
                    }

                    if (completedTask == timeoutEvent)
                    {
                        throw new RealTimeQueueTimeOutException($"The push operation timed out after {timeOutMilliseconds} milliseconds.");
                    }
                }

                _realtimeQueue.Enqueue(buffer);
                IsFinished = isFinalBuffer;

                _pushEvent.Set();
            }
            catch (Exception ex)
            when(!(ex is RealTimeQueueCancelledException || ex is RealTimeQueueFinishedException || ex is RealTimeQueueTimeOutException || ex is RealTimeQueuePushExceededException))
            {
                throw new RealTimeQueueException("The push operation failed.  See inner exception for details.", ex);
            }
        }

        public void SetError(string message, Exception exception)
        {
            IsFailed = true;
            IsFinished = true;
            Message = message;

            if (exception == null)
            {
                Exception = new RealTimeQueueException(message);
            }
            else
            {
                Exception = exception;
            }

            if (_pushEvent != null) _pushEvent.Set();
        }

        public Task<RealTimeQueuePackage<T>> Pop()
        {
            return Pop(CancellationToken.None, _defaulttimeOutMilliseconds);
        }

        public Task<RealTimeQueuePackage<T>> Pop(CancellationToken cancellationToken)
        {
            return Pop(cancellationToken, _defaulttimeOutMilliseconds);
        }

        public async Task<RealTimeQueuePackage<T>> Pop(CancellationToken cancellationToken, int timeOutMilliseconds)
        {
            try
            {
                while (_realtimeQueue.Count == 0)
                {
                    if (IsFinished)
                    {
                        return new RealTimeQueuePackage<T>(ERealTimeQueueStatus.Complete);
                    }

                    var pushEvent = _pushEvent.WaitAsync();

                    if(IsFailed)
                    {
                        throw Exception;
                    }

                    var timeoutEvent = Task.Delay(timeOutMilliseconds, cancellationToken);

                    var completedTask = await Task.WhenAny(pushEvent, timeoutEvent);

                    if (cancellationToken.IsCancellationRequested)
                    {
                        return new RealTimeQueuePackage<T>(ERealTimeQueueStatus.Cancalled);
                    }

                    if (completedTask == timeoutEvent)
                    {
                        throw new RealTimeQueueTimeOutException($"The pull operation timed out after {timeOutMilliseconds} milliseconds.");
                    }
                }

                while (true)
                {
                    if (IsFailed)
                    {
                        throw Exception;
                    }

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
                    await Task.Delay(100, cancellationToken);
                }
            } catch(Exception ex)
            when (!(ex is RealTimeQueueCancelledException || ex is RealTimeQueueFinishedException || ex is RealTimeQueueTimeOutException))
            {
                throw new RealTimeQueueException("The pull operation failed.  See inner exception for details.", ex);
            }
        }
    }
}
