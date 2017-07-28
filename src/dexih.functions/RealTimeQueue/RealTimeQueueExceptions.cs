using System;

namespace dexih.functions
{
    public class RealTimeQueueException : Exception
    {
        public RealTimeQueueException()
        {
        }
        public RealTimeQueueException(string message) : base(message)
        {
        }
        public RealTimeQueueException(string message, Exception innerException): base(message, innerException)
		{
        }
    }

    public class RealTimeQueueTimeOutException: RealTimeQueueException
    {
        public RealTimeQueueTimeOutException(string message) : base(message)
        {
        }
    }

    public class RealTimeQueueCancelledException : RealTimeQueueException
    {
        public RealTimeQueueCancelledException(string message) : base(message)
        {
        }
    }

    public class RealTimeQueueFinishedException : RealTimeQueueException
    {
        public RealTimeQueueFinishedException(string message) : base(message)
        {
        }
    }

    public class RealTimeQueuePushExceededException : RealTimeQueueException
    {
        public RealTimeQueuePushExceededException(string message) : base(message)
        {
        }
    }
}
