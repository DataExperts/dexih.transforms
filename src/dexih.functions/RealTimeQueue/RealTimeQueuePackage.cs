using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace dexih.functions
{
    public enum ERealTimeQueueStatus
    {
        NotComplete,
        Complete,
        Cancalled,
        TimeOut,
        Error
    }

    public class RealTimeQueuePackage<T>
    {
        public RealTimeQueuePackage()
        {

        }

        public RealTimeQueuePackage(ERealTimeQueueStatus status)
        {
            Status = status;
        }

        public RealTimeQueuePackage(T package, ERealTimeQueueStatus status)
        {
            Package = package;
            Status = status;
        }

        public T Package { get; set; }
        public ERealTimeQueueStatus Status { get; set; }
    }
}
