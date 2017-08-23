using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.functions.Tasks
{
    public class ManagedTaskException : Exception
    {
        public ManagedTask ManagedTask { get; protected set; }

        public ManagedTaskException(ManagedTask managedTask)
        {
        }
        public ManagedTaskException(ManagedTask managedTask, string message) : base(message)
        {
        }
        public ManagedTaskException(ManagedTask managedTask, string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    public class ManagedTaskTriggerException: Exception
    {
        public ManagedTaskTrigger ManagedTaskTrigger { get; protected set; }

        public ManagedTaskTriggerException(ManagedTaskTrigger managedTaskTrigger)
        {
            ManagedTaskTrigger = managedTaskTrigger;
        }
        public ManagedTaskTriggerException(ManagedTaskTrigger managedTaskTrigger, string message) : base(message)
        {
            ManagedTaskTrigger = managedTaskTrigger;
        }
        public ManagedTaskTriggerException(ManagedTaskTrigger managedTaskTrigger, string message, Exception innerException) : base(message, innerException)
        {
            ManagedTaskTrigger = managedTaskTrigger;
        }
    }

}
