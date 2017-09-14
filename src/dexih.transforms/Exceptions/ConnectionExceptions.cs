using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.transforms.Exceptions
{
    public class ConnectionException : Exception
    {
#if DEBUG
        public object[] Values { get; set; }
#endif

        public ConnectionException()
        {
        }
        public ConnectionException(string message) : base(message)
        {
        }
        public ConnectionException(string message, Exception innerException, params object[] values) : base(message, innerException)
        {
#if DEBUG
            Values = values;
#endif
        }

        public ConnectionException(string message, params object[] values) : base(message)
        {
#if DEBUG
            Values = values;
#endif 
        }

        public override string Message
        {
            get
            {
#if DEBUG
                if (Data == null)
                {
                    return base.Message;
                }
                else
                {
                    return base.Message + ".  Data values: " + string.Join(",", Data);
                }
#else
                return base.Message;
#endif
            }
        }
    }
}
