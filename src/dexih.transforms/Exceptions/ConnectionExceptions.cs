using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

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
                if (Values == null || Values.Length == 0)
                {
                    return base.Message;
                }
                else
                {
                    return string.Format("{0}.  Data values: {1}", base.Message, String.Join(", ", Values));
                }
#else
                return base.Message;
#endif
            }
        }
    }
}
