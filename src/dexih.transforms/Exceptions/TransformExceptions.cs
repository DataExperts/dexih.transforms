using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.transforms.Exceptions
{
    public class TransformException : Exception
    {
#if DEBUG
        public object[] Values { get; set; }
#endif

        public TransformException()
        {
        }
        public TransformException(string message) : base(message)
        {
        }
        public TransformException(string message, Exception innerException, params object[] values) : base(message, innerException)
        {
#if DEBUG
            Values = values;
#endif
        }

        public TransformException(string message, params object[] values) : base(message)
        {
#if DEBUG
            Values = values;
#endif 
        }

        public override string Message {
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
