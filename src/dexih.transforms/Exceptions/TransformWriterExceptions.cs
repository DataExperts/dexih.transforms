using System;

namespace dexih.transforms.Exceptions
{
    public class TransformWriterException : Exception
    {
#if DEBUG
        public object[] Values { get; set; }
#endif

        public TransformWriterException()
        {
        }
        public TransformWriterException(string message) : base(message)
        {
        }
        public TransformWriterException(string message, Exception innerException, params object[] values) : base(message, innerException)
        {
#if DEBUG
            Values = values;
#endif
        }

        public TransformWriterException(string message, params object[] values) : base(message)
        {
#if DEBUG
            Values = values;
#endif 
        }

        public override string Message {
            get
            {
#if DEBUG
                if (Values == null)
                {
                    return base.Message;
                }
                else
                {
                    return base.Message + ".  Data values: " + string.Join(",", Values);
                }
#else
                return base.Message;
#endif
            }
        }
    }
}
