using System;

namespace dexih.transforms
{
    public class FileHandlerException : Exception
    {
#if DEBUG
        public object[] Values { get; set; }
#endif

        public FileHandlerException()
        {
        }
        public FileHandlerException(string message) : base(message)
        {
        }
        public FileHandlerException(string message, Exception innerException, params object[] values) : base(message, innerException)
        {
#if DEBUG
            Values = values;
#endif
        }

        public FileHandlerException(string message, params object[] values) : base(message)
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
