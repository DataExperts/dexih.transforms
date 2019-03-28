using System;

namespace dexih.functions.Exceptions
{
    public class FunctionException : Exception
    {
#if DEBUG
        public object[] Values { get; set; }
#endif

        public FunctionException()
        {
        }
        public FunctionException(string message) : base(message)
        {
        }
        public FunctionException(string message, Exception innerException, params object[] values) : base(message, innerException)
        {
#if DEBUG
            Values = values;
#endif
        }

        public FunctionException(string message, params object[] values) : base(message)
        {
#if DEBUG
            Values = values;
#endif 
        }

        public override string Message {
            get
            {
#if DEBUG
                if (Values == null  || Values.Length == 0)
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
    
    public class FunctionInvalidParametersException: FunctionException
    {
        public FunctionInvalidParametersException(string message) : base(message)
        {
        }
    }

    public class FunctionInvalidDataTypeException : FunctionException
    {
        public FunctionInvalidDataTypeException(string message) : base(message)
        {
        }
    }

    public class FunctionNullValueException : FunctionException
    {
        public FunctionNullValueException(string message) : base(message)
        {
        }
    }

    public class FunctionIgnoreRowException : FunctionException
    {
        public FunctionIgnoreRowException()
        {
        }

        public FunctionIgnoreRowException(string message) : base(message)
        {
        }
    }

    public class FunctionNotFoundException : FunctionException
    {
        public FunctionNotFoundException() : base()
        {
        }

        public FunctionNotFoundException(string message) : base(message)
        {
        }
    }
}
