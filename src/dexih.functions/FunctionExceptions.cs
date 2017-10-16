using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.functions
{
    public class FunctionException : Exception
    {
        public FunctionException()
        {
        }
        public FunctionException(string message) : base(message)
        {
        }
        public FunctionException(string message, Exception innerException): base(message, innerException)
		{
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
		public FunctionIgnoreRowException() : base()
		{
		}

		public FunctionIgnoreRowException(string message) : base(message)
		{
		}
	}

}
