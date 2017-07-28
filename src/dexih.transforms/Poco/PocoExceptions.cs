using System;

namespace dexih.transforms
{

    public class PocoException : Exception
    {
        public PocoException() : base()
        {
        }
        public PocoException(string message) : base(message)
        {
        }
        public PocoException(string message, Exception innerException): base(message, innerException)
        {
        }
    }

    public class PocoLoaderClosedException : PocoException
    {
        public PocoLoaderClosedException(string message) : base(message)
        {
        }
    }
    
    public class PocoLoaderNoResetException : PocoException
    {
        public PocoLoaderNoResetException(string message) : base(message)
        {
        }
    }
    
    public class PocoLoaderIndexOutOfBoundsException : PocoException
    {
        public PocoLoaderIndexOutOfBoundsException(string message) : base(message)
        {
        }
    }
}