using System;

namespace dexih.transforms
{
    public class ConnectionNotFoundException : Exception
    {
        public ConnectionNotFoundException() : base()
        {
        }

        public ConnectionNotFoundException(string message) : base(message)
        {
        }
    }
}