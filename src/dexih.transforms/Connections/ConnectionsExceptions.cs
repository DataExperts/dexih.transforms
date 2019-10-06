using System;

namespace dexih.transforms
{
    public class ConnectionNotFoundException : Exception
    {
        public ConnectionNotFoundException()
        {
        }

        public ConnectionNotFoundException(string message) : base(message)
        {
        }
    }
}