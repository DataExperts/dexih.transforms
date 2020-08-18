using System;

namespace dexih.transforms.Exceptions
{
    public class InvalidJoinStrategyException : Exception
    {
        public InvalidJoinStrategyException(string message) : base(message)
        {
        }
    }
}
