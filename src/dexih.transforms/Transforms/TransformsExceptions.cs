using System;

namespace dexih.transforms
{
    public class TransformnNotFoundException : Exception
    {
        public TransformnNotFoundException()
        {
        }

        public TransformnNotFoundException(string message) : base(message)
        {
        }
    }
}