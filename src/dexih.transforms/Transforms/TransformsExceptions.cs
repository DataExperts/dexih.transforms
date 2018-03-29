using System;

namespace dexih.transforms
{
    public class TransformnNotFoundException : Exception
    {
        public TransformnNotFoundException() : base()
        {
        }

        public TransformnNotFoundException(string message) : base(message)
        {
        }
    }
}