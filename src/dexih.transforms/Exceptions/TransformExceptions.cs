using System;
using dexih.functions.Exceptions;

namespace dexih.transforms.Exceptions
{
    public class TransformException : FunctionException
    {

        public TransformException()
        {
        }
        public TransformException(string message) : base(message)
        {
        }
        public TransformException(string message, Exception innerException, params object[] values) : base(message, innerException)
        {
#if DEBUG
            Values = values;
#endif
        }

        public TransformException(string message, params object[] values) : base(message)
        {
#if DEBUG
            Values = values;
#endif 
        }
    }
}
