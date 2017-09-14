using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.functions.Query
{
    public class QueryException: Exception
    {
        public QueryException()
        {
        }
        public QueryException(string message) : base(message)
        {
        }
        public QueryException(string message, Exception innerException): base(message, innerException)
		{
        }
    }
}
