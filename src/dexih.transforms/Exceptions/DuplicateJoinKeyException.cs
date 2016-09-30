using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace dexih.transforms.Exceptions
{
    public class DuplicateJoinKeyException : Exception
    {
        public DuplicateJoinKeyException(string message, string joinTableAlias, object[] keyValue) : base(message)
        {
            JoinTableAlias = joinTableAlias;
            KeyValue = keyValue;
        }

        public string JoinTableAlias { get; set; }
        public object[] KeyValue { get; set; }
    }
}
