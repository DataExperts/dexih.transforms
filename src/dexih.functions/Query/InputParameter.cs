using ProtoBuf;
using System;
using System.Text;

namespace dexih.repository
{
    [ProtoContract]
    public class InputParameter
    {
        [ProtoMember(1)]
        public string Name { get; set; }

        [ProtoMember(2)]
        public string Value { get; set; }
   
    }
}