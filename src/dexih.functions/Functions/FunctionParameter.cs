using System;
using Dexih.Utils.DataType;
using ProtoBuf;

namespace dexih.functions
{
    [ProtoContract]
    public class FunctionParameter
    {
        [ProtoMember(1)]
        public string ParameterName { get; set; }

        [ProtoMember(2)]
        public string Name { get; set; }

        [ProtoMember(3)]
        public string Description { get; set; }

        [ProtoMember(4)]
        public bool IsGeneric { get; set; }

        [ProtoMember(5)]
        public DataType.ETypeCode DataType { get; set; }

        [ProtoMember(6)]
        public bool AllowNull { get; set; }

        [ProtoMember(7)]
        public int Rank { get; set; }

        [ProtoMember(8)]
        public bool IsIndex { get; set; }

        [ProtoMember(9)]
        public string LinkedName { get; set; }

        [ProtoMember(10)]
        public string LinkedDescription { get; set; }

        [ProtoMember(11)]
        public bool IsLabel { get; set; }

        [ProtoMember(12)]
        public string[] ListOfValues { get; set; }

        [ProtoMember(13)]
        public string DefaultValue { get; set; }
        
    }
}