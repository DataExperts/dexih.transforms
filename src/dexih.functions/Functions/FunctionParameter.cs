using System.Runtime.Serialization;
using Dexih.Utils.DataType;


namespace dexih.functions
{
    [DataContract]
    public class FunctionParameter
    {
        [DataMember(Order = 0)]
        public string ParameterName { get; set; }

        [DataMember(Order = 1)]
        public string Name { get; set; }

        [DataMember(Order = 2)]
        public string Description { get; set; }

        [DataMember(Order = 3)]
        public bool IsGeneric { get; set; }

        [DataMember(Order = 4)]
        public ETypeCode DataType { get; set; }

        [DataMember(Order = 5)]
        public bool AllowNull { get; set; }

        [DataMember(Order = 6)]
        public int Rank { get; set; }

        [DataMember(Order = 7)]
        public bool IsIndex { get; set; }

        [DataMember(Order = 8)]
        public string LinkedName { get; set; }

        [DataMember(Order = 9)]
        public string LinkedDescription { get; set; }

        [DataMember(Order = 10)]
        public bool IsLabel { get; set; }

        [DataMember(Order = 11)]
        public string[] ListOfValues { get; set; }

        [DataMember(Order = 12)]
        public string DefaultValue { get; set; }
        
        [DataMember(Order = 13)]
        public bool IsPassword { get; set; }
        
    }
}