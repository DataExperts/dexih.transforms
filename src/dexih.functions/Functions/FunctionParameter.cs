using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.functions
{
    [MessagePackObject]
    public class FunctionParameter
    {
        [Key(0)]
        public string ParameterName { get; set; }

        [Key(1)]
        public string Name { get; set; }

        [Key(2)]
        public string Description { get; set; }

        [Key(3)]
        public bool IsGeneric { get; set; }

        [Key(4)]
        public ETypeCode DataType { get; set; }

        [Key(5)]
        public bool AllowNull { get; set; }

        [Key(6)]
        public int Rank { get; set; }

        [Key(7)]
        public bool IsIndex { get; set; }

        [Key(8)]
        public string LinkedName { get; set; }

        [Key(9)]
        public string LinkedDescription { get; set; }

        [Key(10)]
        public bool IsLabel { get; set; }

        [Key(11)]
        public string[] ListOfValues { get; set; }

        [Key(12)]
        public string DefaultValue { get; set; }
        
        [Key(13)]
        public bool IsPassword { get; set; }
        
    }
}