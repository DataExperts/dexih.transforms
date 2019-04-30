using Dexih.Utils.DataType;

namespace dexih.functions
{
    public class FunctionParameter
    {
        public string ParameterName { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public bool IsGeneric { get; set; }
        public DataType.ETypeCode DataType { get; set; }
        public bool AllowNull { get; set; }

        public int Rank { get; set; }
        public bool IsIndex { get; set; }
        public bool IsTwin { get; set; }
        public bool IsLabel { get; set; }
        
        public string[] ListOfValues { get; set; }
        public object DefaultValue { get; set; }
    }
}