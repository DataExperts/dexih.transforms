using System.Runtime.Serialization;


namespace dexih.functions.Query
{
    [DataContract]
    public class InputParameter
    {
        [DataMember(Order = 0)]
        public string Name { get; set; }

        [DataMember(Order = 1)]
        public string Value { get; set; }
   
    }
}