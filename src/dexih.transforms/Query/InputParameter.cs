using System.Runtime.Serialization;


namespace dexih.functions.Query
{
    [DataContract]
    public class InputParameter
    {
        [DataMember(Order = 0)]
        public string Name { get; set; }

        [DataMember(Order = 1)]
        public object Value { get; set; }
        
        [DataMember(Order = 2)]
        public int Rank { get; set; }
   
    }
}