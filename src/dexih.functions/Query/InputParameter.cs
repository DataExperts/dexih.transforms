using MessagePack;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class InputParameter
    {
        [Key(0)]
        public string Name { get; set; }

        [Key(1)]
        public string Value { get; set; }
   
    }
}