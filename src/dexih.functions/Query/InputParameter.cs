using MessagePack;
using System;
using System.Text;

namespace dexih.repository
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