using System.Collections.Generic;
using MessagePack;

namespace dexih.transforms
{
    /// <summary>
    /// Used to transmit data previews
    /// </summary>
    [MessagePackObject]
    public class DataPack
    {
        [Key("name")]
        public string Name { get; set; }

        [Key("columns")]
        public DataPackColumn[] Columns { get; set; }

        [Key("data")]
        public List<object[]> Data { get; set; } = new List<object[]>();
        
        [Key("transformProperties")]
        public TransformProperties TransformProperties { get; set; }
    }
}