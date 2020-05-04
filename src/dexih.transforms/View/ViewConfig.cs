using System.Runtime.Serialization;
using dexih.repository;
using dexih.repository.Properties;

namespace dexih.transforms.View
{
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EViewType
    {
        Table = 1, Chart
    }
    
    [DataContract]
    public class ViewConfig
    {
        [DataMember(Order = 0)]
        public EViewType ViewType { get; set; }
        
        [DataMember(Order = 1)]
        public ChartConfig ChartConfig { get; set; }
        
        [DataMember(Order = 2)]
        public AnimateConfig AnimateConfig { get; set; }
    }
}