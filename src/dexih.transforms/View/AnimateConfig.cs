using System.Runtime.Serialization;

namespace dexih.repository.Properties
{
    [DataContract]
    public class AnimateConfig
    {
        [DataMember(Order = 0)]
        public string SeriesColumn { get; set; }

        [DataMember(Order = 1)] 
        public bool Automatic { get; set; } = true;

        [DataMember(Order = 2)] 
        public int Delay { get; set; } = 500;

    }
}