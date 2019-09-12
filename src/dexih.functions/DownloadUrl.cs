using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;

namespace dexih.functions
{
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EDataPrivacyStatus
    {
        NotAllowed = 1,
        Proxy,
        Lan,
        Internet
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EDownloadUrlType
    {
        Proxy = 1,
        Direct
    }

    [ProtoContract]
    public class DownloadUrl
    {
        [ProtoMember(1)]
        public string Url { get; set; }

        [ProtoMember(2)]
        public EDownloadUrlType DownloadUrlType { get; set; }

        [ProtoMember(3)]
        public bool IsEncrypted { get; set; }
    }
}