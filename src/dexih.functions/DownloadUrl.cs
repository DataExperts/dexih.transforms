using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using MessagePack;

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

    [MessagePackObject]
    public class DownloadUrl
    {
        [Key(0)]
        public string Url { get; set; }

        [Key(1)]
        public EDownloadUrlType DownloadUrlType { get; set; }

        [Key(2)]
        public bool IsEncrypted { get; set; }
    }
}