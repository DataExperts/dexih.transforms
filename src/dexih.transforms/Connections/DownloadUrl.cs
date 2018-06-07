using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.transforms
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EDataPrivacyStatus
    {
        NotAllowed,
        Proxy,
        Lan,
        Internet
    }

    [JsonConverter(typeof(StringEnumConverter))]
    public enum EDownloadUrlType
    {
        Proxy,
        Direct
    }

    public class DownloadUrl
    {
        public string Url { get; set; }
        public EDownloadUrlType DownloadUrlType { get; set; }
        public bool IsEncrypted { get; set; }
    }
}