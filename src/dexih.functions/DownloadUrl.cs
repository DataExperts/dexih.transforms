using System.Runtime.Serialization;

namespace dexih.functions
{
    [DataContract]
    public class DownloadUrl
    {
        public DownloadUrl()
        {
            
        }
        public DownloadUrl(string url, EDownloadUrlType downloadUrlType, bool isEncrypted)
        {
            Url = url;
            DownloadUrlType = downloadUrlType;
            IsEncrypted = isEncrypted;
        }
        
        [DataMember(Order = 0)]
        public string Url { get; set; }

        [DataMember(Order = 1)]
        public EDownloadUrlType DownloadUrlType { get; set; }

        [DataMember(Order = 2)]
        public bool IsEncrypted { get; set; }
    }
}