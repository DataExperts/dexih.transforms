using MessagePack;

namespace dexih.functions
{
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