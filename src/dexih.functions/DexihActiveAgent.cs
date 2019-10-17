using MessagePack;

namespace dexih.functions
{
    /// <summary>
    /// Contains information on an active remote agent.
    /// </summary>
    [MessagePackObject]
    public class DexihActiveAgent
    {
        /// <summary>
        /// RemoteAgentKey reference in the repository
        /// </summary>
        [Key(0)]
        public long RemoteAgentKey { get; set; }


        /// <summary>
        /// The public reference for the remote agent instance.
        /// </summary>
        [Key(1)]
        public string InstanceId { get; set; }

        [Key(2)]
        public string User { get; set; }

        [Key(3)]
        public string Name { get; set; }

        [Key(4)]
        public bool IsRunning { get; set; }

        [Key(5)]
        public string IpAddress { get; set; }

        [Key(6)]
        public bool IsEncrypted { get; set; }

        [Key(7)]
        public EDataPrivacyStatus DataPrivacyStatus { get; set; }

        [Key(8)]
        public DownloadUrl[] DownloadUrls { get; set; }

        [Key(9)]
        public bool UpgradeAvailable { get; set; }

        [Key(10)]
        public string Version { get; set; }

        [Key(11)]
        public string LatestVersion { get; set; }

        [Key(12)]
        public string LatestDownloadUrl { get; set; }
        
        [Key(13)]
        public NamingStandards NamingStandards { get; set; }

    }
}