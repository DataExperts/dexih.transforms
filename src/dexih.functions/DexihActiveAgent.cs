using System.Runtime.Serialization;


namespace dexih.functions
{
    /// <summary>
    /// Contains information on an active remote agent.
    /// </summary>
    [DataContract]
    public class DexihActiveAgent
    {
        /// <summary>
        /// RemoteAgentKey reference in the repository
        /// </summary>
        [DataMember(Order = 0)]
        public long RemoteAgentKey { get; set; }


        /// <summary>
        /// The public reference for the remote agent instance.
        /// </summary>
        [DataMember(Order = 1)]
        public string InstanceId { get; set; }

        [DataMember(Order = 2)]
        public string User { get; set; }

        [DataMember(Order = 3)]
        public string Name { get; set; }

        [DataMember(Order = 4)]
        public bool IsRunning { get; set; }

        [DataMember(Order = 5)]
        public string IpAddress { get; set; }

        [DataMember(Order = 6)]
        public bool IsEncrypted { get; set; }

        [DataMember(Order = 7)]
        public EDataPrivacyStatus DataPrivacyStatus { get; set; }

        [DataMember(Order = 8)]
        public DownloadUrl[] DownloadUrls { get; set; }

        [DataMember(Order = 9)]
        public bool UpgradeAvailable { get; set; }

        [DataMember(Order = 10)]
        public string Version { get; set; }

        [DataMember(Order = 11)]
        public string LatestVersion { get; set; }

        [DataMember(Order = 12)]
        public string LatestDownloadUrl { get; set; }
        
        [DataMember(Order = 13)]
        public NamingStandards NamingStandards { get; set; }

        [DataMember(Order = 14)]
        public bool AutoUpgrade { get; set; }
    }
}