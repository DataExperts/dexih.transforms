using ProtoBuf;

namespace dexih.functions
{
    /// <summary>
    /// Contains information on an active remote agent.
    /// </summary>
    [ProtoContract]
    public class DexihActiveAgent
    {
        /// <summary>
        /// RemoteAgentKey reference in the repository
        /// </summary>
        [ProtoMember(1)]
        public long RemoteAgentKey { get; set; }


        /// <summary>
        /// The public reference for the remote agent instance.
        /// </summary>
        [ProtoMember(2)]
        public string InstanceId { get; set; }

        [ProtoMember(3)]
        public string User { get; set; }

        [ProtoMember(4)]
        public string Name { get; set; }

        [ProtoMember(5)]
        public bool IsRunning { get; set; }

        [ProtoMember(6)]
        public string IpAddress { get; set; }

        [ProtoMember(7)]
        public bool IsEncrypted { get; set; }

        [ProtoMember(8)]
        public EDataPrivacyStatus DataPrivacyStatus { get; set; }

        [ProtoMember(9)]
        public DownloadUrl[] DownloadUrls { get; set; }

        [ProtoMember(10)]
        public bool UpgradeAvailable { get; set; }

        [ProtoMember(11)]
        public string Version { get; set; }

        [ProtoMember(12)]
        public string LatestVersion { get; set; }

        [ProtoMember(13)]
        public string LatestDownloadUrl { get; set; }

    }
}