namespace dexih.functions
{
    /// <summary>
    /// Contains information on an active remote agent.
    /// </summary>
    public class DexihActiveAgent
    {
        /// <summary>
        /// RemoteAgentKey reference in the repository
        /// </summary>
        public long RemoteAgentKey { get; set; }
        

        /// <summary>
        /// The public reference for the remote agent instance.
        /// </summary>
        public string InstanceId { get; set; }

        public string User { get; set; }
        public string Name { get; set; }
        public bool IsRunning { get; set; }
        public string IpAddress { get; set; }
        public bool IsEncrypted { get; set; }
        public EDataPrivacyStatus DataPrivacyStatus { get; set; }
        public DownloadUrl[] DownloadUrls { get; set; }
        
        public bool UpgradeAvailable { get; set; }
        public string Version { get; set; }
        public string LatestVersion { get; set; }
        public string LatestDownloadUrl { get; set; }

    }
}