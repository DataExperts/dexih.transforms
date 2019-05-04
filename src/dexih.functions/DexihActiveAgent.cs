namespace dexih.functions
{
    /// <summary>
    /// Contains information on an active remote agent.
    /// </summary>
    public class DexihActiveAgent
    {
        public long RemoteAgentKey { get; set; }
        public string User { get; set; }
        public string Name { get; set; }
        public bool IsRunning { get; set; }
        public string IpAddress { get; set; }
        public string InstanceId { get; set; }
        public bool IsEncrypted { get; set; }
        public EDataPrivacyStatus DataPrivacyStatus { get; set; }
        public DownloadUrl[] DownloadUrls { get; set; }
        
        public bool UpgradeAvailable { get; set; }
        public string Version { get; set; }
        public string LatestVersion { get; set; }
        public string LatestDownloadUrl { get; set; }

    }
}