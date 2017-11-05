using System.Globalization;
using CsvHelper.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.connections.flatfile
{

    /// <summary>
    /// reflects options used by the csvHelper = https://joshclose.github.io/CsvHelper/
    /// </summary>
    public class FileConfiguration : CsvConfiguration
    {
        public FileConfiguration()
        {
        }
        private long HubKey { get; set; }

        public bool MatchHeaderRecord { get; set; } = true;

        [JsonIgnore]
        public override CultureInfo CultureInfo { get => base.CultureInfo; set => base.CultureInfo = value; }

    }

}
