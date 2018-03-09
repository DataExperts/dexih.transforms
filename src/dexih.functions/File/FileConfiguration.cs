using System.Globalization;
using CsvHelper.Configuration;
using Newtonsoft.Json;

namespace dexih.functions.File
{

    /// <summary>
    /// reflects options used by the csvHelper = https://joshclose.github.io/CsvHelper/
    /// </summary>
    public class FileConfiguration : CsvHelper.Configuration.Configuration
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
