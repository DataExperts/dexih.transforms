using System.Globalization;
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

        /// <summary>
        /// Number of rows in at the start of the file to skip
        /// </summary>
        public int SkipHeaderRows { get; set; } = 0;

        /// <summary>
        /// Set empty cells to null (otherwise set to "" value)
        /// </summary>
        public bool SetWhiteSpaceCellsToNull { get; set; } = true;

        [JsonIgnore]
        public override CultureInfo CultureInfo { get => base.CultureInfo; set => base.CultureInfo = value; }

    }

}
