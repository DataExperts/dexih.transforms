using System.Globalization;
using System.Text.Json.Serialization;
using MessagePack;

namespace dexih.functions.File
{

    /// <summary>
    /// reflects options used by the csvHelper = https://joshclose.github.io/CsvHelper/
    /// </summary>
    [MessagePackObject]
    public class FileConfiguration : CsvHelper.Configuration.Configuration
    {
        public FileConfiguration()
        {
        }

        [Key(0)]
        public bool MatchHeaderRecord { get; set; } = true;

        /// <summary>
        /// Number of rows in at the start of the file to skip
        /// </summary>
        [Key(1)]
        public int SkipHeaderRows { get; set; } = 0;

        /// <summary>
        /// Set empty cells to null (otherwise set to "" value)
        /// </summary>
        [Key(2)]
        public bool SetWhiteSpaceCellsToNull { get; set; } = true;

        [JsonIgnore, IgnoreMember]
        public override CultureInfo CultureInfo { get => base.CultureInfo; set => base.CultureInfo = value; }

    }

}
