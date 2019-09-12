using System.Globalization;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.functions.File
{

    /// <summary>
    /// reflects options used by the csvHelper = https://joshclose.github.io/CsvHelper/
    /// </summary>
    [ProtoContract]
    public class FileConfiguration : CsvHelper.Configuration.Configuration
    {
        public FileConfiguration()
        {
        }

        [ProtoMember(1)]
        public bool MatchHeaderRecord { get; set; } = true;

        /// <summary>
        /// Number of rows in at the start of the file to skip
        /// </summary>
        [ProtoMember(2)]
        public int SkipHeaderRows { get; set; } = 0;

        /// <summary>
        /// Set empty cells to null (otherwise set to "" value)
        /// </summary>
        [ProtoMember(3)]
        public bool SetWhiteSpaceCellsToNull { get; set; } = true;

        [JsonIgnore]
        public override CultureInfo CultureInfo { get => base.CultureInfo; set => base.CultureInfo = value; }

    }

}
