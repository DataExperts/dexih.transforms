using System.Globalization;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;


namespace dexih.transforms.File
{

    /// <summary>
    /// reflects options used by the csvHelper = https://joshclose.github.io/CsvHelper/
    /// </summary>
    [DataContract]
    public class FileConfiguration : CsvHelper.Configuration.CsvConfiguration
    {
        public FileConfiguration(): base(CultureInfo.CurrentCulture)
        {
        }

        [DataMember(Order = 0)]
        public bool MatchHeaderRecord { get; set; } = true;

        /// <summary>
        /// Number of rows in at the start of the file to skip
        /// </summary>
        [DataMember(Order = 1)]
        public int SkipHeaderRows { get; set; } = 0;

        /// <summary>
        /// Set empty cells to null (otherwise set to "" value)
        /// </summary>
        [DataMember(Order = 2)]
        public bool SetWhiteSpaceCellsToNull { get; set; } = true;

        [JsonIgnore, IgnoreDataMember]
        public override CultureInfo CultureInfo { get => base.CultureInfo; set => base.CultureInfo = value; }
        
    }

}
