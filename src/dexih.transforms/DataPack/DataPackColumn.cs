using System.Linq;
using dexih.functions;
using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.transforms
{
    [MessagePackObject]
    public class DataPackColumn
    {
        public DataPackColumn()
        {

        }

        public DataPackColumn(TableColumn column)
        {
            Name = column.Name;
            LogicalName = column.LogicalName;
            DataType = column.DataType;
            ChildColumns = column.ChildColumns?.Select(c => new DataPackColumn(c)).ToArray();
        }

        [Key("name")]
        public string Name { get; set; }

        [Key("logicalName")]
        public string LogicalName { get; set; }

        [Key("dataType")]
        public ETypeCode DataType { get; set; }

        [Key("childColumns")]
        public DataPackColumn[] ChildColumns;
    }
}