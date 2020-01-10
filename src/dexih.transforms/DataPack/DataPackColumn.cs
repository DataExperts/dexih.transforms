using System.Linq;
using System.Runtime.Serialization;
using dexih.functions;
using Dexih.Utils.DataType;


namespace dexih.transforms
{
    [DataContract]
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

        [DataMember(Order = 1)]
        public string Name { get; set; }

        [DataMember(Order = 2)]
        public string LogicalName { get; set; }

        [DataMember(Order = 3)]
        public ETypeCode DataType { get; set; }

        [DataMember(Order = 4)]
        public DataPackColumn[] ChildColumns;
    }
}