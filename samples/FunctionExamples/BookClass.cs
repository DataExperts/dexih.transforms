using System;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.Poco;

namespace FunctionExamples
{
    public class BookClass
    {
        [PocoColumn("code", DeltaType = TableColumn.EDeltaType.NaturalKey)]
        public string Code { get; set; }

        [PocoColumn("name")]
        public string Name { get; set; }

        [PocoColumn("name")]
        public int Cost { get; set; }

        [PocoColumn("date_published")]
        public DateTime Published { get; set; }
    
    }
}