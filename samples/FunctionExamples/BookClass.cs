using System;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.Poco;

namespace FunctionExamples
{
    public class BookClass
    {
        [Field("code", DeltaType = TableColumn.EDeltaType.NaturalKey)]
        public string Code { get; set; }

        [Field("name")]
        public string Name { get; set; }

        [Field("name")]
        public int Cost { get; set; }

        [Field("date_published")]
        public DateTime Published { get; set; }
    
    }
}