using System;
using dexih.functions;
using dexih.transforms;

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