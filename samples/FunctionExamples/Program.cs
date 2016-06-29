using dexih.functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static dexih.functions.DataType;

namespace FunctionExamples
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Parameter parameter = new dexih.functions.Parameter("test", ETypeCode.Boolean);
            parameter.SetValue("true");

            Console.WriteLine(parameter.Value);

            Console.Read();
        }
    }
}
