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

            Function custom = new Function("CustomColumn", false, "Test", "return value1 + value2.ToString();", null, ETypeCode.String,
                new dexih.functions.Parameter[] {
                    new dexih.functions.Parameter("value1", ETypeCode.String, false),
                    new dexih.functions.Parameter("value2", ETypeCode.Int32, false),
                }, null);
            Console.WriteLine(custom.CreateFunctionMethod().Success);
            Console.WriteLine((string)custom.RunFunction(new object[] { "abc", "123" }).Value);
            Console.WriteLine((bool)custom.RunFunction(new object[] { "123", "abc" }).Success);

            Console.Read();
        }
    }
}
