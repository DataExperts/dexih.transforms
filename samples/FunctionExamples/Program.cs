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
            while (true)
            {
                Console.WriteLine();
                Console.WriteLine("1. Create a reader. ");
                Console.WriteLine("q. Quit. ");

                var key = Console.ReadKey();

                switch (key.KeyChar)
                {
                    case '1':
                        var poco = new CreatePocoReader();
                        poco.Create();
                        break;
                    case 'q':
                        return;

                }
            }
        }
    }
}
