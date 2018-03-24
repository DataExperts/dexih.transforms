using System;
using System.Collections.Generic;
using System.Diagnostics;
using dexih.functions;
using dexih.standard.functions;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.Crypto;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities.Serialization;
using Newtonsoft.Json;
using Xunit;

namespace dexih.standard.function.tests
{
    public class MapFunctionTests
    {
        [Fact]
        public void GetFunctions()
        {
          
            var functions = new Functions();
            var mapFunctions = functions.GetMapFunctions();

            foreach (var mapFunction in mapFunctions)
            {
                Debug.WriteLine($"Function: " + mapFunction.Name);
            }

            var json = JsonConvert.SerializeObject(mapFunctions);
            Debug.WriteLine(json);
        }
    }
}