using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace dexih.functions.tests
{
    public class FunctionInitializers
    {
        [Fact]
        public void FunctionFromDelegate()
        {
            //create a custom function
            var globalVariable = new GlobalVariables(null);
            TransformFunction function1 = new TransformFunction(new Func<int, int, int>((i, j) => i + j), globalVariable);
            Assert.True((Int32)function1.Invoke(new object[] { 6, 2 }) == 8);
        }

        [Fact]
        public void FunctionFromMethod()
        {
            //create a custom function
            var globalVariable = new GlobalVariables(null);
            TransformFunction function1 = new TransformFunction(
                this, 
                "TestMethod", 
                globalVariable);
            Assert.True((Int32)function1.Invoke(new object[] { 6, 2 }) == 8);
        }

        [Fact]
        public void FunctionFromReflection()
        {
            //create a custom function
            TransformFunction function1 = new TransformFunction(this, this.GetType().GetMethod("TestMethod"), new GlobalVariables(null));
            Assert.True((Int32)function1.Invoke(new object[] { 6, 2 }) == 8);
        }

        public int TestMethod(int a, int b) => a + b;
    }
}
