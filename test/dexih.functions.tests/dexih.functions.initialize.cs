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
            Function function1 = new Function(new Func<int, int, int>((i, j) => i + j), new string[] { "value1", "value2" }, "Add", null);
            Assert.True((Int32)function1.RunFunction(new object[] { 6, 2 }).Value == 8);
        }

        [Fact]
        public void FunctionFromMethod()
        {
            //create a custom function
            Function function1 = new Function(this, "TestMethod", "", "", null, "", null);
            Assert.True((Int32)function1.RunFunction(new object[] { 6, 2 }).Value == 8);
        }

        [Fact]
        public void FunctionFromReflection()
        {
            //create a custom function
            Function function1 = new Function(this, this.GetType().GetMethod("TestMethod"), null, "", null);
            Assert.True((Int32)function1.RunFunction(new object[] { 6, 2 }).Value == 8);
        }

        public int TestMethod(int a, int b) => a + b;
    }
}
