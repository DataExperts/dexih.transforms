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
            TransformFunction function1 = new TransformFunction(new Func<int, int, int>((i, j) => i + j), new TableColumn[] { new TableColumn("value1"), new TableColumn("value2") }, new TableColumn("Add"), null);
            Assert.True((Int32)function1.RunFunction(new object[] { 6, 2 }) == 8);
        }

        [Fact]
        public void FunctionFromMethod()
        {
            //create a custom function
            TransformFunction function1 = new TransformFunction(this, "TestMethod", "", "", null, new TableColumn("test"), null);
            Assert.True((Int32)function1.RunFunction(new object[] { 6, 2 }) == 8);
        }

        [Fact]
        public void FunctionFromReflection()
        {
            //create a custom function
            TransformFunction function1 = new TransformFunction(this, this.GetType().GetMethod("TestMethod"), null, new TableColumn("test"), null);
            Assert.True((Int32)function1.RunFunction(new object[] { 6, 2 }) == 8);
        }

        public int TestMethod(int a, int b) => a + b;
    }
}
