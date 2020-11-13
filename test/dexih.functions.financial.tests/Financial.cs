using System;
using Xunit;

namespace dexih.functions.financial.tests
{
    /// <summary>
    /// Note, only testing the financial functions which are hand coded, not ones from the ExcelFinancialFunctions library.
    /// </summary>
    public class FinancialTests
    {

        [Fact]
        public void IIR_Test()
        {
            var function = new FinancialFunctions();
            
            Assert.Null(function.RRI(5, 0, 0));
            Assert.Equal(0, function.RRI(5, 10, 10));
            Assert.Equal(1, function.RRI(2, 2, 8));
            Assert.Equal(-0.5, function.RRI(2, 8, 2));
            Assert.Equal(-1, function.RRI(2, 8, 0));
            Assert.Null(function.RRI(2, 0, 10));
        }
    }
}