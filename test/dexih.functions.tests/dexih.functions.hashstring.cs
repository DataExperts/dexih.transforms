using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace dexih.functions.tests
{
    public class FunctionHashString
    {
        [Theory]
        [InlineData("a")]
        [InlineData("1a")]
        [InlineData("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
        [InlineData("123123123123123")]
        [InlineData("123!@#$%^&*()_+-=`~\\|[]{};':\",./><?")]
        [InlineData("   ")]
        public void HashFunctions(string TestValue)
        {
            //Use a for loop to similate gen sequence.
            string HashString1 = HashString.CreateHash(TestValue);
            string HashString2 = HashString.CreateHash(TestValue);
            Assert.NotEqual(HashString1, HashString2); //two hashes in a row should not be equal as they are salted;

            string HashString3 = HashString.CreateHash(TestValue + " ");

            Assert.True(HashString.ValidateHash(TestValue, HashString1));
            Assert.True(HashString.ValidateHash (TestValue, HashString2));

            Assert.False(HashString.ValidateHash (TestValue, HashString3));
            Assert.False(HashString.ValidateHash(TestValue + "1", HashString1 ));
        }
    }
}
