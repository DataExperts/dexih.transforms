using MathNet.Numerics;

namespace dexih.functions.maths
{
    public class EuclidFunctions
    {

        /// <summary>
        /// Returns the greatest common divisor (<c>gcd</c>) of a set of integers using Euclid's
        /// algorithm.
        /// </summary>
        /// <param name="integers">List of Integers.</param>
        /// <returns>Greatest common divisor <c>gcd</c>(list of integers)</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths - Euclid", Name = "Greatest Common Divisor (GCD)", Description = "The greatest common divisor of a set of integers.")]
        public long GreatestCommonDivisor(long[] integers) => Euclid.GreatestCommonDivisor(integers);


        /// <summary>
        /// Computes the extended greatest common divisor, such that a*x + b*y = <c>gcd</c>(a,b).
        /// </summary>
        /// <param name="a">First Integer: a.</param>
        /// <param name="b">Second Integer: b.</param>
        /// <param name="x">Resulting x, such that a*x + b*y = <c>gcd</c>(a,b).</param>
        /// <param name="y">Resulting y, such that a*x + b*y = <c>gcd</c>(a,b)</param>
        /// <returns>Greatest common divisor <c>gcd</c>(a,b)</returns>
        /// <example>
        /// <code>
        /// long x,y,d;
        /// d = Fn.GreatestCommonDivisor(45,18,out x, out y);
        /// -&gt; d == 9 &amp;&amp; x == 1 &amp;&amp; y == -2
        /// </code>
        /// The <c>gcd</c> of 45 and 18 is 9: 18 = 2*9, 45 = 5*9. 9 = 1*45 -2*18, therefore x=1 and y=-2.
        /// </example>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths - Euclid", Name = "Extended Greatest CommonDivisor", Description = "The extended greatest common divisor, such that a*x + b*y = gcd(a,b)..")]
        public long ExtendedGreatestCommonDivisor(long a, long b, out long x, out long y) =>
            Euclid.ExtendedGreatestCommonDivisor(a, b, out x, out y);

        /// <summary>
        /// Returns the least common multiple (<c>lcm</c>) of a set of integers using Euclid's algorithm.
        /// </summary>
        /// <param name="integers">List of Integers.</param>
        /// <returns>Least common multiple <c>lcm</c>(list of integers)</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths - Euclid", Name = "Least Common Multiple", Description = "The least common multiple (LCM) of a set of integers")]
        public long LeastCommonMultiple(long[] integers) => Euclid.LeastCommonMultiple(integers);

    }
}