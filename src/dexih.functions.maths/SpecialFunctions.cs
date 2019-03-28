using System;
using MathNet.Numerics;

namespace dexih.functions.maths
{
    public class SpecialMathFunctions
    {
        public enum EMathConstant
        {
            E, 
            Atto, 
            Avogadro, 
            BohrMagneton, 
            BohrRadius, 
            Catalan, 
            Centi, 
            CharacteristicImpedanceVacuum, 
            ClassicalElectronRadius, 
            ComptonWavelength, 
            ConductanceQuantum, 
            Deca, 
            Deci, 
            Degree, 
            DeuteronMagneticMoment, 
            DeuteronMass, 
            DeuteronMassEnegryEquivalent, 
            DeuteronMolarMass, 
            DiracsConstant, 
            ElectricPermittivity, 
            ElectronGFactor, 
            ElectronMagneticMoment, 
            ElectronMass, 
            ElectronMassEnergyEquivalent, 
            ElectronMolarMass, 
            ElementaryCharge, 
            EulerMascheroni, 
            Exa, 
            Femto, 
            FermiCouplingConstant, 
            FineStructureConstant, 
            Giga, 
            Glaisher, 
            GoldenRatio, 
            Grad, 
            GravitationalConstant, 
            HalfSqrt3, 
            HartreeEnergy, 
            Hecto, 
            HelionMass, 
            HelionMassEnegryEquivalent, 
            HelionMolarMass, 
            InvE, 
            InvPi, 
            InvSqrt2Pi, 
            InvSqrtPi, 
            JosephsonConstant, 
            Khinchin, 
            Kilo, 
            Ln10, 
            Ln2, 
            Ln2PiOver2, 
            LnPi, 
            Log10E, 
            Log2E, 
            LogSqrt2Pi, 
            LogSqrt2PiE, 
            LogTwoSqrtEOverPi, 
            MagneticFluxQuantum, 
            MagneticPermeability, 
            Mega, 
            Micro, 
            Milli, 
            MuonComptonWavelength, 
            MuonGFactor, 
            MuonMagneticMoment, 
            MuonMass, 
            MuonMassEnegryEquivalent, 
            MuonMolarMass, 
            Nano, 
            NeutralDecibel, 
            NeutronComptonWavelength, 
            NeutronGFactor, 
            NeutronGyromagneticRatio, 
            NeutronMagneticMoment, 
            NeutronMass, 
            NeutronMassEnegryEquivalent, 
            NeutronMolarMass, 
            NuclearMagneton, 
            Peta, 
            Pi, 
            Pi2, 
            Pi3Over2, 
            Pico, 
            PiOver2, 
            PiOver4, 
            PlancksConstant, 
            PlancksLength, 
            PlancksMass, 
            PlancksTemperature, 
            PlancksTime, 
            PowerDecibel, 
            ProtonComptonWavelength, 
            ProtonGFactor, 
            ProtonGyromagneticRatio, 
            ProtonMagneticMoment, 
            ProtonMass, 
            ProtonMassEnergyEquivalent, 
            ProtonMolarMass, 
            QuantumOfCirculation, 
            RydbergConstant, 
            ShieldedProtonGyromagneticRatio, 
            ShieldedProtonMagneticMoment, 
            SpeedOfLight, 
            Sqrt1Over2, 
            Sqrt2, 
            Sqrt2Pi, 
            Sqrt2PiE, 
            Sqrt3, 
            SqrtE, 
            SqrtPi, 
            TauComptonWavelength, 
            TauMass, 
            TauMassEnergyEquivalent, 
            TauMolarMass, 
            Tera, 
            ThomsonCrossSection, 
            TwoInvPi, 
            TwoInvSqrtPi, 
            TwoSqrtEOverPi, 
            VonKlitzingConstant, 
            WeakMixingAngle, 
            Yocto, 
            Yotta, 
            Zepto, 
            Zetta, 
            SizeOfComplex, 
            SizeOfComplex32, 
            SizeOfDouble, 
            SizeOfFloat, 
            SizeOfInt, 
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Math Constant", Description = "Returns the specified maths constant.")]
        public double MathConstant(EMathConstant mathConstant)
        {
            switch (mathConstant)
            {
                case EMathConstant.E: return Constants.E;
                case EMathConstant.Atto: return Constants.Atto;
                case EMathConstant.Avogadro: return Constants.Avogadro;
                case EMathConstant.BohrMagneton: return Constants.BohrMagneton;
                case EMathConstant.BohrRadius: return Constants.BohrRadius;
                case EMathConstant.Catalan: return Constants.Catalan;
                case EMathConstant.Centi: return Constants.Centi;
                case EMathConstant.CharacteristicImpedanceVacuum: return Constants.CharacteristicImpedanceVacuum;
                case EMathConstant.ClassicalElectronRadius: return Constants.ClassicalElectronRadius;
                case EMathConstant.ComptonWavelength: return Constants.ComptonWavelength;
                case EMathConstant.ConductanceQuantum: return Constants.ConductanceQuantum;
                case EMathConstant.Deca: return Constants.Deca;
                case EMathConstant.Deci: return Constants.Deci;
                case EMathConstant.Degree: return Constants.Degree;
                case EMathConstant.DeuteronMagneticMoment: return Constants.DeuteronMagneticMoment;
                case EMathConstant.DeuteronMass: return Constants.DeuteronMass;
                case EMathConstant.DeuteronMassEnegryEquivalent: return Constants.DeuteronMassEnegryEquivalent;
                case EMathConstant.DeuteronMolarMass: return Constants.DeuteronMolarMass;
                case EMathConstant.DiracsConstant: return Constants.DiracsConstant;
                case EMathConstant.ElectricPermittivity: return Constants.ElectricPermittivity;
                case EMathConstant.ElectronGFactor: return Constants.ElectronGFactor;
                case EMathConstant.ElectronMagneticMoment: return Constants.ElectronMagneticMoment;
                case EMathConstant.ElectronMass: return Constants.ElectronMass;
                case EMathConstant.ElectronMassEnergyEquivalent: return Constants.ElectronMassEnergyEquivalent;
                case EMathConstant.ElectronMolarMass: return Constants.ElectronMolarMass;
                case EMathConstant.ElementaryCharge: return Constants.ElementaryCharge;
                case EMathConstant.EulerMascheroni: return Constants.EulerMascheroni;
                case EMathConstant.Exa: return Constants.Exa;
                case EMathConstant.Femto: return Constants.Femto;
                case EMathConstant.FermiCouplingConstant: return Constants.FermiCouplingConstant;
                case EMathConstant.FineStructureConstant: return Constants.FineStructureConstant;
                case EMathConstant.Giga: return Constants.Giga;
                case EMathConstant.Glaisher: return Constants.Glaisher;
                case EMathConstant.GoldenRatio: return Constants.GoldenRatio;
                case EMathConstant.Grad: return Constants.Grad;
                case EMathConstant.GravitationalConstant: return Constants.GravitationalConstant;
                case EMathConstant.HalfSqrt3: return Constants.HalfSqrt3;
                case EMathConstant.HartreeEnergy: return Constants.HartreeEnergy;
                case EMathConstant.Hecto: return Constants.Hecto;
                case EMathConstant.HelionMass: return Constants.HelionMass;
                case EMathConstant.HelionMassEnegryEquivalent: return Constants.HelionMassEnegryEquivalent;
                case EMathConstant.HelionMolarMass: return Constants.HelionMolarMass;
                case EMathConstant.InvE: return Constants.InvE;
                case EMathConstant.InvPi: return Constants.InvPi;
                case EMathConstant.InvSqrt2Pi: return Constants.InvSqrt2Pi;
                case EMathConstant.InvSqrtPi: return Constants.InvSqrtPi;
                case EMathConstant.JosephsonConstant: return Constants.JosephsonConstant;
                case EMathConstant.Khinchin: return Constants.Khinchin;
                case EMathConstant.Kilo: return Constants.Kilo;
                case EMathConstant.Ln10: return Constants.Ln10;
                case EMathConstant.Ln2: return Constants.Ln2;
                case EMathConstant.Ln2PiOver2: return Constants.Ln2PiOver2;
                case EMathConstant.LnPi: return Constants.LnPi;
                case EMathConstant.Log10E: return Constants.Log10E;
                case EMathConstant.Log2E: return Constants.Log2E;
                case EMathConstant.LogSqrt2Pi: return Constants.LogSqrt2Pi;
                case EMathConstant.LogSqrt2PiE: return Constants.LogSqrt2PiE;
                case EMathConstant.LogTwoSqrtEOverPi: return Constants.LogTwoSqrtEOverPi;
                case EMathConstant.MagneticFluxQuantum: return Constants.MagneticFluxQuantum;
                case EMathConstant.MagneticPermeability: return Constants.MagneticPermeability;
                case EMathConstant.Mega: return Constants.Mega;
                case EMathConstant.Micro: return Constants.Micro;
                case EMathConstant.Milli: return Constants.Milli;
                case EMathConstant.MuonComptonWavelength: return Constants.MuonComptonWavelength;
                case EMathConstant.MuonGFactor: return Constants.MuonGFactor;
                case EMathConstant.MuonMagneticMoment: return Constants.MuonMagneticMoment;
                case EMathConstant.MuonMass: return Constants.MuonMass;
                case EMathConstant.MuonMassEnegryEquivalent: return Constants.MuonMassEnegryEquivalent;
                case EMathConstant.MuonMolarMass: return Constants.MuonMolarMass;
                case EMathConstant.Nano: return Constants.Nano;
                case EMathConstant.NeutralDecibel: return Constants.NeutralDecibel;
                case EMathConstant.NeutronComptonWavelength: return Constants.NeutronComptonWavelength;
                case EMathConstant.NeutronGFactor: return Constants.NeutronGFactor;
                case EMathConstant.NeutronGyromagneticRatio: return Constants.NeutronGyromagneticRatio;
                case EMathConstant.NeutronMagneticMoment: return Constants.NeutronMagneticMoment;
                case EMathConstant.NeutronMass: return Constants.NeutronMass;
                case EMathConstant.NeutronMassEnegryEquivalent: return Constants.NeutronMassEnegryEquivalent;
                case EMathConstant.NeutronMolarMass: return Constants.NeutronMolarMass;
                case EMathConstant.NuclearMagneton: return Constants.NuclearMagneton;
                case EMathConstant.Peta: return Constants.Peta;
                case EMathConstant.Pi: return Constants.Pi;
                case EMathConstant.Pi2: return Constants.Pi2;
                case EMathConstant.Pi3Over2: return Constants.Pi3Over2;
                case EMathConstant.Pico: return Constants.Pico;
                case EMathConstant.PiOver2: return Constants.PiOver2;
                case EMathConstant.PiOver4: return Constants.PiOver4;
                case EMathConstant.PlancksConstant: return Constants.PlancksConstant;
                case EMathConstant.PlancksLength: return Constants.PlancksLength;
                case EMathConstant.PlancksMass: return Constants.PlancksMass;
                case EMathConstant.PlancksTemperature: return Constants.PlancksTemperature;
                case EMathConstant.PlancksTime: return Constants.PlancksTime;
                case EMathConstant.PowerDecibel: return Constants.PowerDecibel;
                case EMathConstant.ProtonComptonWavelength: return Constants.ProtonComptonWavelength;
                case EMathConstant.ProtonGFactor: return Constants.ProtonGFactor;
                case EMathConstant.ProtonGyromagneticRatio: return Constants.ProtonGyromagneticRatio;
                case EMathConstant.ProtonMagneticMoment: return Constants.ProtonMagneticMoment;
                case EMathConstant.ProtonMass: return Constants.ProtonMass;
                case EMathConstant.ProtonMassEnergyEquivalent: return Constants.ProtonMassEnergyEquivalent;
                case EMathConstant.ProtonMolarMass: return Constants.ProtonMolarMass;
                case EMathConstant.QuantumOfCirculation: return Constants.QuantumOfCirculation;
                case EMathConstant.RydbergConstant: return Constants.RydbergConstant;
                case EMathConstant.ShieldedProtonGyromagneticRatio: return Constants.ShieldedProtonGyromagneticRatio;
                case EMathConstant.ShieldedProtonMagneticMoment: return Constants.ShieldedProtonMagneticMoment;
                case EMathConstant.SpeedOfLight: return Constants.SpeedOfLight;
                case EMathConstant.Sqrt1Over2: return Constants.Sqrt1Over2;
                case EMathConstant.Sqrt2: return Constants.Sqrt2;
                case EMathConstant.Sqrt2Pi: return Constants.Sqrt2Pi;
                case EMathConstant.Sqrt2PiE: return Constants.Sqrt2PiE;
                case EMathConstant.Sqrt3: return Constants.Sqrt3;
                case EMathConstant.SqrtE: return Constants.SqrtE;
                case EMathConstant.SqrtPi: return Constants.SqrtPi;
                case EMathConstant.TauComptonWavelength: return Constants.TauComptonWavelength;
                case EMathConstant.TauMass: return Constants.TauMass;
                case EMathConstant.TauMassEnergyEquivalent: return Constants.TauMassEnergyEquivalent;
                case EMathConstant.TauMolarMass: return Constants.TauMolarMass;
                case EMathConstant.Tera: return Constants.Tera;
                case EMathConstant.ThomsonCrossSection: return Constants.ThomsonCrossSection;
                case EMathConstant.TwoInvPi: return Constants.TwoInvPi;
                case EMathConstant.TwoInvSqrtPi: return Constants.TwoInvSqrtPi;
                case EMathConstant.TwoSqrtEOverPi: return Constants.TwoSqrtEOverPi;
                case EMathConstant.VonKlitzingConstant: return Constants.VonKlitzingConstant;
                case EMathConstant.WeakMixingAngle: return Constants.WeakMixingAngle;
                case EMathConstant.Yocto: return Constants.Yocto;
                case EMathConstant.Yotta: return Constants.Yotta;
                case EMathConstant.Zepto: return Constants.Zepto;
                case EMathConstant.Zetta: return Constants.Zetta;
                case EMathConstant.SizeOfComplex: return Constants.SizeOfComplex;
                case EMathConstant.SizeOfComplex32: return Constants.SizeOfComplex32;
                case EMathConstant.SizeOfDouble: return Constants.SizeOfDouble;
                case EMathConstant.SizeOfFloat: return Constants.SizeOfFloat;
                case EMathConstant.SizeOfInt: return Constants.SizeOfInt;
            }
            
            throw new ArgumentOutOfRangeException();
        }
        
        
      /// <summary>Computes the logarithm of the Euler Beta function.</summary>
      /// <param name="z">The first Beta parameter, a positive real number.</param>
      /// <param name="w">The second Beta parameter, a positive real number.</param>
      /// <returns>The logarithm of the Euler Beta function evaluated at z,w.</returns>
      /// <exception cref="T:System.ArgumentException">If <paramref name="z" /> or <paramref name="w" /> are not positive.</exception>
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Beat Logarithm", Description = "The logarithm of the Euler Beta function evaluated at z,w.")]
      public double BetaLn(double z, double w) => SpecialFunctions.BetaLn(z, w);

      /// <summary>Computes the Euler Beta function.</summary>
      /// <param name="z">The first Beta parameter, a positive real number.</param>
      /// <param name="w">The second Beta parameter, a positive real number.</param>
      /// <returns>The Euler Beta function evaluated at z,w.</returns>
      /// <exception cref="T:System.ArgumentException">If <paramref name="z" /> or <paramref name="w" /> are not positive.</exception>
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Beta", Description = "The Euler Beta function evaluated at z,w.")]
      public double Beta(double z, double w) => SpecialFunctions.Beta(z, w);

      /// <summary>
      /// Returns the lower incomplete (unregularized) beta function
      /// B(a,b,x) = int(t^(a-1)*(1-t)^(b-1),t=0..x) for real a &gt; 0, b &gt; 0, 1 &gt;= x &gt;= 0.
      /// </summary>
      /// <param name="a">The first Beta parameter, a positive real number.</param>
      /// <param name="b">The second Beta parameter, a positive real number.</param>
      /// <param name="x">The upper limit of the integral.</param>
      /// <returns>The lower incomplete (unregularized) beta function.</returns>
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Beta Incomplete", Description = "Returns the lower incomplete (unregularized) beta function")]
      public double BetaIncomplete(double a, double b, double x) => SpecialFunctions.BetaIncomplete(a, b, x);

      /// <summary>
      /// Returns the regularized lower incomplete beta function
      /// I_x(a,b) = 1/Beta(a,b) * int(t^(a-1)*(1-t)^(b-1),t=0..x) for real a &gt; 0, b &gt; 0, 1 &gt;= x &gt;= 0.
      /// </summary>
      /// <param name="a">The first Beta parameter, a positive real number.</param>
      /// <param name="b">The second Beta parameter, a positive real number.</param>
      /// <param name="x">The upper limit of the integral.</param>
      /// <returns>The regularized lower incomplete beta function.</returns>
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Beta Regularized", Description = "Returns the regularized lower incomplete beta function")]
      public double BetaRegularized(double a, double b, double x) => SpecialFunctions.BetaRegularized(a, b, x);


      /// <summary>Calculates the error function.</summary>
      /// <param name="x">The value to evaluate.</param>
      /// <returns>the error function evaluated at given value.</returns>
      /// <remarks>
      ///     <list type="bullet">
      ///         <item>returns 1 if <c>x == double.PositiveInfinity</c>.</item>
      ///         <item>returns -1 if <c>x == double.NegativeInfinity</c>.</item>
      ///     </list>
      /// </remarks>
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Error", Description = "Calculates the error function")]
      public double Erf(double x) => SpecialFunctions.Erf(x);

      /// <summary>Calculates the complementary error function.</summary>
      /// <param name="x">The value to evaluate.</param>
      /// <returns>the complementary error function evaluated at given value.</returns>
      /// <remarks>
      ///     <list type="bullet">
      ///         <item>returns 0 if <c>x == double.PositiveInfinity</c>.</item>
      ///         <item>returns 2 if <c>x == double.NegativeInfinity</c>.</item>
      ///     </list>
      /// </remarks>
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Error Complimentary", Description = "Calculates the complementary error function.")]
      public double Erfc(double x) => SpecialFunctions.Erfc(x);


      /// <summary>Calculates the inverse error function evaluated at z.</summary>
      /// <returns>The inverse error function evaluated at given value.</returns>
      /// <remarks>
      ///   <list type="bullet">
      ///     <item>returns double.PositiveInfinity if <c>z &gt;= 1.0</c>.</item>
      ///     <item>returns double.NegativeInfinity if <c>z &lt;= -1.0</c>.</item>
      ///   </list>
      /// </remarks>
      /// <summary>Calculates the inverse error function evaluated at z.</summary>
      /// <param name="z">value to evaluate.</param>
      /// <returns>the inverse error function evaluated at Z.</returns>
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Error Inverse", Description = "Calculates the inverse error function evaluated at z.")]
      public double ErfInv(double z) => SpecialFunctions.ErfInv(z);

      /// <summary>Calculates the complementary inverse error function evaluated at z.</summary>
      /// <returns>The complementary inverse error function evaluated at given value.</returns>
      /// <remarks> We have tested this implementation against the arbitrary precision mpmath library
      /// and found cases where we can only guarantee 9 significant figures correct.
      ///     <list type="bullet">
      ///         <item>returns double.PositiveInfinity if <c>z &lt;= 0.0</c>.</item>
      ///         <item>returns double.NegativeInfinity if <c>z &gt;= 2.0</c>.</item>
      ///     </list>
      /// </remarks>
      /// <summary>calculates the complementary inverse error function evaluated at z.</summary>
      /// <param name="z">value to evaluate.</param>
      /// <returns>the complementary inverse error function evaluated at Z.</returns>
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Error Complementary Inverse", Description = "Calculates the complementary inverse error function evaluated at z.")]
      public double ErfcInv(double z) => SpecialFunctions.ErfcInv(z);




      /// <summary>
      /// Computes the generalized Exponential Integral function (En).
      /// </summary>
      /// <param name="x">The argument of the Exponential Integral function.</param>
      /// <param name="n">Integer power of the denominator term. Generalization index.</param>
      /// <returns>The value of the Exponential Integral function.</returns>
      /// <remarks>
      /// <para>This implementation of the computation of the Exponential Integral function follows the derivation in
      ///     "Handbook of Mathematical Functions, Applied Mathematics Series, Volume 55", Abramowitz, M., and Stegun, I.A. 1964,  reprinted 1968 by
      ///     Dover Publications, New York), Chapters 6, 7, and 26.
      ///     AND
      ///     "Advanced mathematical methods for scientists and engineers", Bender, Carl M.; Steven A. Orszag (1978). page 253
      /// </para>
      /// <para>
      ///     for x &gt; 1  uses continued fraction approach that is often used to compute incomplete gamma.
      ///     for 0 &lt; x &lt;= 1 uses Taylor series expansion
      /// </para>
      /// <para>Our unit tests suggest that the accuracy of the Exponential Integral function is correct up to 13 floating point digits.</para>
      /// </remarks>
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Exponential Integral", Description = "Computes the generalized Exponential Integral function (En).")]
      public double ExponentialIntegral(double x, int n) => SpecialFunctions.ExponentialIntegral(x, n);


        /// <summary>
        /// Computes the factorial function x -&gt; x! of an integer number &gt; 0. The function can represent all number up
        /// to 22! exactly, all numbers up to 170! using a double representation. All larger values will overflow.
        /// </summary>
        /// <returns>A value value! for value &gt; 0</returns>
        /// <remarks>
        /// If you need to multiply or divide various such factorials, consider using the logarithmic version
        /// <see cref="M:MathNet.Numerics.SpecialFunctions.FactorialLn(System.Int32)" /> instead so you can add instead of multiply and subtract instead of divide, and
        /// then exponentiate the result using <see cref="M:System.Math.Exp(System.Double)" />. This will also circumvent the problem that
        /// factorials become very large even for small parameters.
        /// </remarks>
        /// <exception cref="T:System.ArgumentOutOfRangeException" />
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Factorial", Description = "Computes the factorial function x.")]
        public double Factorial(int x) => SpecialFunctions.Factorial(x);

        /// <summary>
        /// Computes the logarithmic factorial function x -&gt; ln(x!) of an integer number &gt; 0.
        /// </summary>
        /// <returns>A value value! for value &gt; 0</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Factorial Logarithm", Description = "Computes the logarithmic factorial function x")]
        public double FactorialLn(int x) => SpecialFunctions.FactorialLn(x);


        /// <summary>Computes the binomial coefficient: n choose k.</summary>
        /// <param name="n">A nonnegative value n.</param>
        /// <param name="k">A nonnegative value h.</param>
        /// <returns>The binomial coefficient: n choose k.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Binomial Coefficient", Description = "Computes the binomial coefficient: n choose k.")]
        public double Binomial(int n, int k) => SpecialFunctions.Binomial(n, k);


        /// <summary>
        /// Computes the natural logarithm of the binomial coefficient: ln(n choose k).
        /// </summary>
        /// <param name="n">A nonnegative value n.</param>
        /// <param name="k">A nonnegative value h.</param>
        /// <returns>The logarithmic binomial coefficient: ln(n choose k).</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Binomial Logarithm Coefficient", Description = "Computes the natural logarithm of the binomial coefficient: ln(n choose k)")]
        public double BinomialLn(int n, int k) => SpecialFunctions.BinomialLn(n, k);


        /// <summary>
        /// Computes the multinomial coefficient: n choose n1, n2, n3, ...
        /// </summary>
        /// <param name="n">A nonnegative value n.</param>
        /// <param name="ni">An array of nonnegative values that sum to <paramref name="n" />.</param>
        /// <returns>The multinomial coefficient.</returns>
        /// <exception cref="T:System.ArgumentNullException">if <paramref name="ni" /> is <see langword="null" />.</exception>
        /// <exception cref="T:System.ArgumentException">If <paramref name="n" /> or any of the <paramref name="ni" /> are negative.</exception>
        /// <exception cref="T:System.ArgumentException">If the sum of all <paramref name="ni" /> is not equal to <paramref name="n" />.</exception>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Multinomial Coefficient", Description = "Computes the multinomial coefficient: n choose n1, n2, n3, ...")]
        public double Multinomial(int n, int[] ni) => SpecialFunctions.Multinomial(n, ni);


        /// <summary>Computes the logarithm of the Gamma function.</summary>
        /// <param name="z">The argument of the gamma function.</param>
        /// <returns>The logarithm of the gamma function.</returns>
        /// <remarks>
        /// <para>This implementation of the computation of the gamma and logarithm of the gamma function follows the derivation in
        ///     "An Analysis Of The Lanczos Gamma Approximation", Glendon Ralph Pugh, 2004.
        /// We use the implementation listed on p. 116 which achieves an accuracy of 16 floating point digits. Although 16 digit accuracy
        /// should be sufficient for double values, improving accuracy is possible (see p. 126 in Pugh).</para>
        /// <para>Our unit tests suggest that the accuracy of the Gamma function is correct up to 14 floating point digits.</para>
        /// </remarks>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Gamma Logarithm", Description = "Computes the logarithm of the Gamma function.")]
        public double GammaLn(double z) => SpecialFunctions.GammaLn(z);


        /// <summary>Computes the Gamma function.</summary>
        /// <param name="z">The argument of the gamma function.</param>
        /// <returns>The logarithm of the gamma function.</returns>
        /// <remarks>
        /// <para>
        /// This implementation of the computation of the gamma and logarithm of the gamma function follows the derivation in
        ///     "An Analysis Of The Lanczos Gamma Approximation", Glendon Ralph Pugh, 2004.
        /// We use the implementation listed on p. 116 which should achieve an accuracy of 16 floating point digits. Although 16 digit accuracy
        /// should be sufficient for double values, improving accuracy is possible (see p. 126 in Pugh).
        /// </para>
        /// <para>Our unit tests suggest that the accuracy of the Gamma function is correct up to 13 floating point digits.</para>
        /// </remarks>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Gamma", Description = "Computes the Gamma function.")]
        public double Gamma(double z) => SpecialFunctions.Gamma(z);


        /// <summary>
        /// Returns the upper incomplete regularized gamma function
        /// Q(a,x) = 1/Gamma(a) * int(exp(-t)t^(a-1),t=0..x) for real a &gt; 0, x &gt; 0.
        /// </summary>
        /// <param name="a">The argument for the gamma function.</param>
        /// <param name="x">The lower integral limit.</param>
        /// <returns>The upper incomplete regularized gamma function.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Gamma Upper Regularized", Description = "Returns the upper incomplete regularized gamma function")]
        public double GammaUpperRegularized(double a, double x) => SpecialFunctions.GammaUpperRegularized(a, x);


        /// <summary>
        /// Returns the upper incomplete gamma function
        /// Gamma(a,x) = int(exp(-t)t^(a-1),t=0..x) for real a &gt; 0, x &gt; 0.
        /// </summary>
        /// <param name="a">The argument for the gamma function.</param>
        /// <param name="x">The lower integral limit.</param>
        /// <returns>The upper incomplete gamma function.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Gamma Upper Incomplete", Description = "Returns the upper incomplete gamma function")]
        public double GammaUpperIncomplete(double a, double x) => SpecialFunctions.GammaUpperIncomplete(a, x);


        /// <summary>
        /// Returns the lower incomplete gamma function
        /// gamma(a,x) = int(exp(-t)t^(a-1),t=0..x) for real a &gt; 0, x &gt; 0.
        /// </summary>
        /// <param name="a">The argument for the gamma function.</param>
        /// <param name="x">The upper integral limit.</param>
        /// <returns>The lower incomplete gamma function.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Gamma Lower Incomplete", Description = "Returns the lower incomplete gamma function")]
        public double GammaLowerIncomplete(double a, double x) => SpecialFunctions.GammaLowerIncomplete(a, x);


        /// <summary>
        /// Returns the lower incomplete regularized gamma function
        /// P(a,x) = 1/Gamma(a) * int(exp(-t)t^(a-1),t=0..x) for real a &gt; 0, x &gt; 0.
        /// </summary>
        /// <param name="a">The argument for the gamma function.</param>
        /// <param name="x">The upper integral limit.</param>
        /// <returns>The lower incomplete gamma function.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Gamma Lower Regularized", Description = "Returns the lower incomplete regularized gamma function")]
        public double GammaLowerRegularized(double a, double x) => SpecialFunctions.GammaLowerRegularized(a, x);


        /// <summary>
        /// Returns the inverse P^(-1) of the regularized lower incomplete gamma function
        /// P(a,x) = 1/Gamma(a) * int(exp(-t)t^(a-1),t=0..x) for real a &gt; 0, x &gt; 0,
        /// such that P^(-1)(a,P(a,x)) == x.
        /// </summary>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Gamma Lower Regularize Inverse", Description = "Returns the inverse P^(-1) of the regularized lower incomplete gamma function")]
        public double GammaLowerRegularizedInv(double a, double y0) => SpecialFunctions.GammaLowerRegularizedInv(a, y0);


        /// <summary>
        /// Computes the Digamma function which is mathematically defined as the derivative of the logarithm of the gamma function.
        /// This implementation is based on
        ///     Jose Bernardo
        ///     Algorithm AS 103:
        ///     Psi ( Digamma ) Function,
        ///     Applied Statistics,
        ///     Volume 25, Number 3, 1976, pages 315-317.
        /// Using the modifications as in Tom Minka's lightspeed toolbox.
        /// </summary>
        /// <param name="x">The argument of the digamma function.</param>
        /// <returns>The value of the DiGamma function at <paramref name="x" />.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "DiGamma", Description = "Computes the Digamma function which is mathematically defined as the derivative of the logarithm of the gamma function.")]
        public double DiGamma(double x) => SpecialFunctions.DiGamma(x);


        /// <summary>
        /// <para>Computes the inverse Digamma function: this is the inverse of the logarithm of the gamma function. This function will
        /// only return solutions that are positive.</para>
        /// <para>This implementation is based on the bisection method.</para>
        /// </summary>
        /// <param name="p">The argument of the inverse digamma function.</param>
        /// <returns>The positive solution to the inverse DiGamma function at <paramref name="p" />.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Digamma Inverse", Description = "Computes the inverse Digamma function: this is the inverse of the logarithm of the gamma function.")]
        public double DiGammaInv(double p) => SpecialFunctions.DiGammaInv(p);


        /// <summary>
        /// Computes the <paramref name="t" />'th Harmonic number.
        /// </summary>
        /// <param name="t">The Harmonic number which needs to be computed.</param>
        /// <returns>The t'th Harmonic number.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Harmonic Number", Description = "Computes the t'th Harmonic number.")]
        public double Harmonic(int t) => SpecialFunctions.Harmonic(t);


        /// <summary>
        /// Compute the generalized harmonic number of order n of m. (1 + 1/2^m + 1/3^m + ... + 1/n^m)
        /// </summary>
        /// <param name="n">The order parameter.</param>
        /// <param name="m">The power parameter.</param>
        /// <returns>General Harmonic number.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "General Harmonic Number", Description = "Compute the generalized harmonic number of order n of m. (1 + 1/2^m + 1/3^m + ... + 1/n^m)")]
        public double GeneralHarmonic(int n, double m) => SpecialFunctions.GeneralHarmonic(n, m);


        /// <summary>
        /// Computes the logistic function. see: http://en.wikipedia.org/wiki/Logistic
        /// </summary>
        /// <param name="p">The parameter for which to compute the logistic function.</param>
        /// <returns>The logistic function of <paramref name="p" />.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Logistic", Description = "Computes the logistic function. see: http://en.wikipedia.org/wiki/Logistic")]
        public double Logistic(double p) => SpecialFunctions.Logistic(p);


        /// <summary>
        /// Computes the logit function, the inverse of the sigmoid logistic function. see: http://en.wikipedia.org/wiki/Logit
        /// </summary>
        /// <param name="p">The parameter for which to compute the logit function. This number should be
        /// between 0 and 1.</param>
        /// <returns>The logarithm of <paramref name="p" /> divided by 1.0 - <paramref name="p" />.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Logit", Description = "Computes the logit function, the inverse of the sigmoid logistic function. see: http://en.wikipedia.org/wiki/Logit")]
        public double Logit(double p) => SpecialFunctions.Logit(p);


        /// <summary>Returns the modified Bessel function of first kind, order 0 of the argument.
        /// <p />
        /// The function is defined as <tt>i0(x) = j0( ix )</tt>.
        /// <p />
        /// The range is partitioned into the two intervals [0, 8] and
        /// (8, infinity). Chebyshev polynomial expansions are employed
        /// in each interval.
        /// </summary>
        /// <param name="x">The value to compute the Bessel function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Bessel I0", Description = "Returns the modified Bessel function of first kind, order 0 of the argument.")]
        public double BesselI0(double x) => SpecialFunctions.BesselI0(x);


        /// <summary>Returns the modified Bessel function of first kind,
        /// order 1 of the argument.
        /// <p />
        /// The function is defined as <tt>i1(x) = -i j1( ix )</tt>.
        /// <p />
        /// The range is partitioned into the two intervals [0, 8] and
        /// (8, infinity). Chebyshev polynomial expansions are employed
        /// in each interval.
        /// </summary>
        /// <param name="x">The value to compute the Bessel function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Bessel I1", Description = "Returns the modified Bessel function of first kind, order 1 of the argument.")]
        public double BesselI1(double x) => SpecialFunctions.BesselI1(x);


        /// <summary> Returns the modified Bessel function of the second kind
        /// of order 0 of the argument.
        /// <p />
        /// The range is partitioned into the two intervals [0, 8] and
        /// (8, infinity). Chebyshev polynomial expansions are employed
        /// in each interval.
        /// </summary>
        /// <param name="x">The value to compute the Bessel function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Bessel K0", Description = "Returns the modified Bessel function of the second kind of order 0 of the argument.")]
        public double BesselK0(double x) => SpecialFunctions.BesselK0(x);


        /// <summary>Returns the exponentially scaled modified Bessel function
        /// of the second kind of order 0 of the argument.
        /// </summary>
        /// <param name="x">The value to compute the Bessel function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Bessel K0e", Description = "Returns the exponentially scaled modified Bessel function of the second kind of order 0 of the argument.")]
        public double BesselK0e(double x) => SpecialFunctions.BesselK0e(x);


        /// <summary> Returns the modified Bessel function of the second kind
        /// of order 1 of the argument.
        /// <p />
        /// The range is partitioned into the two intervals [0, 2] and
        /// (2, infinity). Chebyshev polynomial expansions are employed
        /// in each interval.
        /// </summary>
        /// <param name="x">The value to compute the Bessel function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Bessel K1", Description = "Returns the modified Bessel function of the second kind of order 1 of the argument.")]
        public double BesselK1(double x) => SpecialFunctions.BesselK1(x);


        /// <summary> Returns the exponentially scaled modified Bessel function
        /// of the second kind of order 1 of the argument.
        /// <p />
        /// <tt>k1e(x) = exp(x) * k1(x)</tt>.
        /// </summary>
        /// <param name="x">The value to compute the Bessel function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Bessel K1e", Description = "Returns the exponentially scaled modified Bessel function of the second kind of order 1 of the argument.")]
        public double BesselK1e(double x) => SpecialFunctions.BesselK1e(x);

        /// <summary>Returns the modified Struve function of order 0.</summary>
        /// <param name="x">The value to compute the function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Struve L0", Description = "Returns the modified Struve function of order 0.")]
        public double StruveL0(double x) => SpecialFunctions.StruveL0(x);

        /// <summary>Returns the modified Struve function of order 1.</summary>
        /// <param name="x">The value to compute the function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Struve L1", Description = "Returns the modified Struve function of order 1.")]
        public double StruveL1(double x) => SpecialFunctions.StruveL1(x);

        /// <summary>
        /// Returns the difference between the Bessel I0 and Struve L0 functions.
        /// </summary>
        /// <param name="x">The value to compute the function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Bessel I0 and Struve L0", Description = "Returns the difference between the Bessel I0 and Struve L0 functions.")]
        public double BesselI0MStruveL0(double x) => SpecialFunctions.BesselI0MStruveL0(x);

        /// <summary>
        /// Returns the difference between the Bessel I1 and Struve L1 functions.
        /// </summary>
        /// <param name="x">The value to compute the function of.</param>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Bessel I1 and Struve L1", Description = "Returns the difference between the Bessel I1 and Struve L1 functions.")]
        public double BesselI1MStruveL1(double x) => SpecialFunctions.BesselI1MStruveL1(x);


        /// <summary>
        /// Numerically stable exponential minus one, i.e. <code>x -&gt; exp(x)-1</code>
        /// </summary>
        /// <param name="power">A number specifying a power.</param>
        /// <returns>Returns <code>exp(power)-1</code>.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Exponential Minus One", Description = "Numerically stable exponential minus one.")]
        public double ExponentialMinusOne(double power) => SpecialFunctions.ExponentialMinusOne(power);



        /// <summary>
        /// Numerically stable hypotenuse of a right angle triangle, i.e. <code>(a,b) -&gt; sqrt(a^2 + b^2)</code>
        /// </summary>
        /// <param name="a">The length of side a of the triangle.</param>
        /// <param name="b">The length of side b of the triangle.</param>
        /// <returns>Returns <code>sqrt(a<sup>2</sup> + b<sup>2</sup>)</code> without underflow/overflow.</returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Special Maths", Name = "Hypotenuse", Description = "Numerically stable hypotenuse of a right angle triangle.")]
        public double Hypotenuse(double a, double b) => SpecialFunctions.Hypotenuse(a, b);
   
    }
}