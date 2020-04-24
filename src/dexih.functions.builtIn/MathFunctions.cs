using System;

namespace dexih.functions.BuiltIn
{
    public class MathFunctions
    {

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Abs",
     Description = "Returns the absolute value of a Decimal number.")]
        public double Abs(double value)
        {
            return Math.Abs(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Acos",
            Description = "Returns the angle whose cosine is the specified number.")]
        public double Acos(double value)
        {
            return Math.Acos(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Asin",
            Description = "Returns the angle whose sine is the specified number.")]
        public double Asin(double value)
        {
            return Math.Asin(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Atan",
            Description = "Returns the angle whose tangent is the specified number.")]
        public double Atan(double value)
        {
            return Math.Atan(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Atan2",
            Description = "Returns the angle whose tangent is the quotient of two specified numbers.")]
        public double Atan2(double x, double y)
        {
            return Math.Atan2(x, y);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Cos",
            Description = "Returns the cosine of the specified angle.")]
        public double Cos(double value)
        {
            return Math.Cos(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Cosh",
            Description = "Returns the hyperbolic cosine of the specified angle.")]
        public double Cosh(double value)
        {
            return Math.Cosh(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Divide Remainder",
            Description =
                "Calculates the quotient of two 32-bit signed integers and also returns the remainder in an output parameter.")]
        public int DivRem(int dividend, int divisor, out int remainder)
        {
            //return Math.DivRem(dividend, divisor, out remainder); Not working in DNX50
            var quotient = dividend / divisor;
            remainder = dividend - divisor * quotient;
            return quotient;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Exp",
            Description = "Returns e raised to the specified power.")]
        public double Exp(double value)
        {
            return Math.Exp(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "IEEERemainder",
            Description =
                "Returns the remainder resulting from the division of a specified number by another specified number.")]
        public double IeeeRemainder(double x, double y)
        {
            return Math.IEEERemainder(x, y);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Log",
            Description = "Returns the natural (base e) logarithm of a specified number.")]
        public double Log(double value)
        {
            return Math.Log(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Log(10)",
            Description = "Returns the base 10 logarithm of a specified number.")]
        public double Log10(double value)
        {
            return Math.Log10(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Pow",
            Description = "Returns a specified number raised to the specified power.")]
        public double Pow(double x, double y)
        {
            return Math.Pow(x, y);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Round",
            Description = "Rounds a decimal value to the nearest integral value.")]
        public double Round(double value)
        {
            return Math.Round(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Sin",
            Description = "Returns the sine of the specified angle.")]
        public double Sin(double value)
        {
            return Math.Sin(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Sinh",
            Description = "Returns the hyperbolic sine of the specified angle.")]
        public double Sinh(double value)
        {
            return Math.Sinh(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Sqrt",
            Description = "Returns the square root of a specified number.")]
        public double Sqrt(double value)
        {
            return Math.Sqrt(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Tan",
            Description = "Returns the tangent of the specified angle.")]
        public double Tan(double value)
        {
            return Math.Tan(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Tanh",
            Description = "Returns the hyperbolic tangent of the specified angle.")]
        public double Tanh(double value)
        {
            return Math.Tanh(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Truncate",
            Description = "Calculates the integral part of a specified decimal number.")]
        public double Truncate(double value)
        {
            return Math.Truncate(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Ceiling",
            Description =
                "Returns the smallest integral value that is greater than or equal to the specified decimal number.")]
        public decimal Ceiling(decimal value)
        {
            return decimal.Ceiling(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Floor",
            Description = "Rounds a specified Decimal number to the closest integer toward negative infinity.")]
        public decimal Floor(decimal value)
        {
            return decimal.Floor(value);
        }

        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Remainder",
            Description = "Computes the remainder after dividing two Decimal values.")]
        public decimal Remainder(decimal value1, decimal value2)
        {
            return decimal.Remainder(value1, value2);
        }

 
    }
}
