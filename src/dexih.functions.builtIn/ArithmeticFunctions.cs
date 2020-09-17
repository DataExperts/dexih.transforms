using System;
using System.ComponentModel;
using Dexih.Utils.DataType;
using System.Linq;

namespace dexih.functions.BuiltIn
{
    public enum EDivideByZero
    {
        [Description("Error")]
        Error = 1,
        
        [Description("Zero/Null")]
        Zero,
        
        [Description("Inifinity")]
        Infinity
    }
    
    public class ArithmeticFunctions<T>
    {

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Sign",
            Description = "Returns a value indicating the sign of a decimal number.", GenericType = EGenericType.Numeric, GenericTypeDefault = ETypeCode.Decimal)]
        public int Sign(T value)
        {
            return Operations.Compare(value, default);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Add",
            Description = "Adds two or more specified Decimal values.", GenericType = EGenericType.Numeric, GenericTypeDefault = ETypeCode.Decimal)]
        public T Add(T value1, T[] value2)
        {
            return Operations.Add(value1, value2.Aggregate(Operations.Add));
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Divide",
            Description = "Divides two specified Decimal values.", GenericType = EGenericType.Numeric, GenericTypeDefault = ETypeCode.Decimal)]
        public T Divide(T value1, T value2, EDivideByZero divideByZero = EDivideByZero.Error)
        {
            if (Equals(value2, default(T)))
            {
                switch (divideByZero)
                {
                    case EDivideByZero.Error:
                        throw new DivideByZeroException("Cannot divide by zero.  Change the DivideByZero value to correct this.");
                    case EDivideByZero.Zero:
                        return default;
                    case EDivideByZero.Infinity:
                        var type = typeof(T);
                        if (type == typeof(double))
                        {
                            if (Sign(value1) < 0)
                            {
                                return (T)(object) double.NegativeInfinity;
                            }
                            return (T)(object) double.PositiveInfinity;
                        } else if (type == typeof(float))
                        {
                            if (Sign(value1) < 0)
                            {
                                return (T)(object) float.NegativeInfinity;
                            }
                            return (T)(object) float.PositiveInfinity;
                        }
                        else
                        {
                            throw new DivideByZeroException("Divide by zero failed as Infinity can only be returned for double/float types.");
                        }
                    default:
                        throw new ArgumentOutOfRangeException(nameof(divideByZero), divideByZero, null);
                }
            }

            return Operations.Divide(value1, value2);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Multiply",
            Description = "Multiplies two or more specified Decimal values.", GenericType = EGenericType.Numeric, GenericTypeDefault = ETypeCode.Decimal)]
        public T Multiply(T value1, T[] value2)
        {
            return Operations.Multiply(value1, value2.Aggregate(Operations.Multiply));
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Negate",
            Description = "Returns the result of multiplying the specifiedDecimal value by negative one.", GenericType = EGenericType.Numeric, GenericTypeDefault = ETypeCode.Decimal)]
        public T Negate(T value)
        {
            return Operations.Negate(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Subtract",
            Description = "Subtracts one or more specified Decimal values from another.", GenericType = EGenericType.Numeric, GenericTypeDefault = ETypeCode.Decimal)]
        public T Subtract(T value1, T[] value2)
        {
            return Operations.Subtract(value1, value2.Aggregate(Operations.Add));
        }
    }

}
