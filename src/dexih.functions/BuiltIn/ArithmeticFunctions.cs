using Dexih.Utils.DataType;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace dexih.functions.BuiltIn
{
    public class ArithmeticFunctions<T>
    {

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Sign",
            Description = "Returns a value indicating the sign of a decimal number.", GenericType = EGenericType.Numeric)]
        public int Sign(T value)
        {
            return Operations.Compare(value, default(T));
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Add",
            Description = "Adds two or more specified Decimal values.", GenericType = EGenericType.Numeric)]
        public T Add(T value1, T[] value2)
        {
            return Operations.Add(value1, value2.Aggregate(Operations.Add));
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Divide",
            Description = "Divides two specified Decimal values.", GenericType = EGenericType.Numeric)]
        public T Divide(T value1, T value2)
        {
            return Operations.Divide(value1, value2);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Multiply",
            Description = "Multiplies two or more specified Decimal values.", GenericType = EGenericType.Numeric)]
        public T Multiply(T value1, T[] value2)
        {
            return Operations.Multiply(value1, value2.Aggregate(Operations.Multiply));
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Negate",
            Description = "Returns the result of multiplying the specifiedDecimal value by negative one.", GenericType = EGenericType.Numeric)]
        public T Negate(T value)
        {
            return Operations.Negate(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Maths", Name = "Subtract",
            Description = "Subtracts one or more specified Decimal values from another.", GenericType = EGenericType.Numeric)]
        public T Subtract(T value1, T[] value2)
        {
            return Operations.Subtract(value1, value2.Aggregate(Operations.Add));
        }
    }
}
