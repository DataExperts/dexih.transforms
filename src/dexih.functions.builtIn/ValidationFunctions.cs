using Dexih.Utils.DataType;

namespace dexih.functions.BuiltIn
{
    public class ValidationFunctions
    {
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Default Blank String", Description = "Checks if the string is blank or null, and sets to the defualtValue when true.")]
        public bool DefaultBlankString(string value, string defaultValue, out string adjustedValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                adjustedValue = defaultValue;
                return false;
            }
            adjustedValue = value;
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Set Value to Null or 0", Description = "Replaces the specified value with null or 0 value.")]
        public bool SetValueToNull<T>(T value, T checkValue, out T adjustedValue)
        {
            if (Equals(value, checkValue))
            {
                adjustedValue = default(T);
                return false;
            }

            adjustedValue = value;
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Set Value to Default", Description = "Replaces the specified value with another value.")]
        public bool SetValueToDefault<T>(T value, T checkValue, T defaultValue, out T adjustedValue)
        {
            if (Equals(value, checkValue))
            {
                adjustedValue = defaultValue;
                return false;
            }

            adjustedValue = value;
            return true;
        }

        
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Maximum Length", Description = "Checks if the string exceeds the length, and trims the string when true.")]
        public bool MaxLength(string value, int maxLength, out string trimmedValue)
        {
            if (value.Length > maxLength)
            {
                trimmedValue = value.Substring(0, maxLength);
                return false;
            }
            trimmedValue = null;
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Maximum Value", Description = "Checks if the number is greater than the value, and sets to the adjusted value when true.")]
        public bool MaxValue<T>(T value, T maxValue, out T adjustedValue)
        {
            if (Operations.GreaterThan(value, maxValue))
            {
                adjustedValue = maxValue;
                return false;
            }
            adjustedValue = value;
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Minimum Value", Description = "Checks if the number is less than the value, and sets to the adjusted value when true.")]
        public bool MinValue<T>(T value, T minValue, out T adjustedValue)
        {
            if (Operations.LessThan(value, minValue))
            {
                adjustedValue = minValue;
                return false;
            }
            adjustedValue = value;
            return true;
        }
    }
}