namespace dexih.functions.BuiltIn
{
    public class ValidationFunctions
    {
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Default Null String", Description = "Checks if the string is null, and sets to the defualtValue when true.")]
        public bool DefaultNullString(string value, string defaultValue, out string adjustedValue)
        {
            if (value == null)
            {
                adjustedValue = defaultValue;
                return false;
            }
            adjustedValue = value;
            return true;
        }
        
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
        
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Default Null Number", Description = "Checks if the number is null, and sets to the defualtValue when true.")]
        public bool DefaultNullNumber(decimal? value, decimal defaultValue, out decimal adjustedValue)
        {
            if (value == null)
            {
                adjustedValue = defaultValue;
                return false;
            }
            adjustedValue = (decimal)value;
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
        
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Maximum Value", Description = "Checks if the number is greater than the value, and sets to the value when true.")]
        public bool MaxValue(decimal value, decimal maxValue, out decimal adjustedValue)
        {
            if (value > maxValue)
            {
                adjustedValue = maxValue;
                return false;
            }
            adjustedValue = value;
            return true;
        }
    }
}