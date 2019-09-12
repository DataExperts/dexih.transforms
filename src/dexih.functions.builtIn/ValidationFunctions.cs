using Dexih.Utils.DataType;

namespace dexih.functions.BuiltIn
{
    public class ValidationFunctions
    {
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Clean Blank String", Description = "Checks if the string is blank or null, and sets to the defualtValue when true.")]
        public bool CleanBlankString(
            [TransformFunctionParameter(Description = "Value to test for blanks")] string value,
            [TransformFunctionParameter(Description = "Value to set when blank")] string defaultValue,
            [TransformFunctionParameter(Name ="Cleaned Output", Description = "Cleaned output value")]  out string cleanedValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                cleanedValue = defaultValue;
                return false;
            }
            cleanedValue = value;
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Clean Null", Description = "Checks if the string is null, and sets to the defualtValue when true.")]
        public bool CleanNull(
            [TransformFunctionParameter(Description = "Value to test for nulls")] string value,
            [TransformFunctionParameter(Description = "Value to set when null")] string defaultValue,
            [TransformFunctionParameter(Name = "Cleaned Output", Description = "Cleaned output value")]  out string cleanedValue)
        {
            if (string.IsNullOrEmpty(value))
            {
                cleanedValue = defaultValue;
                return false;
            }
            cleanedValue = value;
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Validate Maximum Length", Description = "Checks if the string exceeds the length, and trims the string when true.")]
        public bool MaxLength(
             [TransformFunctionParameter(Description = "Value to test for length")] string value,
            [TransformFunctionParameter(Description = "Maximum allowed length")] int maxLength,
            [TransformFunctionParameter(Name = "Cleaned Output", Description = "Cleaned output value")]  out string cleanedValue)
        {
            if (value.Length > maxLength)
            {
                cleanedValue = value.Substring(0, maxLength);
                return false;
            }
            cleanedValue = value;
            return true;
        }

        public enum EBeforeAfter
        {
            Before = 1, After
        }

        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Validate Minimum Length", Description = "Checks if the string is greater or equal to the minimum length.")]
        public bool MinLength(
         [TransformFunctionParameter(Description = "Value to test for length")] string value,
        [TransformFunctionParameter(Description = "Minimum allowed length")] int maxLength,
        [TransformFunctionParameter(Description = "Pad small strings with character.")] string padChar,
        [TransformFunctionParameter(Description = "Pad before or after string.")] EBeforeAfter padBeforeAfter,
        [TransformFunctionParameter(Name = "Cleaned Output", Description = "Cleaned output value")]  out string cleanedValue)
        {
            if (value.Length <= maxLength)
            {
                var padString = new string(padChar[0], maxLength - value.Length);
                if(padBeforeAfter == EBeforeAfter.Before)
                {
                    cleanedValue = padString + value;
                } else
                {
                    cleanedValue = value + padString;
                }
                return false;
            }
            cleanedValue = value;
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Validate Maximum Value", Description = "Checks if the number is greater than the value, and sets to the adjusted value when true.")]
        public bool MaxValue<T>(
             [TransformFunctionParameter(Description = "Value to test")] T value,
            [TransformFunctionParameter(Description = "Maximum Value")] T maxValue,
        [TransformFunctionParameter(Name = "Cleaned Output", Description = "Cleaned output value")]  out T cleanedValue)
        {
            if (Operations.GreaterThan(value, maxValue))
            {
                cleanedValue = maxValue;
                return false;
            }
            cleanedValue = value;
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Validate Minimum Value", Description = "Checks if the number is less than the value, and sets to the adjusted value when true.")]
        public bool MinValue<T>(
             [TransformFunctionParameter(Description = "Value to test")] T value,
             [TransformFunctionParameter(Description = "Minimum Value")] T minValue,
        [TransformFunctionParameter(Name = "Cleaned Output", Description = "Cleaned output value")]  out T cleanedValue)
        {
            if (Operations.LessThan(value, minValue))
            {
                cleanedValue = minValue;
                return false;
            }
            cleanedValue = value;
            return true;
        }
    }
}