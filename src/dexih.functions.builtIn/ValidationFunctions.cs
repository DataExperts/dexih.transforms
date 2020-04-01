using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;
using Dexih.Utils.DataType;

namespace dexih.functions.BuiltIn
{
    public class ValidationFunctions
    {
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Clean Blank String", Description = "Checks if the string is blank or null, and sets to the default value when true.")]
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

        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Clean Null", Description = "Checks if the string is null, and sets to the default value when true.", GenericType = EGenericType.All)]
        public bool CleanNull<T>(
            [TransformFunctionParameter(Description = "Value to test for nulls")] T value,
            [TransformFunctionParameter(Description = "Value to set when null")] T defaultValue,
            [TransformFunctionParameter(Name = "Cleaned Output", Description = "Cleaned output value")]  out T cleanedValue)
        {
            if (EqualityComparer<T>.Default.Equals(value, default(T)))
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
        
        [TransformFunction(FunctionType = EFunctionType.Validate, Category = "Validation", Name = "Validate Email", Description = "Checks if the email address has the correct format")]
        public bool IsValidEmail(string email)
        {
            if (string.IsNullOrWhiteSpace(email))
                return false;

            try
            {
                // Normalize the domain
                email = Regex.Replace(email, @"(@)(.+)$", DomainMapper,
                    RegexOptions.None, TimeSpan.FromMilliseconds(200));

                // Examines the domain part of the email and normalizes it.
                string DomainMapper(Match match)
                {
                    // Use IdnMapping class to convert Unicode domain names.
                    var idn = new IdnMapping();

                    // Pull out and process domain name (throws ArgumentException on invalid)
                    var domainName = idn.GetAscii(match.Groups[2].Value);

                    return match.Groups[1].Value + domainName;
                }
            }
            catch (RegexMatchTimeoutException)
            {
                return false;
            }
            catch (ArgumentException)
            {
                return false;
            }

            try
            {
                return Regex.IsMatch(email,
                    @"^(?("")("".+?(?<!\\)""@)|(([0-9a-z]((\.(?!\.))|[-!#\$%&'\*\+/=\?\^`\{\}\|~\w])*)(?<=[0-9a-z])@))" +
                    @"(?(\[)(\[(\d{1,3}\.){3}\d{1,3}\])|(([0-9a-z][-0-9a-z]*[0-9a-z]*\.)+[a-z0-9][\-a-z0-9]{0,22}[a-z0-9]))$",
                    RegexOptions.IgnoreCase, TimeSpan.FromMilliseconds(250));
            }
            catch (RegexMatchTimeoutException)
            {
                return false;
            }
        }
    }
}