using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.XPath;
using Newtonsoft.Json.Linq;
using System.Reflection;

namespace dexih.functions
{
    public class StandardValidations
    {
        const string NullPlaceHolder = "A096F007-26EE-479E-A9E1-4E12427A5AF0"; //used a a unique string that can be substituted for null

        public bool Reset()
        {
            return true;
        }

        public static Function GetValidationReference(string FunctionName, string[] inputMappings, string targetColumn, string[] outputMappings)
        {
            return new Function(typeof(StandardValidations), FunctionName, FunctionName + "Result", "Reset", inputMappings, targetColumn, outputMappings);
        }

        public static Function GetValidationReference(string FunctionName)
        {
            if (typeof(StandardValidations).GetMethod(FunctionName) == null)
                throw new Exception("The method " + FunctionName + " was not found in the validation functions");
            return new Function(typeof(StandardValidations), FunctionName, FunctionName + "Result", "Reset", null, null, null);
        }

        public bool MaxLength(string value, int maxLength, out string cleanedValue)
        {
            if (value.Length > maxLength)
            {
                cleanedValue = value.Substring(0, maxLength);
                return false;
            }
            else
            {
                cleanedValue = null;
                return true;
            }
        }

        public bool MaxValue(Decimal value, Decimal maxValue, out Decimal cleanedValue)
        {
            if (value > maxValue)
            {
                cleanedValue = 0;
                return false;
            }
            else
            {
                cleanedValue = 0;
                return true;
            }
        }

    }
}

 