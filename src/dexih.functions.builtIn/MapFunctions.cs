using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml;
using dexih.functions.Exceptions;
using Dexih.Utils.DataType;
using Newtonsoft.Json.Linq;


namespace dexih.functions.BuiltIn
{
    /// <summary>
    /// 
    /// </summary>

    public class MapFunctions
    {

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Concatenate Strings",
            Description = "Concatenates multiple string fields.")]
        public string Concat([TransformFunctionParameter(Description = "Array of Values to Concatenate")] string[] values)
        {
            return string.Concat(values);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Index Of",
            Description = "The zero-based index of the first occurrence of the specified string in this field.")]
        public int IndexOf(string value, string search)
        {
            return value.IndexOf(search, StringComparison.Ordinal);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Insert",
            Description =
                "Returns a new string in which a specified string is inserted at a specified index position in this instance.")]
        public string Insert(string value, int startIndex, string insertString)
        {
            return value.Insert(startIndex, insertString);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Join",
            Description =
                "Concatenates all the elements of a string array, using the specified separator between each element.")]
        public string Join(string separator, string[] values)
        {
            return string.Join(separator, values);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Regex Replace",
            Description = "Replaces a string value with a regular expression match.")]
        public string RegExMatch(string input, string regEx, string replacement)
        {
            return Regex.Replace(input, regEx, replacement);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Pad Left",
            Description =
                "Returns a new string that right-aligns the characters in this instance by padding them with spaces on the left, for a specified total length.")]
        public string PadLeft(string value, int width, string paddingChar)
        {
            return value.PadLeft(width, paddingChar[0]);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Pad Right",
            Description =
                "Returns a new string that left-aligns the characters in this string by padding them with spaces on the right, for a specified total length.")]
        public string PadRight(string value, int width, string paddingChar)
        {
            return value.PadRight(width, paddingChar[0]);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Remove",
            Description =
                "Returns a new string in which a specified number of characters in the current instance beginning at a specified position have been deleted.")]
        public string Remove(string value, int startIndex, int count)
        {
            return value.Remove(startIndex, count);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Replace",
            Description =
                "Returns a new string in which all occurrences of a specified string in the current instance are replaced with another specified string.")]
        public string Replace(string value, string oldValue, string newValue)
        {
            if (string.IsNullOrEmpty(value))
                return null;

            if (string.IsNullOrEmpty(oldValue))
                return value;

            if (newValue == null)
                newValue = "";

            return value.Replace(oldValue, newValue);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Split",
            Description = "Splits a string into multiple return fields that are based on the characters in an array.")]
        public int Split(string value, string separator, int? count, out string[] result)
        {
            if (string.IsNullOrEmpty(value) || string.IsNullOrEmpty(separator))
            {
                result = null;
                return 0;
            }

            result = count == null ? value.Split(separator.ToCharArray()) : value.Split(separator.ToCharArray(), count.Value);
            
            return result.Length;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Substring",
            Description =
                "Retrieves a substring from this instance. The substring starts at a specified character position and has a specified length.")]
        public string Substring(string stringValue, int start, int length)
        {
            var stringLength = stringValue.Length;
            if (start > stringLength) return "";
            if (start + length > stringLength) return stringValue.Substring(start);
            return stringValue.Substring(start, length);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "To Lowercase",
            Description = "Returns a copy of this string converted to lowercase.")]
        public string ToLower(string value)
        {
            return value.ToLower();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "To Uppercase",
            Description = "Returns a copy of this string converted to uppercase.")]
        public string ToUpper(string value)
        {
            return value.ToUpper();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Trim",
            Description = "Removes all leading and trailing white-space characters from the current field.")]
        public string Trim(string value)
        {
            return value.Trim();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Trim End",
            Description =
                "Removes all leading and trailing occurrences of a set of characters specified in an array from the current field.")]
        public string TrimEnd(string value)
        {
            return value.TrimEnd();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Trim Start",
            Description =
                "Removes all trailing occurrences of a set of characters specified in an array from the current field.")]
        public string TrimStart(string value)
        {
            return value.TrimStart();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Length",
            Description = "Return the length of the string.")]
        public int Length(string value)
        {
            return value.Length;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Word Count",
            Description = "Returns the number of words in the string.")]
        public int WordCount(string value)
        {
            if (string.IsNullOrEmpty(value))
                return 0;

            var c = 0;
            for (var i = 1; i < value.Length; i++)
                if (char.IsWhiteSpace(value[i - 1]))
                    if (char.IsLetterOrDigit(value[i]) || char.IsPunctuation(value[i]))
                        c++;
            if (value.Length > 2) c++;
            return c;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Word Extract",
            Description = "Returns the nth word (starting from 0) in the string.")]
        public string WordExtract(string value, int wordNumber)
        {
            if (string.IsNullOrEmpty(value))
                return null;

            var start = 0;
            var c = 0;
            int i;
            for (i = 1; i < value.Length; i++)
                if (char.IsWhiteSpace(value[i - 1]))
                    if (char.IsLetterOrDigit(value[i]) || char.IsPunctuation(value[i]))
                    {
                        c++;
                        if (c == wordNumber) start = i;
                        else if (c > wordNumber)
                            break;
                    }

            if (value.Length > 2) c++;
            return c == 0 || c <= wordNumber ? "" : value.Substring(start, i - start);
        }
        
     

 

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Xml", Name = "XPath Values",
            Description = "Parses an xml string into a series of xpath results.")]
        public bool XPathValues(XmlDocument xml, [TransformFunctionLinkedParameter("XPath to Value")] string[] xPaths, [TransformFunctionLinkedParameter("XPath to Value")] out string[] values)
        {
            var returnValue = true;

            try
            {
                //XmlDocument xmlDoc = new XmlDocument();
                //xmlDoc.LoadXml(xml);

                //var stream = new StringReader(xml);
                //var xPathDocument = new XPathDocument(stream);
                //var xPathNavigator = xPathDocument.CreateNavigator();

                values = new string[xPaths.Length];

                for (var i = 0; i < xPaths.Length; i++)
                {
                    var xmlNode = xml.SelectSingleNode(xPaths[i]);
                    //var xmlNode = xPathNavigator.SelectSingleNode(xPaths[i]);

                    //((XmlElement) xmlNode)?.RemoveAttribute("xmlns");

                    if (xmlNode == null)
                    {
                        returnValue = false;
                        values[i] = null;
                    }
                    else
                    {
                        values[i] = xmlNode.InnerXml;
                    }
                }

                return returnValue;
            }
            catch
            {
                values = new string[xPaths.Length];
                return false;
            }
        }

        [TransformFunction(
            FunctionType = EFunctionType.Map, 
            Category = "JSON", 
            Name = "JSON Values",
            Description = "Parses a JSON string into a series of elements.  The JSON string must contain only one result set.",
            ImportMethod = nameof(JsonValuesImport))
        ]
        public bool JsonValues(string json, [TransformFunctionLinkedParameter("JsonPath to Value")] string[] jsonPaths, [TransformFunctionLinkedParameter("JsonPath to Value")] out string[] values)
        {
            try
            {
                var returnValue = true;

                var jToken = JToken.Parse(json);

                values = new string[jsonPaths.Length];

                for (var i = 0; i < jsonPaths.Length; i++)
                {
                    var token = jToken.SelectToken(jsonPaths[i]);
                    if (token == null)
                    {
                        returnValue = false;
                        values[i] = null;
                    }
                    else
                    {
                        values[i] = token.ToString();
                    }
                }

                return returnValue;
            }
            catch
            {
                values = new string[jsonPaths.Length];
                return false;
            }
        }

        public string[] JsonValuesImport(string json)
        {
            var result = JToken.Parse(json);
            var values = JsonValueImportRecurse(result);
            return values.ToArray();
        }

        private List<string> JsonValueImportRecurse(JToken jToken)
        {
            var values = new List<string>();
            
            foreach (var child in jToken.Children())
            {
                if (child.Type == JTokenType.Object || child.Type == JTokenType.Property)
                {
                    values.AddRange(JsonValueImportRecurse(child));
                }
                else
                {
                    values.Add(child.Path);
                }
            }

            return values;
        }
        
        [TransformFunction(
            FunctionType = EFunctionType.Map, 
            Category = "JSON", 
            Name = "Json Array To Columns",
            Description = "Pivots a json array into column values. ",
            ImportMethod = nameof(JsonArrayToColumnsImport))
        ]
        public bool JsonArrayToColumns(string jsonString, string jsonPath, string columnPath, string valuePath, [TransformFunctionLinkedParameter("Column to Value")] string[] columns, [TransformFunctionLinkedParameter("Column to Value")] out string[] values)
        {
            try
            {
                var json = JToken.Parse(jsonString);

                JToken array;
                if (string.IsNullOrEmpty(jsonPath))
                {
                    array = json;
                }
                else
                {
                    array = json.SelectToken(jsonPath);
                }

                if (array.Type == JTokenType.Property || array.Type == JTokenType.Object)
                {
                    array = array.FirstOrDefault();
                }

                if (array == null || array.Type != JTokenType.Array)
                {
                    throw new FunctionException($"The jsonPath {jsonPath} did not point to a json array.");
                }
                
                var keyValues = new Dictionary<string, string>();

                foreach (var item in array.AsJEnumerable())
                {
                    keyValues.Add(item.SelectToken(columnPath).ToString(), item.SelectToken(valuePath).ToString());
                }

                values = new string[columns.Length];
                for(var i = 0; i < columns.Length; i++)
                {
                    if (keyValues.ContainsKey(columns[i]))
                    {
                        values[i] = keyValues[columns[i]];
                    }
                }

                return true;
            }
            catch(Exception)
            {
                values = new string[columns.Length];
                return false;
            }
        }

        public string[] JsonArrayToColumnsImport(string jsonString, string jsonPath, string columnPath)
        {
            var json = JToken.Parse(jsonString);
            JToken array;
            if (string.IsNullOrEmpty(jsonPath))
            {
                array = json;
            }
            else
            {
                array = json.SelectToken(jsonPath);
            }

            if (array.Type == JTokenType.Property || array.Type == JTokenType.Object)
            {
                array = array.FirstOrDefault();
            }

            if (array == null || array.Type != JTokenType.Array)
            {
                throw new FunctionException($"The jsonPath {jsonPath} did not point to a json array.");
            }
                
            var columns = new List<string>();

            foreach (var item in array.AsJEnumerable())
            {
                var column = item.SelectToken(columnPath).ToString();
                if (!string.IsNullOrEmpty(column))
                {
                    columns.Add(column);
                }
            }

            return columns.ToArray();
        }

 

      
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Split Fixed Width",
            Description = "Splits a string based on the string positions specified.")]
        public string[] SplitFixedWidth(string value, int[] positions)
        {
            var previousPos = 0;
            var result = new string[positions.Length];

            for (var i = 0; i < positions.Length; i++)
            {
                result[i] = value.Substring(previousPos, positions[i] - previousPos);
                previousPos = positions[i];
            }

            return result;
        }
        
        

       

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Create SHA1/Hash",
            Description = "Creates a sha1 hash of the value.  This will (virtually) always be unique for any different value.")]
        public string CreateSHA1(string value)
        {
            return value?.CreateSHA1();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Maximum Number",
            Description = "Returns the highest value in the array ")]
        public T MaxValue<T>(T[] value)
        {
            return value.Max();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Minimum Number",
            Description = "Returns the lowest value in the array ")]
        public T MinValue<T>(T[] value)
        {
            return value.Min();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "String", Name = "Switch Condition", GenericTypeDefault = ETypeCode.String,
            Description = "Maps the 'value' to the matching 'when' and returns the 'then'.  No matches returns default value (or original value if default is null."),
        ]
        public T Switch<T>(object value, [TransformFunctionLinkedParameter("When")] object[] when, [TransformFunctionLinkedParameter("When")] T[] then, T defaultValue)
        {
            for(var i = 0; i < when.Length; i++)
            {
                if (i > then.Length)
                {
                    break;
                }

                if (Operations.Equal(value, when[i]))
                {
                    return then[i];
                }
            }

            return defaultValue;
        }

    }

}

 