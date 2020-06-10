using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;


namespace dexih.functions.Query
{
    [DataContract]
    public class InputParameters: List<InputParameter>
    {
        public void Add(string name, object value, int rank)
        {
            Add(new InputParameter() { Name =  name, Value = value, Rank = rank});
        }

        /// <summary>
        /// Replaces parameters with values
        /// </summary>
        /// <returns></returns>
        public object SetParameters(object data, int rank)
        {
            var items = new List<string>();
            if (data is Array arrayValues && rank == 1)
            {
                // if the values are json, then just return the "key" attributes.
                var values = new List<object>();
                foreach(var arrayValue in arrayValues)
                {
                    if (arrayValue is JsonElement jsonElement)
                    {
                        if (jsonElement.ValueKind == JsonValueKind.Object)
                        {
                            var objectElements = jsonElement.EnumerateObject();
                            foreach (var element in objectElements.Where(c => c.Name == "key"))
                            {
                                var value = element.Value.GetString();
                                var result = SetParameter(value, rank);
                                if (result == null)
                                {
                                    continue;
                                }
                                if (result is object[] resultList)
                                {
                                    values.AddRange(resultList);
                                }
                                else
                                {
                                    values.Add(result.ToString());
                                }
                            }
                        }
                    }
                    else
                    {
                        items.Add(arrayValue.ToString());
                    }
                }

                return values.ToArray();
            }
            else if (data is string dataString)
            {
                if (string.IsNullOrEmpty(dataString))
                {
                    return null;
                }

                return SetParameter(dataString, rank);
            }
            else
            {
                return null;
            }
            
        }

        private object SetParameter(string value, int rank)
        {
            {
                var ignoreNext = false;
                var openStart = -1;
                var previousPos = 0;
                StringBuilder newValue = null;

                for (var pos = 0; pos < value.Length; pos++)
                {
                    var character = value[pos];

                    if (ignoreNext)
                    {
                        ignoreNext = false;
                        continue;
                    }

                    // backslash is escape character, so ignore next value when one is found.
                    if (character == '\\')
                    {
                        ignoreNext = true;
                        continue;
                    }

                    if (openStart == -1 && character == '{')
                    {
                        openStart = pos;
                        continue;
                    }

                    if (openStart >= 0 && character == '}')
                    {
                        var name = value.Substring(openStart + 1, pos - openStart - 1);
                        var parameter = Find(c => c.Name == name);

                        if (parameter != null)
                        {
                            // if the rank is 0, then insert the variable value into the correct position
                            if (rank == 0 && parameter.Rank == 0)
                            {
                                if (newValue == null)
                                {
                                    newValue = new StringBuilder();
                                }

                                newValue.Append(value.Substring(previousPos, openStart - previousPos));
                                newValue.Append(parameter.Value);
                                previousPos = pos + 1;
                            }

                            // if the rank is 1, then just return the array.  
                            if (rank == 1 || parameter.Rank == 1)
                            {
                                if (parameter.Value is Array array)
                                {
                                    if (array.Length == 0)
                                    {
                                        return new string[0];
                                    }
                                    else
                                    {
                                        var values = new List<object>();
                                        foreach (var arrayValue in array)
                                        {
                                            // if the values are json, then just return the "key" attributes.
                                            if (arrayValue is JsonElement jsonElement)
                                            {
                                                if (jsonElement.ValueKind == JsonValueKind.Object)
                                                {
                                                    var objectElements = jsonElement.EnumerateObject();
                                                    foreach (var element in objectElements.Where(c => c.Name == "key"))
                                                    {
                                                        values.Add(element.Value.GetString());
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                values.Add(arrayValue.ToString());
                                            }
                                        }

                                        return values.ToArray();
                                    }
                                }
                                else
                                {
                                    if (rank == 1)
                                    {
                                        return new[] {parameter.Value};
                                    }
                                    else
                                    {
                                        return parameter.Value;
                                    }
                                }
                            }
                        }

                        openStart = -1;
                    }
                }

                if (newValue == null)
                {
                    return value;
                }
                else
                {
                    newValue.Append(value.Substring(previousPos));
                    return newValue.ToString();
                }
            }
        }
    }
}