using System.Collections.Generic;
using System.Text;
using dexih.repository;

namespace dexih.functions.Query
{
    public class InputParameters: List<InputParameter>
    {
        public void Add(string name, string value)
        {
            Add(new InputParameter() { Name =  name, Value = value});
        }
        
            /// <summary>
        /// Replaces parameters with values
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public string SetParameters(object data)
        {
            if (!(data is string value)) return null;
            if (string.IsNullOrEmpty(value))
            {
                return null;
            }

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
                        if (newValue == null)
                        {
                            newValue = new StringBuilder();
                        }

                        newValue.Append(value.Substring(previousPos, openStart - previousPos));
                        newValue.Append(parameter.Value);
                        previousPos = pos + 1;
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