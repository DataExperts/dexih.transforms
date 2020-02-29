using System;
using System.Linq;
using System.Xml;
using dexih.functions.Exceptions;
using dexih.functions.Parameter;
using Newtonsoft.Json.Linq;


namespace dexih.functions.BuiltIn
{
    public class RowFunctions
    {
        //The cache parameters are used by the functions to maintain a state during a transform process.
        private int? _cacheInt;
        private DateTime? _cacheDate;
        private string[] _cacheArray;
        private XmlNodeList _cacheXmlNodeList;
        private JToken[] _cacheJsonTokens;

        /// <summary>
        /// Used by row transform, contains the parameters used in the array.
        /// </summary>
        [Parameters]
        public Parameters Parameters { get; set; }


        public bool Reset()
        {
            _cacheInt = null;
            _cacheDate = null;
            _cacheArray = null;
            _cacheXmlNodeList = null;
            _cacheJsonTokens = null;
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "Generate Sequence",
            Description = "Generate rows with a sequence number field.", ResetMethod = nameof(Reset))]
        public bool GenerateSequence(int start, int end, int step, out int sequence)
        {
            if (_cacheInt == null)
                _cacheInt = start;

            sequence = (int) _cacheInt;
            _cacheInt = _cacheInt + step;

            if (sequence > end)
                return false;
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "Generate Date Sequence",
            Description = "Generate rows from start to end date.", ResetMethod = nameof(Reset))]
        public bool GenerateDateSequence(DateTime start, DateTime end, int step, out DateTime sequence)
        {
            if (_cacheDate == null)
                _cacheDate = start;

            sequence = (DateTime) _cacheDate;
            _cacheDate = _cacheDate.Value.AddDays(step);

            if (sequence > end)
                return false;
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "Split Column To Rows",
            Description = "Split a delimited value into rows.", ResetMethod = nameof(Reset))]
        public bool SplitColumnToRows(string separator, string value, int maxItems, out string item)
        {
            if (_cacheArray == null)
            {
                _cacheArray = value.Split(separator.ToCharArray(), maxItems + 1);
                _cacheInt = 0;
            }
            else
            {
                _cacheInt++;
            }

            if ((maxItems > 0 && _cacheInt > maxItems - 1) || _cacheInt > _cacheArray.Length - 1)
            {
                item = "";
                return false;
            }

            item = _cacheArray[_cacheInt.Value];
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "Columns To Rows",
            Description = "Columns into rows.", ResetMethod = nameof(Reset))]
        public bool ColumnsToRows<T>(T[] column, out string columnName, out T item)
        {
            if (_cacheInt == null)
            {
                _cacheInt = 0;
            }
            else
            {
                _cacheInt++;
            }

            if (_cacheInt > column.Length - 1)
            {
                item = default;
                columnName = "";
                return false;
            }

            item = column[_cacheInt.Value];

            if (Parameters?.Inputs == null)
            {
                throw new FunctionException($"The parameters.inputs was not set in the column to rows function.");                
            }
            
            if (Parameters.Inputs[0] is ParameterArray parameterArray && parameterArray.Parameters[_cacheInt.Value] is ParameterColumn parameterColumn)
            {
                columnName = parameterColumn.Column.Name;
            }
            else
            {
                throw new FunctionException($"The parameter {Parameters.Inputs[(int) _cacheInt].Name} is not using a column input.");                
            }
            
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "XPath Nodes To Rows",
            Description = "Split an XPath query into multiple rows", ResetMethod = nameof(Reset))]
        public bool XPathNodesToRows(XmlDocument xml, string xPath, int maxItems, out string node)
        {
            if (_cacheXmlNodeList == null)
            {
                _cacheXmlNodeList = xml.SelectNodes(xPath);
                _cacheInt = 0;
            }

            if ((maxItems > 0 && _cacheInt > maxItems - 1) || _cacheInt >= _cacheXmlNodeList.Count)
            {
                node = "";
                return false;
            }

            node = _cacheXmlNodeList[_cacheInt.Value].InnerXml;
            _cacheInt++;
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "JSON Elements To Rows",
            Description = "Split a JSON array into separate rows", ResetMethod = nameof(Reset))]

        public bool JsonElementsToRows(string jsonString, string jsonPath, int maxItems, out string item)
        {
            var json = JToken.Parse(jsonString);
            
            if (_cacheJsonTokens == null)
            {
                // var results = JToken.Parse(json);
                _cacheJsonTokens = string.IsNullOrEmpty(jsonPath)
                    ? json.ToArray()
                    : json.SelectTokens(jsonPath).ToArray();
                _cacheInt = 0;
            }
            else
                _cacheInt++;

            if ((maxItems > 0 && _cacheInt > maxItems - 1) || _cacheInt > _cacheJsonTokens.Length - 1)
            {
                item = "";
                return false;
            }

            item = _cacheJsonTokens[(int) _cacheInt].ToString();
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "Json Pivot Element To Rows",
            Description = "Splits the properties of a Json element into rows containing the property name and value.", ResetMethod = nameof(Reset))]
        public bool JsonPivotElementToRows(string jsonString, string jsonPath, int maxItems, out string name,
            out string value)
        {
            var json = JToken.Parse(jsonString);
            
            if (json == null)
            {
                throw new FunctionException("The json value contained no data.");
            }
            
            if (_cacheJsonTokens == null)
            {
                // var results = JToken.Parse(json);

                _cacheJsonTokens = string.IsNullOrEmpty(jsonPath)
                    ? json.SelectTokens(" ").ToArray()
                    : json.SelectTokens(jsonPath).ToArray();

                _cacheInt = 0;
            }
            else
            {
                _cacheInt++;
            }

            var item = _cacheJsonTokens == null || _cacheJsonTokens.Length == 0
                ? null
                : _cacheJsonTokens[0].ElementAtOrDefault((int) _cacheInt);
            if ((maxItems > 0 && _cacheInt > maxItems - 1) || item == null)
            {
                name = "";
                value = "";
                return false;
            }

            var property = (JProperty) item;
            name = property.Name;

            var count = item.Values().Count();
            if (count == 0)
            {
                value = null;
            } else if (count == 1)
            {
                value = item.Values().FirstOrDefault()?.ToString();
            }
            else
            {
                value = item.Children().FirstOrDefault()?.ToString();
            }
            return true;
        }
        
        
        
    }
}