using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.XPath;
using dexih.functions;
using Newtonsoft.Json.Linq;

namespace dexih.standard.functions
{
    public class RowFunctions
    {
        //The cache parameters are used by the functions to maintain a state during a transform process.
        private int? _cacheInt;
        private DateTime? _cacheDate;
        private string[] _cacheArray;
        private XPathNodeIterator _cacheXmlNodeList;
        private JToken[] _cacheJsonTokens;


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

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "SplitColumnToRows",
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

            item = _cacheArray[(int) _cacheInt];
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "XPathNodesToRows",
            Description = "Split an XPath query into multiple rows", ResetMethod = nameof(Reset))]
        public bool XPathNodesToRows(string xml, string xPath, int maxItems, out string node)
        {
            if (_cacheXmlNodeList == null)
            {
                var stream = new StringReader(xml);
                var xPathDocument = new XPathDocument(stream);

                _cacheXmlNodeList = xPathDocument.CreateNavigator().Select(xPath);
                _cacheInt = 0;
            }

            if ((maxItems > 0 && _cacheInt > maxItems - 1) || _cacheXmlNodeList.MoveNext() == false)
            {
                node = "";
                return false;
            }

            _cacheInt++;
            node = _cacheXmlNodeList.Current.InnerXml;
            return true;
        }

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "JSONElementsToRows",
            Description = "Split a JSON array into separate rows", ResetMethod = nameof(Reset))]

        public bool JsonElementsToRows(string json, string jsonPath, int maxItems, out string item)
        {
            if (_cacheJsonTokens == null)
            {
                var results = JToken.Parse(json);
                _cacheJsonTokens = string.IsNullOrEmpty(jsonPath)
                    ? results.ToArray()
                    : results.SelectTokens(jsonPath).ToArray();
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

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Rows", Name = "JsonPivotElementToRows",
            Description = "Splits the properties of a Json element into rows containing the property name and value.", ResetMethod = nameof(Reset))]
        public bool JsonPivotElementToRows(string json, string jsonPath, int maxItems, out string name,
            out string value)
        {
            if (_cacheJsonTokens == null)
            {
                var results = JToken.Parse(json);

                _cacheJsonTokens = string.IsNullOrEmpty(jsonPath)
                    ? results.SelectTokens(" ").ToArray()
                    : results.SelectTokens(jsonPath).ToArray();

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
            value = item.Values().FirstOrDefault()?.ToString();
            return true;
        }
    }
}