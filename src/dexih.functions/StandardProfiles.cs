using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace dexih.functions
{
    public class StandardProfiles
    {
        private readonly Dictionary<string, int> _dictionary = new Dictionary<string, int>();
        private int _intValue = 0;
        private object _objectValue = null;

        private int _recordCount = 0;
        private bool _detailedResults;

        public static Function GetProfileReference(bool detailedResults, string profileName, string inputColumn)
        {
            StandardProfiles newProfile = new StandardProfiles(detailedResults);
            return new Function(newProfile, profileName, profileName + "Result", "Reset", new string[] { inputColumn }, "Result", new string[] { "Distribution" });
        }

        public void Reset()
        {
            _intValue = 0;
            _objectValue = null;
            _dictionary.Clear();
            _recordCount = 0;
        }

        public StandardProfiles(bool DetailedResults = false)
        {
            _detailedResults = DetailedResults;
        }

        public StandardProfiles()
        {
            _detailedResults = false;
        }

        public void BestDataType(string value)
        {
            _recordCount++;

            //only makes recommendations on string types, otherwise uses the type of the field.
            string sValue = value as string;
            if (string.IsNullOrEmpty(sValue))
            {
                if (_dictionary.ContainsKey("Null"))
                    _dictionary["Null"]++;
                else
                    _dictionary.Add("Null", 1);
                return;
            }

            long valueInt64;
            if (Int64.TryParse(sValue, out valueInt64))
            {
                if (_dictionary.ContainsKey("Int64"))
                    _dictionary["Int64"]++;
                else
                    _dictionary.Add("Int64", 1);
                return;
            }

            double valueDouble;
            if (Double.TryParse(sValue, out valueDouble))
            {
                if (_dictionary.ContainsKey("Double"))
                    _dictionary["Double"]++;
                else
                    _dictionary.Add("Double", 1);
                return;
            }

            float valueFloat;
            if (float.TryParse(sValue, out valueFloat))
            {
                if (_dictionary.ContainsKey("Float"))
                    _dictionary["Float"]++;
                else
                    _dictionary.Add("Float", 1);
                return;
            }

            DateTime valueDateTime;
            if (DateTime.TryParse(sValue, out valueDateTime))
            {
                if (_dictionary.ContainsKey("DateTime"))
                    _dictionary["DateTime"]++;
                else
                    _dictionary.Add("DateTime", 1);
                return;
            }

            if (_dictionary.ContainsKey("String"))
                _dictionary["String"]++;
            else
                _dictionary.Add("String", 1);

        }
        public string BestDataTypeResult(out Dictionary<string, int> distribution)
        {
            string result = "";

            if (_recordCount == 0)
            {
                distribution = null;
                return "N/A";
            }
            else
            {
                int count = _dictionary.Count();
                if (count == 0)
                    result = "N/A";
                if (count == 1 && _dictionary.ContainsKey("DateTime"))
                    result = "DateTime";
                if (count == 1 && _dictionary.ContainsKey("Int64"))
                    result = "Integer";
                if ((count == 1 && _dictionary.ContainsKey("Double")) || (count == 2 && _dictionary.ContainsKey("Double") && _dictionary.ContainsKey("Int64")))
                    result = "Double";
                if (_dictionary.ContainsKey("Float") && _dictionary.ContainsKey("String") == false && _dictionary.ContainsKey("DateTime") == false)
                    result = "Float";
                if (result == "")
                    result = "String";

                if (_detailedResults)
                    distribution = _dictionary;
                else
                    distribution = null;
            }
            return result;
        }

        public void Nulls(string value)
        {
            if (value == null) _intValue++;
            _recordCount++;
        }

        public string NullsResult(out Dictionary<string, int> distribution)
        {
            if (_detailedResults)
            {
                distribution = new Dictionary<string, int>();
                distribution.Add("Nulls", _intValue);
                distribution.Add("Non Nulls", _recordCount - _intValue);
            }
            else
                distribution = null;
            return _recordCount == 0 ? "N/A" : ((float)_intValue / _recordCount).ToString("P");
        }

        public void Blanks(string value)
        {
            if (value==null || value.ToString() == "") _intValue++;
            _recordCount++;
        }

        public string BlanksResult(out Dictionary<string, int> distribution)
        {
            if (_detailedResults)
            {
                distribution = new Dictionary<string, int>();
                distribution.Add("Blanks", _intValue);
                distribution.Add("Non Blanks", _recordCount - _intValue);
            }
            else
                distribution = null;
            return _recordCount == 0 ? "N/A" : ((float)_intValue / _recordCount).ToString("P");
        }

        public void Zeros(string value)
        {
            decimal number;
            if (Decimal.TryParse(value.ToString(), out number))
                if (number == 0) _intValue++;
            _recordCount++;
        }

        public string ZerosResult(out Dictionary<string, int> distribution)
        {
            if (_detailedResults)
            {
                distribution = new Dictionary<string, int>();
                distribution.Add("Zeros", _intValue);
                distribution.Add("Non Zeros", _recordCount - _intValue);
            }
            else
                distribution = null;

            if (_recordCount == 0)
                return "N/A";
            else
                return ((float)_intValue / _recordCount).ToString("P");
        }

        public void MaxLength(string value)
        {
            _recordCount++;
            int length = value.ToString().Length;
            if (value.ToString().Length > _intValue)
            {
                _intValue = value.ToString().Length;
            }
            if(_detailedResults)
            {
                if (_dictionary.ContainsKey(length.ToString()))
                    _dictionary[length.ToString()]++;
                else
                    _dictionary.Add(length.ToString(), 1);
                return;
            }
        }

        public string MaxLengthResult(out Dictionary<string, int> distribution)
        {
            if (_detailedResults)
            {
                distribution = _dictionary;
            }
            else
                distribution = null;

            if (_recordCount == 0)
                return "N/A";
            else
                return _intValue.ToString();
        }

        public void MaxValue(string value)
        {
            double number;
            _recordCount++;

            if (Double.TryParse(value.ToString(), out number))
            {
                if (_objectValue == null || Convert.ToDouble(_objectValue) < number)
                {
                    _objectValue = value;
                }

                if (_detailedResults)
                {
                    if (_dictionary.ContainsKey(_objectValue.ToString()))
                        _dictionary[_objectValue.ToString()]++;
                    else
                        _dictionary.Add(_objectValue.ToString(), 1);
                    return;
                }
            }
            else
            {
                if(_detailedResults)
                {
                    if (_dictionary.ContainsKey("Non Numeric"))
                        _dictionary["Non Numeric"]++;
                    else
                        _dictionary.Add("Non Numeric", 1);
                    return;
                }
            }
        }

        public string MaxValueResult(out Dictionary<string, int> distribution)
        {
            if (_detailedResults)
            {
                distribution = _dictionary;
            }
            else
                distribution = null;

            if (_recordCount == 0 || _objectValue == null)
                return "N/A";
            else
                return _objectValue.ToString();
        }

        public void DistinctValues(string value)
        {
            _recordCount++;

            if (value.GetType().Name == "DBNull" || value == null)
                value = "Null";

            if (_dictionary.ContainsKey(value.ToString()) == false)
                _dictionary.Add(value.ToString(), 1);
            else
                _dictionary[value.ToString()]++;
        }

        public string DistinctValuesResult(out Dictionary<string, int> distribution)
        {
            if (_detailedResults)
            {
                distribution = _dictionary;
            }
            else
                distribution = null;

            if (_recordCount == 0)
                return "N/A";
            else
            {
                if (_dictionary.Count() == _recordCount)
                    return "Unique";
                else
                    return _dictionary.Count().ToString();
            }
        }

        public void Patterns(string value)
        {
            _recordCount++;

            Type type = value.GetType();
            if (type != typeof(DBNull) && type == typeof(String))
            {
                string pattern = value.ToString();
                if (pattern.Length < 50)
                {
                    pattern = Regex.Replace(pattern, "[A-Z]", "A");
                    pattern = Regex.Replace(pattern, "[a-z]", "a");
                    pattern = Regex.Replace(pattern, "[0-9]", "9");

                    if (_dictionary.ContainsKey(pattern))
                        _dictionary[pattern]++;
                    else
                        _dictionary.Add(pattern, 1);
                }
            }
            value = "DBNull";
        }

        public string PatternsResult(out Dictionary<string, int> distribution)
        {
            if (_detailedResults)
            {
                distribution = _dictionary;
            }
            else
                distribution = null;

            if (_recordCount == 0 || !_dictionary.Any())
                return "N/A";
            else
            {
                KeyValuePair<string, int> pattern = _dictionary.OrderByDescending(c => c.Value).First();

                if (_dictionary.Count == 1)
                    return "Unique: " + pattern.Key;
                else
                {
                    return "Pattern Count=" + _dictionary.Count.ToString() + ", Most common(" + ((float)pattern.Value / _recordCount).ToString("P") + "): " + pattern.Key;
                }
            }
        }

    }
}
