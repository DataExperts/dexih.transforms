using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using dexih.functions;

namespace dexih.functions.BuiltIn
{
    public class ProfileFunctions
    {
        private readonly Dictionary<string, int> _dictionary = new Dictionary<string, int>();
        private int _intValue;
        private object _objectValue;

        private int _recordCount;
        
        [TransformFunctionDetailedFlag]
        public bool DetailedResults { get; set; }
        
        public void Reset()
        {
            _intValue = 0;
            _objectValue = null;
            _dictionary.Clear();
            _recordCount = 0;
        }

        public ProfileFunctions(bool detailedResults = false)
        {
            DetailedResults = detailedResults;
        }

        public ProfileFunctions()
        {
            DetailedResults = false;
        }

        [TransformFunction(FunctionType = EFunctionType.Profile, Category = "Profile", Name = "Best Datatype", Description = "Find the best datatype for a column.", ResultMethod = nameof(BestDataTypeResult), ResetMethod = nameof(Reset))]
        public void BestDataType(string value)
        {
            _recordCount++;

            //only makes recommendations on string types, otherwise uses the type of the field.
            var sValue = value;
            if (string.IsNullOrEmpty(sValue))
            {
                if (_dictionary.ContainsKey("Null"))
                    _dictionary["Null"]++;
                else
                    _dictionary.Add("Null", 1);
                return;
            }

            long valueInt64;
            if (long.TryParse(sValue, out valueInt64))
            {
                if (_dictionary.ContainsKey("Int64"))
                    _dictionary["Int64"]++;
                else
                    _dictionary.Add("Int64", 1);
                return;
            }

            double valueDouble;
            if (double.TryParse(sValue, out valueDouble))
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
            var result = "";

            if (_recordCount == 0)
            {
                distribution = null;
                return "N/A";
            }
            var count = _dictionary.Count();
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

            if (DetailedResults)
                distribution = _dictionary;
            else
                distribution = null;
            return result;
        }

        [TransformFunction(FunctionType = EFunctionType.Profile, Category = "Profile", Name = "Nulls", Description = "Find the percentage of null columns.", ResultMethod = nameof(NullsResult), ResetMethod = nameof(Reset))]
        public void Nulls(string value)
        {
            if (value == null) _intValue++;
            _recordCount++;
        }

        public string NullsResult(out Dictionary<string, int> distribution)
        {
            if (DetailedResults)
            {
                distribution = new Dictionary<string, int>();
                distribution.Add("Nulls", _intValue);
                distribution.Add("Non Nulls", _recordCount - _intValue);
            }
            else
                distribution = null;
            return _recordCount == 0 ? "N/A" : ((float)_intValue / _recordCount).ToString("P");
        }

        [TransformFunction(FunctionType = EFunctionType.Profile, Category = "Profile", Name = "Blanks", Description = "Find the percentage of blank columns.", ResultMethod = nameof(BlanksResult), ResetMethod = nameof(Reset))]
        public void Blanks(string value)
        {
            if (value==null || value == "") _intValue++;
            _recordCount++;
        }

        public string BlanksResult(out Dictionary<string, int> distribution)
        {
            if (DetailedResults)
            {
                distribution = new Dictionary<string, int>();
                distribution.Add("Blanks", _intValue);
                distribution.Add("Non Blanks", _recordCount - _intValue);
            }
            else
                distribution = null;
            return _recordCount == 0 ? "N/A" : ((float)_intValue / _recordCount).ToString("P");
        }

        [TransformFunction(FunctionType = EFunctionType.Profile, Category = "Profile", Name = "Zeros", Description = "Find the percentage of zero value columns.", ResultMethod = nameof(ZerosResult), ResetMethod = nameof(Reset))]
        public void Zeros(string value)
        {
            decimal number;
            if (value != null && decimal.TryParse(value, out number))
                if (number == 0) _intValue++;
            _recordCount++;
        }

        public string ZerosResult(out Dictionary<string, int> distribution)
        {
            if (DetailedResults)
            {
                distribution = new Dictionary<string, int>();
                distribution.Add("Zeros", _intValue);
                distribution.Add("Non Zeros", _recordCount - _intValue);
            }
            else
                distribution = null;

            if (_recordCount == 0)
                return "N/A";
            return ((float)_intValue / _recordCount).ToString("P");
        }

        [TransformFunction(FunctionType = EFunctionType.Profile, Category = "Profile", Name = "Maximum Length", Description = "Find the maximum string length.", ResultMethod = nameof(MaxLengthResult), ResetMethod = nameof(Reset))]
        public void MaxLength(string value)
        {
            _recordCount++;
            var length = value == null ? 0 : value.Length;
            if (length > _intValue)
            {
                _intValue = length;
            }
            if(DetailedResults)
            {
                if (_dictionary.ContainsKey(length.ToString()))
                    _dictionary[length.ToString()]++;
                else
                    _dictionary.Add(length.ToString(), 1);
            }
        }

        public string MaxLengthResult(out Dictionary<string, int> distribution)
        {
            if (DetailedResults)
            {
                distribution = _dictionary;
            }
            else
                distribution = null;

            if (_recordCount == 0)
                return "N/A";
            return _intValue.ToString();
        }

        [TransformFunction(FunctionType = EFunctionType.Profile, Category = "Profile", Name = "Maximum Value", Description = "Find the maximum numerical value.", ResultMethod = nameof(MaxValueResult), ResetMethod = nameof(Reset))]
        public void MaxValue(string value)
        {
            double number;
            _recordCount++;

            if (value != null &&  double.TryParse(value, out number))
            {
                if (_objectValue == null || Convert.ToDouble(_objectValue) < number)
                {
                    _objectValue = value;
                }

                if (DetailedResults)
                {
                    if (_dictionary.ContainsKey(_objectValue.ToString()))
                        _dictionary[_objectValue.ToString()]++;
                    else
                        _dictionary.Add(_objectValue.ToString(), 1);
                }
            }
            else
            {
                if(DetailedResults)
                {
                    if (_dictionary.ContainsKey("Non Numeric"))
                        _dictionary["Non Numeric"]++;
                    else
                        _dictionary.Add("Non Numeric", 1);
                }
            }
        }

        public string MaxValueResult(out Dictionary<string, int> distribution)
        {
            if (DetailedResults)
            {
                distribution = _dictionary;
            }
            else
                distribution = null;

            if (_recordCount == 0 || _objectValue == null)
                return "N/A";
            return _objectValue.ToString();
        }

        [TransformFunction(FunctionType = EFunctionType.Profile, Category = "Profile", Name = "Distinct Values", Description = "Find number of distinct values.", ResultMethod = nameof(DistinctValuesResult), ResetMethod = nameof(Reset))]
        public void DistinctValues(string value)
        {
            _recordCount++;

            if (value == null )
                value = "Null";

            if (_dictionary.ContainsKey(value) == false)
                _dictionary.Add(value, 1);
            else
                _dictionary[value]++;
        }

        public string DistinctValuesResult(out Dictionary<string, int> distribution)
        {
            if (DetailedResults)
            {
                distribution = _dictionary;
            }
            else
                distribution = null;

            if (_recordCount == 0)
                return "N/A";
            if (_dictionary.Count() == _recordCount)
                return "Unique";
            return _dictionary.Count().ToString();
        }

        [TransformFunction(FunctionType = EFunctionType.Profile, Category = "Profile", Name = "Patterns", Description = "Find different types of string/numerical patterns.", ResultMethod = nameof(PatternsResult), ResetMethod = nameof(Reset))]
        public void Patterns(string value)
        {
            _recordCount++;

            if (value != null)
            {
                var pattern = value;
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
            value = "Null";
        }

        public string PatternsResult(out Dictionary<string, int> distribution)
        {
            if (DetailedResults)
            {
                distribution = _dictionary;
            }
            else
                distribution = null;

            if (_recordCount == 0 || !_dictionary.Any())
                return "N/A";
            var pattern = _dictionary.OrderByDescending(c => c.Value).First();

            if (_dictionary.Count == 1)
                return "Unique: " + pattern.Key;
            return "Pattern Count=" + _dictionary.Count + ", Most common(" + ((float)pattern.Value / _recordCount).ToString("P") + "): " + pattern.Key;
        }
    }
}