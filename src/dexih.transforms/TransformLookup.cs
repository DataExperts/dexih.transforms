using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using static dexih.functions.DataType;

namespace dexih.transforms
{

    /// <summary>
    /// The join table is loaded into memory and then joined to the primary table.
    /// </summary>
    public class TransformLookup : Transform
    {
        readonly Dictionary<int, string> _fieldNames = new Dictionary<int, string>();
        readonly Dictionary<string, int> _fieldOrdinals = new Dictionary<string, int>();
        int _fieldCount;

        public override bool Initialize()
        {
            if (JoinReader == null || JoinPairs == null || !JoinPairs.Any())
                throw new Exception("There must be a join reader and at least one join pair specified");

            int pos = 0;
            for (int i = 0; i < Reader.FieldCount; i++)
            {
                _fieldNames.Add(pos, Reader.GetName(i));
                _fieldOrdinals.Add(Reader.GetName(i), pos);
                pos++;
            }
            for (int i = 0; i < JoinReader.FieldCount; i++)
            {
                string columnName = JoinReader.GetName(i);
                if (_fieldOrdinals.ContainsKey(columnName))
                {
                    int append = 1;
                    while (_fieldOrdinals.ContainsKey(columnName + append) ) //where columns are same in source/target add a "1" to the target.
                        append++;
                    columnName = columnName + append;
                }
                _fieldNames.Add(pos, columnName);
                _fieldOrdinals.Add(columnName, pos);
                pos++;
            }

            Fields = _fieldNames.Select(c => c.Value).ToArray();

            _fieldCount = pos;

            return true;
        }

        public bool SetJoins(string joinTable, List<JoinPair> joinPairs)
        {
            JoinTable = joinTable;
            JoinPairs = joinPairs;
            return true;
        }

        public override int FieldCount => _fieldCount;

        /// <summary>
        /// checks if filter can execute against the database query.
        /// </summary>
        public override bool CanRunQueries => false;

        public override bool PrefersSort => true;
        public override bool RequiresSort => false;


        public override string GetName(int i)
        {
            return _fieldNames[i];
        }

        public override int GetOrdinal(string columnName)
        {
            if (_fieldOrdinals.ContainsKey(columnName))
                return _fieldOrdinals[columnName];
            return -1;
        }

        protected override bool ReadRecord()
        {
            if (Reader.Read() == false)
            {
                CurrentRow = null;
                return false;
            }
            //load in the primary table values
            CurrentRow = new object[_fieldCount];
            int pos = 0;
            for (int i = 0; i < Reader.FieldCount; i++)
            {
                CurrentRow[pos] = Reader[i];
                pos++;
            }

            //set the values for the lookup
            List<Filter> filters = new List<Filter>();
            for (int i = 0; i < JoinPairs.Count; i++)
            {
                var value = JoinPairs[i].SourceColumn != "" ? Reader[JoinPairs[i].SourceColumn].ToString() : JoinPairs[i].JoinValue;

                filters.Add(new Filter
                {
                    Column1 = JoinPairs[i].JoinColumn,
                    CompareDataType = ETypeCode.String,
                    Operator = Filter.ECompare.EqualTo,
                    Value2 = value
                });
            }

            var lookup = JoinReader.LookupRow(filters).Result;
            if (lookup.Success)
            {
                for (int i = 0; i < JoinReader.FieldCount; i++)
                {
                    CurrentRow[pos] = JoinReader.GetValue(i);
                    pos++;
                }
            }

            return true;
        }

        public override bool ResetValues()
        {
            return true; 
        }

        public override string Details()
        {
            return "Lookup Service:" + JoinTable;
        }

        public override List<Sort> RequiredSortFields()
        {
            List<Sort> fields = new List<Sort>();
            return fields;
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            return null;
        }

        //join will preserve the sort of the input table.
        public override List<Sort> OutputSortFields()
        {
            return InputSortFields;
        }

        public override Task<ReturnValue> LookupRow(List<Filter> filters)
        {
            throw new NotImplementedException();
        }
    }
}
