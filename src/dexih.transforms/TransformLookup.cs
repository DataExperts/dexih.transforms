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
        public override bool Initialize()
        {
            if (JoinReader == null || JoinPairs == null || !JoinPairs.Any())
                throw new Exception("There must be a join reader and at least one join pair specified");

            CachedTable = new Table("Lookup");

            int pos = 0;
            foreach (var column in Reader.CachedTable.Columns)
            {
                CachedTable.Columns.Add(column.Copy());
                pos++;
            }
            foreach (var column in JoinReader.CachedTable.Columns)
            {
                var newColumn = column.Copy();

                //if a column of the same name exists, append a 1 to the name
                if (CachedTable.Columns.SingleOrDefault(c => c.ColumnName == column.ColumnName) != null)
                {
                    int append = 1;
                    while (CachedTable.Columns.SingleOrDefault(c => c.ColumnName == column.ColumnName + append.ToString()) != null) //where columns are same in source/target add a "1" to the target.
                        append++;
                    newColumn.ColumnName = column.ColumnName + append.ToString();
                }
                CachedTable.Columns.Add(newColumn);
                pos++;
            }

            CachedTable.OutputSortFields = Reader.CachedTable.OutputSortFields;

            return true;
        }

        public bool SetJoins(string joinTable, List<JoinPair> joinPairs)
        {
            JoinTable = joinTable;
            JoinPairs = joinPairs;
            return true;
        }

        /// <summary>
        /// checks if filter can execute against the database query.
        /// </summary>
        public override bool CanRunQueries => false;

        public override bool PrefersSort => true;
        public override bool RequiresSort => false;

        protected override bool ReadRecord()
        {
            if (Reader.Read() == false)
            {
                CurrentRow = null;
                return false;
            }
            //load in the primary table values
            CurrentRow = new object[FieldCount];
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

        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true);
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
    }
}
