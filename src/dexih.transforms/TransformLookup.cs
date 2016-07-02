using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using static dexih.functions.DataType;
using System.Threading;

namespace dexih.transforms
{

    /// <summary>
    /// The join table is loaded into memory and then joined to the primary table.
    /// </summary>
    public class TransformLookup : Transform
    {
        public TransformLookup() { }

        public TransformLookup(Transform primaryTransform, Transform joinTransform, List<JoinPair> joinPairs)
        {
            JoinPairs = joinPairs;
            SetInTransform(primaryTransform, joinTransform);
        }

        public override bool InitializeOutputFields()
        {
            if (ReferenceTransform == null || JoinPairs == null || !JoinPairs.Any())
                throw new Exception("There must be a join reader and at least one join pair specified");

            CacheTable = new Table("Lookup");

            int pos = 0;
            foreach (var column in PrimaryTransform.CacheTable.Columns)
            {
                CacheTable.Columns.Add(column.Copy());
                pos++;
            }
            foreach (var column in ReferenceTransform.CacheTable.Columns)
            {
                var newColumn = column.Copy();

                //if a column of the same name exists, append a 1 to the name
                if (CacheTable.Columns.SingleOrDefault(c => c.ColumnName == column.ColumnName) != null)
                {
                    int append = 1;
                    while (CacheTable.Columns.SingleOrDefault(c => c.ColumnName == column.ColumnName + append.ToString()) != null) //where columns are same in source/target add a "1" to the target.
                        append++;
                    newColumn.ColumnName = column.ColumnName + append.ToString();
                }
                CacheTable.Columns.Add(newColumn);
                pos++;
            }

            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return true;
        }

        public override bool RequiresSort => false;

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            object[] newRow = null;

            if (await PrimaryTransform.ReadAsync(cancellationToken)== false)
            {
                return new ReturnValue<object[]>(false, null);
            }

            //load in the primary table values
            newRow = new object[FieldCount];
            int pos = 0;
            for (int i = 0; i < PrimaryTransform.FieldCount; i++)
            {
                newRow[pos] = PrimaryTransform[i];
                pos++;
            }

            //set the values for the lookup
            List<Filter> filters = new List<Filter>();
            for (int i = 0; i < JoinPairs.Count; i++)
            {
                var value = JoinPairs[i].SourceColumn != "" ? PrimaryTransform[JoinPairs[i].SourceColumn].ToString() : JoinPairs[i].JoinValue;

                filters.Add(new Filter
                {
                    Column1 = JoinPairs[i].JoinColumn,
                    CompareDataType = ETypeCode.String,
                    Operator = Filter.ECompare.IsEqual,
                    Value2 = value
                });
            }

            var lookup = ReferenceTransform.LookupRow(filters).Result;
            if (lookup.Success)
            {
                for (int i = 0; i < ReferenceTransform.FieldCount; i++)
                {
                    newRow[pos] = ReferenceTransform.GetValue(i);
                    pos++;
                }
            }

            return new ReturnValue<object[]>(true, newRow);
        }

        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true);
        }

        public override string Details()
        {
            return "Lookup Service";
        }

        public override List<Sort> RequiredSortFields()
        {
            List<Sort> fields = new List<Sort>();
            return fields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }
    }
}
