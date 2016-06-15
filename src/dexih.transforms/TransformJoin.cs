using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{

    /// <summary>
    /// The join table is loaded into memory and then joined to the primary table.
    /// </summary>
    public class TransformJoin : Transform
    {
        public TransformJoin() { }

        public TransformJoin(Transform primaryTransform, Transform joinTransform, List<JoinPair> joinPairs)
        {
            JoinPairs = joinPairs;
            SetInTransform(primaryTransform, joinTransform);
        }

        bool _firstRead;

        SortedDictionary<object[], object[]> _joinData;

        bool _joinReaderOpen;

        public override bool InitializeOutputFields()
        {
            if (ReferenceTransform == null || JoinPairs == null || JoinPairs.Count == 0)
                throw new Exception("There must be a join reader and at least one join pair specified");

            CacheTable = new Table("Join");

            int pos = 0;
            foreach(var column in PrimaryTransform.CacheTable.Columns)
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

            _firstRead = true;

            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return true;
        }

        public override bool RequiresSort => false;

        public override async Task<ReturnValue> Open(SelectQuery query)
        {
            if (query == null)
                query = new SelectQuery();

            query.Sorts = RequiredSortFields();

            var returnValue = await PrimaryTransform.Open(query);
            return returnValue;
        }

        protected override ReturnValue<object[]> ReadRecord()
        {
            object[] newRow = null;

            if (PrimaryTransform.Read() == false)
            {
                return new ReturnValue<object[]>(false, null);
            }
            //if input is sorted, then run a sortedjoin
            if (PrimaryTransform.CacheTable.OutputSortFields != null && ReferenceTransform.CacheTable.OutputSortFields != null)
            {
                //first read get a row from the join table.
                if (_firstRead)
                {
                    _joinReaderOpen = ReferenceTransform.Read();
                    _firstRead = false;
                }

                //loop through join table until we find a matching row.
                while (_joinReaderOpen)
                {
                    bool recordMatch = true;
                    foreach (JoinPair join in JoinPairs)
                    {
                        var joinValue = join.SourceColumn == null ? join.JoinValue : PrimaryTransform[join.SourceColumn].ToString();
                        if (joinValue != ReferenceTransform[join.JoinColumn].ToString())
                        {
                            recordMatch = false;
                            break;
                        }
                    }

                    if (recordMatch == false)
                    {
                        _joinReaderOpen = ReferenceTransform.Read();
                    }
                    else
                    {
                        break;
                    }
                }
                newRow = new object[FieldCount];
                int pos = 0;
                for (int i = 0; i < PrimaryTransform.FieldCount; i++)
                {
                    newRow[pos] = PrimaryTransform[i];
                    pos++;
                }
                for (int i = 0; i < ReferenceTransform.FieldCount; i++)
                {
                    if (_joinReaderOpen)
                    {
                        newRow[pos] = ReferenceTransform[i];
                        pos++;
                    }
                    else
                    {
                        newRow[pos] = DBNull.Value;
                        pos++;
                    }
                }
                return new ReturnValue<object[]>(true, newRow);
            }
            else //if input is not sorted, then run a hash join.
            {
                //first read get a row from the join table.
                if (_firstRead)
                {
                    _joinData = new SortedDictionary<object[], object[]>(new JoinKeyComparer());

                    //load the join data into an in memory list
                    while (ReferenceTransform.Read())
                    {
                        object[] values = new object[ReferenceTransform.FieldCount];
                        object[] joinFields = new object[JoinPairs.Count];

                        ReferenceTransform.GetValues(values);
                        for (int i = 0; i < JoinPairs.Count; i++)
                        {
                            if (JoinPairs[i].JoinColumn == null) continue;

                            joinFields[i] = ReferenceTransform[JoinPairs[i].JoinColumn];
                        }
                        _joinData.Add(joinFields, values);
                    }

                    _firstRead = false;
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
                object[] sourceKeys = new object[JoinPairs.Count];
                for (int i = 0; i < JoinPairs.Count; i++)
                {
                    if (JoinPairs[i].SourceColumn != "")
                        sourceKeys[i] = PrimaryTransform[JoinPairs[i].SourceColumn];
                    else
                        sourceKeys[i] = JoinPairs[i].JoinValue;
                }

                if (_joinData.Keys.Contains(sourceKeys))
                {
                    object[] joinRow = _joinData[sourceKeys];
                    for (int i = 0; i < joinRow.Length; i++)
                    {
                        newRow[pos] = joinRow[i];
                        pos++;
                    }
                }
                return new ReturnValue<object[]>(true, newRow);
            }
        }

        public class JoinKeyComparer : IComparer<object[]>
        {
            public int Compare(object[] x, object[] y)
            {
                for (int i = 0; i < x.Length; i++)
                {
                    if (object.Equals(x[i], y[i])) continue;

                    bool greater = false;

                    if (x[i] is byte)
                        greater = (byte)x[i] > (byte)y[i];
                    if (x[i] is SByte)
                        greater = (SByte)x[i] > (SByte)y[i];
                    if (x[i] is UInt16)
                        greater = (UInt16)x[i] > (UInt16)y[i];
                    if (x[i] is UInt32)
                        greater = (UInt32)x[i] > (UInt32)y[i];
                    if (x[i] is UInt64)
                        greater = (UInt64)x[i] > (UInt64)y[i];
                    if (x[i] is Int16)
                        greater = (Int16)x[i] > (Int16)y[i];
                    if (x[i] is Int32)
                        greater = (Int32)x[i] > (Int32)y[i];
                    if (x[i] is Int64)
                        greater = (Int64)x[i] > (Int64)y[i];
                    if (x[i] is Decimal)
                        greater = (Decimal)x[i] > (Decimal)y[i];
                    if (x[i] is Double)
                        greater = (Double)x[i] > (Double)y[i];
                    if (x[i] is String)
                        greater = String.Compare((String)x[i], (String)y[i]) > 0;
                    if (x[i] is Boolean)
                        greater = (Boolean)x[i] == false && (Boolean)y[i] == true;
                    if (x[i] is DateTime)
                        greater = (DateTime)x[i] > (DateTime)y[i];

                    if (greater)
                        return 1;
                    else
                        return -1;
                }
                return 0;
            }
        }

        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true); 
        }

        public override string Details()
        {
            return "Join";
        }

        public override List<Sort> RequiredSortFields()
        {
            List<Sort> fields = new List<Sort>();
            foreach (JoinPair joinPair in JoinPairs.Where(c=>c.SourceColumn != ""))
                fields.Add(new Sort { Column = joinPair.SourceColumn, Direction = Sort.EDirection.Ascending });

            return fields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            List<Sort> fields = new List<Sort>();
            foreach (JoinPair joinPair in JoinPairs.Where(c=>c.SourceColumn != null))
                fields.Add(new Sort { Column = joinPair.JoinColumn, Direction = Sort.EDirection.Ascending });

            return fields;
        }


    }


}
