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
        bool _firstRead;

        SortedDictionary<object[], object[]> _joinData;

        bool _joinReaderOpen;

        public override bool Initialize()
        {
            if (JoinReader == null || JoinPairs == null || JoinPairs.Count == 0)
                throw new Exception("There must be a join reader and at least one join pair specified");

            CachedTable = new Table("Join");

            int pos = 0;
            foreach(var column in Reader.CachedTable.Columns)
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

            _firstRead = true;

            CachedTable.OutputSortFields = Reader.CachedTable.OutputSortFields;

            return true;
        }



        public bool SetJoins(string joinTable, List<JoinPair> joinPairs)
        {
            JoinTable = joinTable;
            JoinPairs = joinPairs;
            return true;
            //return Initialize();
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
            //if input is sorted, then run a sortedjoin
            if (Reader.CachedTable.OutputSortFields != null && JoinReader.CachedTable.OutputSortFields != null)
            {
                //first read get a row from the join table.
                if (_firstRead)
                {
                    _joinReaderOpen = JoinReader.Read();
                    _firstRead = false;
                }

                //loop through join table until we find a matching row.
                while (_joinReaderOpen)
                {
                    bool recordMatch = true;
                    foreach (JoinPair join in JoinPairs)
                    {
                        var joinValue = join.SourceColumn == null ? join.JoinValue : Reader[join.SourceColumn].ToString();
                        if (joinValue != JoinReader[join.JoinColumn].ToString())
                        {
                            recordMatch = false;
                            break;
                        }
                    }

                    if (recordMatch == false)
                    {
                        _joinReaderOpen = JoinReader.Read();
                    }
                    else
                    {
                        break;
                    }
                }
                CurrentRow = new object[FieldCount];
                int pos = 0;
                for (int i = 0; i < Reader.FieldCount; i++)
                {
                    CurrentRow[pos] = Reader[i];
                    pos++;
                }
                for (int i = 0; i < JoinReader.FieldCount; i++)
                {
                    if (_joinReaderOpen)
                    {
                        CurrentRow[pos] = JoinReader[i];
                        pos++;
                    }
                    else
                    {
                        CurrentRow[pos] = DBNull.Value;
                        pos++;
                    }
                }
                return true;
            }
            else //if input is not sorted, then run a hash join.
            {
                //first read get a row from the join table.
                if (_firstRead)
                {
                    _joinData = new SortedDictionary<object[], object[]>(new JoinKeyComparer());

                    //load the join data into an in memory list
                    while (JoinReader.Read())
                    {
                        object[] values = new object[JoinReader.FieldCount];
                        object[] joinFields = new object[JoinPairs.Count];

                        JoinReader.GetValues(values);
                        for (int i = 0; i < JoinPairs.Count; i++)
                        {
                            if (JoinPairs[i].JoinColumn == null) continue;

                            joinFields[i] = JoinReader[JoinPairs[i].JoinColumn];
                        }
                        _joinData.Add(joinFields, values);
                    }

                    _firstRead = false;
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
                object[] sourceKeys = new object[JoinPairs.Count];
                for (int i = 0; i < JoinPairs.Count; i++)
                {
                    if (JoinPairs[i].SourceColumn != "")
                        sourceKeys[i] = Reader[JoinPairs[i].SourceColumn];
                    else
                        sourceKeys[i] = JoinPairs[i].JoinValue;
                }

                if (_joinData.Keys.Contains(sourceKeys))
                {
                    object[] joinRow = _joinData[sourceKeys];
                    for (int i = 0; i < joinRow.Length; i++)
                    {
                        CurrentRow[pos] = joinRow[i];
                        pos++;
                    }
                }
                return true;
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
            return "Join Table:" + JoinTable;
        }

        public override List<Sort> RequiredSortFields()
        {
            List<Sort> fields = new List<Sort>();
            foreach (JoinPair joinPair in JoinPairs.Where(c=>c.SourceColumn != ""))
                fields.Add(new Sort { Column = joinPair.SourceColumn, Direction = Sort.EDirection.Ascending });

            return fields;
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            List<Sort> fields = new List<Sort>();
            foreach (JoinPair joinPair in JoinPairs.Where(c=>c.SourceColumn != null))
                fields.Add(new Sort { Column = joinPair.JoinColumn, Direction = Sort.EDirection.Ascending });

            return fields;
        }


    }


}
