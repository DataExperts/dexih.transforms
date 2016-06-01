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
        readonly Dictionary<int, string> _fieldNames = new Dictionary<int, string>();
        readonly Dictionary<string, int> _fieldOrdinals = new Dictionary<string, int>();
        int _fieldCount;
        bool _firstRead;

        SortedDictionary<object[], object[]> _joinData;

        bool _joinReaderOpen;

        public override bool Initialize()
        {
            if (JoinReader == null || JoinPairs == null || JoinPairs.Count == 0)
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
            _firstRead = true;

            return true;
        }



        public bool SetJoins(string joinTable, List<JoinPair> joinPairs)
        {
            JoinTable = joinTable;
            JoinPairs = joinPairs;
            return true;
            //return Initialize();
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


     //   public override DataTable GetSchemaTable()
     //   {
     //       DataTable schema = new DataTable("SchemaTable")
     //       {
     //           Locale = CultureInfo.InvariantCulture,
     //           MinimumCapacity = _fieldCount
     //       };

     //       schema.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.BaseColumnName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.BaseSchemaName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.BaseTableName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ColumnName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.DataType, typeof(object)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsAliased, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsExpression, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsKey, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsLong, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(short)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.NumericScale, typeof(short)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ProviderType, typeof(int)).ReadOnly = true;

     //       schema.Columns.Add(SchemaTableOptionalColumn.BaseCatalogName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.BaseServerName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsHidden, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsRowVersion, typeof(bool)).ReadOnly = true;

     //       // null marks columns that will change for each row
     //       object[] schemaRow = {
     //               true,					// 00- AllowDBNull
					//null,					// 01- BaseColumnName
					//string.Empty,			// 02- BaseSchemaName
					//string.Empty,			// 03- BaseTableName
					//null,					// 04- ColumnName
					//null,					// 05- ColumnOrdinal
					//int.MaxValue,			// 06- ColumnSize
					//typeof(string),			// 07- DataType
					//false,					// 08- IsAliased
					//false,					// 09- IsExpression
					//false,					// 10- IsKey
					//false,					// 11- IsLong
					//false,					// 12- IsUnique
					//DBNull.Value,			// 13- NumericPrecision
					//DBNull.Value,			// 14- NumericScale
					//(int) DbType.String,	// 15- ProviderType

					//string.Empty,			// 16- BaseCatalogName
					//string.Empty,			// 17- BaseServerName
					//false,					// 18- IsAutoIncrement
					//false,					// 19- IsHidden
					//true,					// 20- IsReadOnly
					//false					// 21- IsRowVersion
			  //};

     //       DataTable schemaTable = Reader.GetSchemaTable();
     //       if (schemaTable == null)
     //           return null;

     //       Dictionary<string, int> newFieldOrdinals = new Dictionary<string, int>();

     //       int pos = 0;
     //       for (int i = 0; i < Reader.FieldCount; i++)
     //       {
     //           DataRow row = schemaTable.Select("ColumnName='" + Reader.GetName(i) + "'")[0];
     //           schemaRow[1] = Reader.GetName(i); // Base column name
     //           schemaRow[4] = Reader.GetName(i); // Column name
     //           schemaRow[5] = pos; // Column ordinal
     //           schemaRow[7] = row["DataType"];
     //           schema.Rows.Add(schemaRow);
     //           newFieldOrdinals.Add(Reader.GetName(i), pos);
     //           pos++;
     //       }

     //       DataTable joinSchemaTable = JoinReader.GetSchemaTable();

     //       for (int i = 0; i < JoinReader.FieldCount; i++)
     //       {
     //           string columnName = JoinReader.GetName(i);
     //           DataRow row = joinSchemaTable.Select("ColumnName='" + columnName + "'")[0];

     //           if (newFieldOrdinals.ContainsKey(columnName))
     //           {
     //               int append = 1;
     //               while (newFieldOrdinals.ContainsKey(columnName + append)) //where columns are same in source/target add a "1" to the target.
     //                   append++;
     //               columnName = columnName + append;
     //           }
     //           schemaRow[1] = columnName; // Base column name
     //           schemaRow[4] = columnName; // Column name
     //           schemaRow[5] = pos; // Column ordinal
     //           schemaRow[7] = row["DataType"];
     //           schema.Rows.Add(schemaRow);
     //           newFieldOrdinals.Add(columnName, pos);
     //           pos++;
     //       }

     //       return schema;
     //   }

        protected override bool ReadRecord()
        {
            if (Reader.Read() == false)
            {
                CurrentRow = null;
                return false;
            }
            //if input is sorted, then run a sortedjoin
            if (InputIsSorted)
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
                CurrentRow = new object[_fieldCount];
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
                CurrentRow = new object[_fieldCount];
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

        public override bool ResetValues()
        {
            return true; 
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
