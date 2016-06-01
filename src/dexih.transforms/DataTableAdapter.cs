using System;
using System.Collections;
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
    /// Turns a datatable into a IDataReader.
    /// </summary>
    public class DataTableAdapter : Transform
    {
        public DataTableSimple DataTable {get;set;}
        
        int position;
        object[] currentRecord;
        int recordCount;
       

        #region Constructors
        public DataTableAdapter(DataTableSimple dataTable)
        {
            DataTable  = dataTable;
            Fields = DataTable.Columns.Select(c => c.ColumnName).ToArray();
            ResetValues();
        }

        public void Add(object[] values)
        {
            DataTable.Data.Add(values);
        }

        #endregion

        #region IDataRecord Implementation
        public override object this[string name] => currentRecord[GetOrdinal(name)];

        public override object this[int i] => currentRecord[i];

        public override int FieldCount => DataTable.Columns.Count;

        public override int Depth
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool IsClosed => currentRecord != null;

        public override int RecordsAffected
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool HasRows
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool PrefersSort
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool RequiresSort
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool CanRunQueries
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool GetBoolean(int i)
        {
            return Convert.ToBoolean(currentRecord[i]);
        }
        public override byte GetByte(int i)
        {
            return Convert.ToByte(currentRecord[i]);
        }
        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException("GetBytes is not supported.");
        }
        public override char GetChar(int i)
        {
            return Convert.ToChar(currentRecord[i]);
        }
        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException("GetChars is not supported.");
        }
        public override string GetDataTypeName(int i)
        {
            return currentRecord[i].GetType().Name;
        }
        public override DateTime GetDateTime(int i)
        {
            return Convert.ToDateTime(currentRecord[i]);
        }
        public override decimal GetDecimal(int i)
        {
            return Convert.ToDecimal(currentRecord[i]);
        }
        public override double GetDouble(int i)
        {
            return Convert.ToDouble(currentRecord[i]);
        }
        public override Type GetFieldType(int i)
        {
            return currentRecord[i].GetType();
        }
        public override float GetFloat(int i)
        {
            return Convert.ToSingle(currentRecord[i]);
        }
        public override Guid GetGuid(int i)
        {
            return (Guid)currentRecord[i];
        }
        public override short GetInt16(int i)
        {
            return Convert.ToInt16(currentRecord[i]);
        }
        public override int GetInt32(int i)
        {
            return Convert.ToInt32(currentRecord[i]);
        }
        public override long GetInt64(int i)
        {
            return Convert.ToInt64(currentRecord[i]);
        }
        public override string GetName(int i)
        {
            return DataTable.Columns[i].ColumnName;
        }
        public override int GetOrdinal(string columnName)
        {
            return DataTable.Columns.GetOrdinal(columnName);
        }
        public override string GetString(int i)
        {
            return currentRecord[i].ToString();
        }
        public override object GetValue(int i)
        {
            return currentRecord[i];
        }
        public override int GetValues(object[] values)
        {
            currentRecord.CopyTo(values, 0);
            return DataTable.Columns.Count();
        }
        public override bool IsDBNull(int i)
        {
            return currentRecord[i] is DBNull;
        }

        //public override void Close()
        //{
        //}

     //   public override DataTable GetSchemaTable()
     //   {
     //       DataTable schema = new DataTable("SchemaTable")
     //       {
     //           Locale = CultureInfo.InvariantCulture,
     //           MinimumCapacity = Table.Columns.Count
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

     //       for (int i = 0; i < Table.Columns.Count; i++)
     //       {
     //           schemaRow[1] = Table.Columns[i].ColumnName; // Base column name
     //           schemaRow[4] = Table.Columns[i].ColumnName; // Column name
     //           schemaRow[5] = i; // Column ordinal
     //           schemaRow[7] = Table.Columns[i].DataType;

     //           schema.Rows.Add(schemaRow);
     //       }

     //       return schema;
     //   }

        public override bool NextResult()
        {
            position++;
            if (position < recordCount)
            {
                currentRecord = DataTable.Data[position];
                return true;
            }
            else
                return false;
        }

        public override bool ResetValues()
        {
            //_iterator = DataTable.Data.GetEnumerator();
            recordCount = DataTable.Data.Count();
            position = -1;
            return true;
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override bool Read()
        {
            return NextResult();
        }

        public override List<Sort> RequiredSortFields()
        {
            throw new NotImplementedException();
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            throw new NotImplementedException();
        }

        public override List<Sort> OutputSortFields()
        {
            throw new NotImplementedException();
        }



        public override bool Initialize()
        {
            throw new NotImplementedException();
        }

        public override string Details()
        {
            throw new NotImplementedException();
        }

        protected override bool ReadRecord()
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue> LookupRow(List<Filter> filters)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
