using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace dexih.transforms
{
    /// <summary>
    /// A source transform that uses a prepopulated Table as an input.
    /// </summary>
    public class SourceTable : Transform
    {
        int position;
        object[] currentRecord;
        int recordCount;

        public override ECacheMethod CacheMethod
        {
            get
            {
                return ECacheMethod.PreLoadCache;
            }
            set
            {
                throw new Exception("Cache method is always PreLoadCache in the DataTable adapater and cannot be set.");
            }
        } 

        #region Constructors
        public SourceTable(Table dataTable,  List<Sort> sortFields = null)
        {
            CachedData = dataTable;
            Fields = CachedData.Columns.Select(c => c.ColumnName).ToArray();
            SortFields = sortFields;
            ResetValues();
        }

        public void Add(object[] values)
        {
            CachedData.Data.Add(values);
        }

        #endregion

        #region IDataRecord Implementation
        public override object this[string name] => currentRecord[GetOrdinal(name)];

        public override object this[int i] => currentRecord[i];

        public override int FieldCount => CachedData.Columns.Count;

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
            return CachedData.Columns[i].ColumnName;
        }
        public override int GetOrdinal(string columnName)
        {
            return CachedData.GetOrdinal(columnName);
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
            return CachedData.Columns.Count();
        }
        public override bool IsDBNull(int i)
        {
            return currentRecord[i] is DBNull;
        }

        public override bool NextResult()
        {
            position++;
            if (position < recordCount)
            {
                currentRecord = CachedData.Data[position];
                return true;
            }
            else
                return false;
        }

        public override bool ResetValues()
        {
            //_iterator = DataTable.Data.GetEnumerator();
            recordCount = CachedData.Data.Count();
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
            return SortFields;
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


        #endregion
    }
}
