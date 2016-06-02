using System;
using System.Collections;
using System.Data.Common;

namespace dexih.transforms
{
    public class DataRowReader : DbDataReader
    {
        string[] _fields;
        object[] _row;

        #region Constructors
        public DataRowReader(string[] fields, object[] row)
        {
            _row = row;
            _fields = fields;
        }
        #endregion
        #region IDataRecord Implementation
        public override object this[string name] => _row[Array.IndexOf(_fields, name)];

        public override object this[int i] => _row[i];


        public override int FieldCount => _fields.Length;

        public override int Depth
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool IsClosed
        {
            get
            {
                throw new NotImplementedException();
            }
        }

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

        public override bool GetBoolean(int i)
        {
            return Convert.ToBoolean(_row[i]);
        }
        public override byte GetByte(int i)
        {
            return Convert.ToByte(_row[i]);
        }
        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException("GetBytes is not supported.");
        }
        public override char GetChar(int i)
        {
            return Convert.ToChar(_row[i]);
        }
        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException("GetChars is not supported.");
        }

        public override decimal GetDecimal(int i)
        {
            return Convert.ToDecimal(_row[i]);
        }
        public override double GetDouble(int i)
        {
            return Convert.ToDouble(_row[i]);
        }
        public override Type GetFieldType(int i)
        {
            return _row[i].GetType();
        }

        public override short GetInt16(int i)
        {
            return Convert.ToInt16(_row[i]);
        }
        public override int GetInt32(int i)
        {
            return Convert.ToInt32(_row[i]);
        }
        public override long GetInt64(int i)
        {
            return Convert.ToInt64(_row[i]);
        }
        public override string GetName(int i)
        {
            return _fields[i];
        }
        public override int GetOrdinal(string name)
        {
            return Array.IndexOf(_fields, name);
        }
        public override string GetString(int i)
        {
            return _row[i].ToString();
        }
        public override object GetValue(int i)
        {
            return _row[i];
        }
        public override int GetValues(object[] values)
        {
            _row.CopyTo(values, 0);
            return _fields.Length;
        }
        public override bool IsDBNull(int i)
        {
            return _row[i] is DBNull;
        }

        public override bool NextResult()
        {
            return false;
        }

        public override bool Read()
        {
            return false;
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return Convert.ToDateTime(_row[ordinal]);
        }

        public override float GetFloat(int ordinal)
        {
            return Convert.ToSingle(_row[ordinal]);
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
