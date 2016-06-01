using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks;
using dexih.functions;
using System.Linq;

namespace dexih.transforms
{

    public class ColumnPair
    {
        /// <summary>
        /// Sets the source and target mappings to the same column name
        /// </summary>
        /// <param name="sourceTargetColumn">Column Name</param>
        public ColumnPair(string sourceTargetColumn)
        {
            SourceColumn = sourceTargetColumn;
            TargetColumn = sourceTargetColumn;
        }

        /// <summary>
        /// Sets the source and column mapping.
        /// </summary>
        /// <param name="sourceColumn">Source Column Name</param>
        /// <param name="targetColumn">Target Column Name</param>
        public ColumnPair(string sourceColumn, string targetColumn)
        {
            SourceColumn = sourceColumn;
            TargetColumn = targetColumn;
        }

        public string SourceColumn { get; set; }
        public string TargetColumn { get; set; }
    }

    public class JoinPair
    {
        public string SourceColumn { get; set; }
        public string JoinColumn { get; set; }
        public string JoinValue { get; set; }
    }

    public abstract class Transform : DbDataReader
    {
        protected Transform()
        {
            ColumnPairs = new List<ColumnPair>();
            JoinPairs = new List<JoinPair>();
            Functions = new List<Function>();
            SortFields = new List<Sort>();
            TransformErrors = new List<string>();
            TransformTimer = new Stopwatch();
        }

        #region Properties
        public Transform Reader;
        protected object[] CurrentRow;
        protected Transform JoinReader { get; set; }

        public List<string> TransformErrors { get; set; }

        //Generic transform contains properties for a list of Functions, Fields and simple Mappings 
        public List<Function> Functions { get; set; } //functions used for complex mapping, conditions.
        public string[] Fields { get; set; } //list of fields.  use for group, sort lists.
        public List<ColumnPair> ColumnPairs { get; set; } //fields pairs, used for simple mappings.
        public List<JoinPair> JoinPairs { get; set; } //fields pairs, used for table and service joins.
        public string JoinTable { get; set; } //used to store a reference to a join table.  
        public bool PassThroughColumns { get; set; } //indicates that any non-mapped columns should be mapped to the target.

        public abstract List<Sort> RequiredSortFields();
        public abstract List<Sort> RequiredJoinSortFields();

        public abstract bool PrefersSort { get; } //indicates the transform will run better with sorted input
        public abstract bool RequiresSort { get; } //indicates the transform must have sorted input

        public bool InputIsSorted { get; set; } //indicates if the transform can confirm sorted input.

        public List<Sort> InputSortFields { get; set; }
        public abstract List<Sort> OutputSortFields();

        public List<Sort> SortFields { get; set; } //indicates field for the sort transform.

        //diagnostics to record the processing time for the transformation.
        public Stopwatch TransformTimer { get; set; }
        public Stopwatch ProcessingDataTimer;

        public int RecordCount { get; set; }

        public virtual bool SetInTransform(Transform inTransform, Transform joinTransform = null)
        {

            //if the transform requires a sort and input data it not sorted, then add a sort transform in between.
            if(RequiresSort)
            {
                bool sortMatch = false;
                if (inTransform.InputIsSorted)
                {
                    string requiredSortFields = String.Join(",", RequiredSortFields().Select(c => c.Column).ToArray());
                    string inputSortFields = string.Join(",", inTransform.SortFields.Select(c => c.Column).ToArray());

                    if(requiredSortFields == inputSortFields.Substring(0, requiredSortFields.Length))
                    {
                        sortMatch = true;
                    }
                }

                if(!sortMatch)
                {
                    TransformSort sortTransform = new TransformSort(inTransform, RequiredJoinSortFields());
                    Reader = sortTransform;
                }
                else
                {
                    Reader = inTransform;
                }

                if (joinTransform != null)
                {
                    sortMatch = false;
                    if (joinTransform.InputIsSorted)
                    {
                        string requiredSortFields = String.Join(",", RequiredSortFields().Select(c => c.Column).ToArray());
                        string joinSortFields = string.Join(",", joinTransform.SortFields.Select(c => c.Column).ToArray());

                        if (requiredSortFields == joinSortFields.Substring(0, requiredSortFields.Length))
                        {
                            sortMatch = true;
                        }
                    }

                    if (!sortMatch)
                    {
                        TransformSort sortTransform = new TransformSort(joinTransform, RequiredJoinSortFields());
                        JoinReader = sortTransform;
                    }
                    else
                    {
                        Reader = inTransform;
                    }
                }
            }

            Initialize();
            ResetValues();
            return true;
        }

        /// <summary>
        /// Indicates if the source connection can sort data.
        /// </summary>
        public abstract bool CanRunQueries { get; }

        public abstract bool ResetValues();
        public abstract bool Initialize();
        public abstract string Details();

        #endregion

        #region IDataRecord Implementation

        protected abstract bool ReadRecord();

        public override bool Read()
        {
            TransformTimer.Start();
            bool returnValue = ReadRecord();
            if (returnValue) RecordCount++;
            TransformTimer.Stop();
            return returnValue;
        }

        /// <summary>
        /// This is a recursive function that goes through each of the transforms and returns timer values when it gets to a connection.
        /// </summary>
        /// <param name="recordsRead"></param>
        /// <param name="elapsedMilliseconds"></param>
        /// <returns></returns>
        public virtual void ReadThroughput(ref int recordsRead, ref long elapsedMilliseconds)
        {
            Reader?.ReadThroughput(ref recordsRead, ref elapsedMilliseconds);

            Reader.JoinReader?.ReadThroughput(ref recordsRead, ref elapsedMilliseconds);
        }

        public abstract Task<ReturnValue> LookupRow(List<Filter> filters);

        public abstract override int FieldCount { get; }
        //public abstract override DataTable GetSchemaTable();

        public abstract override int GetOrdinal(string columnName);
        public abstract override string GetName(int i);


        public override object this[string name] => GetValue(GetOrdinal(name));

        public override object this[int i] => GetValue(i);

        public override int Depth
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool IsClosed => Reader.IsClosed;

        public override int RecordsAffected
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool GetBoolean(int i)
        {
            return Convert.ToBoolean(GetValue(i));
        }
        public override byte GetByte(int i)
        {
            return Convert.ToByte(GetValue(i));
        }
        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException("GetBytes is not supported.");
        }
        public override char GetChar(int i)
        {
            return Convert.ToChar(GetValue(i));
        }
        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException("GetChars is not supported.");
        }
 
        public override string GetDataTypeName(int i)
        {
            return GetValue(i).GetType().Name;
        }
        public override DateTime GetDateTime(int i)
        {
            return Convert.ToDateTime(GetValue(i));
        }
        public override decimal GetDecimal(int i)
        {
            return Convert.ToDecimal(GetValue(i));
        }
        public override double GetDouble(int i)
        {
            return Convert.ToDouble(GetValue(i));
        }
        public override Type GetFieldType(int i)
        {
            return GetValue(i).GetType();
        }
        public override float GetFloat(int i)
        {
            return Convert.ToSingle(GetValue(i));
        }
        public override Guid GetGuid(int i)
        {
            return (Guid)GetValue(i);
        }
        public override short GetInt16(int i)
        {
            return Convert.ToInt16(GetValue(i));
        }
        public override int GetInt32(int i)
        {
            return Convert.ToInt32(GetValue(i));
        }
        public override long GetInt64(int i)
        {
            return Convert.ToInt64(GetValue(i));
        }

        public override string GetString(int i)
        {
            return GetValue(i).ToString();
        }
        public override object GetValue(int i)
        {

            return CurrentRow[i];
        }
        public override int GetValues(object[] values)
        {
            for (int i = 0; i < values.GetLength(0); i++)
                values[i] = CurrentRow[i];
            return values.GetLength(0);
        }
        public override bool IsDBNull(int i)
        {
            return GetValue(i) is DBNull;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Reader.Dispose(disposing);
            }
            base.Dispose(disposing);
        }

        public override bool NextResult()
        {
            return Reader.NextResult();
        }

        public override IEnumerator GetEnumerator()
        {
            return Reader?.GetEnumerator();
        }

        public override bool HasRows => Reader.HasRows;

        #endregion

    }
}
