using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks;
using dexih.functions;
using System.Linq;
using System.Globalization;

namespace dexih.transforms
{

    /// <summary>
    /// Transform is the abstract class which all other transforms and connection should implement
    /// </summary>
    public abstract class Transform : DbDataReader
    {
        #region Enums
        public enum ECacheMethod
        {
            NoCache = 0,
            OnDemandCache = 1,
            PreLoadCache  =2
        }
        #endregion

        protected Transform()
        {
            //intialize standard objects.
            ColumnPairs = new List<ColumnPair>();
            JoinPairs = new List<JoinPair>();
            Functions = new List<Function>();
            SortFields = new List<Sort>();
            TransformTimer = new Stopwatch();
        }

        #region Properties

        /// <summary>
        /// The main inbound source of data.
        /// </summary>
        public Transform Reader;

        /// <summary>
        /// The secondary souce of data, if supported by the transform.
        /// </summary>
        public Transform JoinReader { get; set; }

        protected object[] CurrentRow;
        protected int CurrentRowNumber;


        /// <summary>
        /// The method to cache data within the transform.  If NoCache is set, the data can only be read in a forward only method (Lookup and other functions are not available).
        /// </summary>
        public virtual ECacheMethod CacheMethod { get; set; } //indicates the data will be stored in memory.  This allows lookup and other operations to work

        /// <summary>
        /// The maximum rows to cache.  When the maximum rows is exceeded, previous rows will be dropped on a first-in first-out basis.
        /// </summary>
        public virtual Int64 MaxCacheRows { get; set; }
        protected Table CachedData { get; set; }

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
        protected bool SortedInputs { get; set; } //this is set if the transforms sort requirements have been met.

        //public bool InputIsSorted { get; set; } //indicates if the transform can confirm sorted input.
        //public List<Sort> InputSortFields { get; set; }

        /// <summary>
        /// Indicates if the transforms output is sorted.
        /// </summary>
        /// <returns></returns>
        public abstract List<Sort> OutputSortFields();

        public List<Sort> SortFields { get; set; } //indicates fields for the sort transform.

        //diagnostics to record the processing time for the transformation.
        public Stopwatch TransformTimer { get; set; }
        public Stopwatch ProcessingDataTimer;

        public int RecordCount { get; set; }

        public virtual bool SetInTransform(Transform inTransform, Transform joinTransform = null)
        {

            //if the transform requires a sort and input data it not sorted, then add a sort transform.
            if (RequiresSort)
            {
                bool sortMatch = SortFieldsMatch(RequiredSortFields(), inTransform.OutputSortFields());

                if (!sortMatch)
                {
                    TransformSort sortTransform = new TransformSort(inTransform, RequiredSortFields());
                    Reader = sortTransform;
                }
                else
                {
                    Reader = inTransform;
                }

                if (joinTransform != null)
                {
                    sortMatch = SortFieldsMatch(RequiredSortFields(), joinTransform.OutputSortFields());

                    if (!sortMatch)
                    {
                        TransformSort sortTransform = new TransformSort(joinTransform, RequiredJoinSortFields());
                        JoinReader = sortTransform;
                    }
                    else
                    {
                        JoinReader = joinTransform;
                    }
                }

                SortedInputs = true;
            }
            else
            {
                bool sortMatch = SortFieldsMatch(RequiredSortFields(), inTransform.OutputSortFields());

                if (JoinReader != null)
                {
                    sortMatch &= SortFieldsMatch(RequiredSortFields(), joinTransform.OutputSortFields());
                }

                SortedInputs = sortMatch;

                Reader = inTransform;
                JoinReader = joinTransform;
            }
            Initialize();
            ResetValues();
            return true;
        }

        /// <summary>
        /// Indicates if the source connection can run queries (such as sql)
        /// </summary>
        public abstract bool CanRunQueries { get; }
        public abstract bool ResetValues();
        public abstract bool Initialize();
        public abstract string Details();
        protected abstract bool ReadRecord();

        public virtual async Task<ReturnValue<object[]>> LookupRow(List<Filter> filters)
        {
            if(CacheMethod == ECacheMethod.PreLoadCache)
            {
                return CachedData.LookupRow(filters);
            }
            else if(CacheMethod == ECacheMethod.OnDemandCache)
            {
                //read records until a match is found.
                while(Read())
                {
                    var lookupResult = CachedData.LookupRow(filters, RecordCount);
                    if(lookupResult.Success == true)
                        return lookupResult;
                }

                return new ReturnValue<object[]>(false, "Lookup not found.", null);
            }

            return new ReturnValue<object[]>(false, "Lookup can not be performed unless transform caching is set on.", null);
        }

        /// <summary>
        /// This function will confirm that the ActualSort is equivalent to the RequiredSort.
        /// </summary>
        /// <param name="PrimarySort"></param>
        /// <param name="CompareSort"></param>
        /// <returns></returns>
        public bool SortFieldsMatch(List<Sort> RequiredSort, List<Sort> ActualSort)
        {
            if (RequiredSort == null && ActualSort == null)
                return true;

            if (RequiredSort == null || ActualSort == null)
                return false;

            string requiredSortFields = String.Join(",", RequiredSort.Select(c => c.Column).ToArray());
            string actualSortFields = string.Join(",", ActualSort.Select(c => c.Column).ToArray());

            //compare the fields.  if actualsortfields are more, that is ok, as the primary sort condition is still met.
            if (actualSortFields.Length >= requiredSortFields.Length && requiredSortFields == actualSortFields.Substring(0, requiredSortFields.Length))
                return true;
            else
                return false;
        }
        #endregion

        #region DbDataRecord Implementation


        public override bool Read()
        {
            //starts  a timer that can be used to measure downstream transform and database performance.
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

#if NET451
        public override DataTable GetSchemaTable()
        {
            DataTable schema = new DataTable("SchemaTable")
            {
                Locale = CultureInfo.InvariantCulture,
                MinimumCapacity = FieldCount
            };

            schema.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.BaseColumnName, typeof(string)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.BaseSchemaName, typeof(string)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.BaseTableName, typeof(string)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.ColumnName, typeof(string)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.DataType, typeof(object)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.IsAliased, typeof(bool)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.IsExpression, typeof(bool)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.IsKey, typeof(bool)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.IsLong, typeof(bool)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(short)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.NumericScale, typeof(short)).ReadOnly = true;
            schema.Columns.Add(SchemaTableColumn.ProviderType, typeof(int)).ReadOnly = true;

            schema.Columns.Add(SchemaTableOptionalColumn.BaseCatalogName, typeof(string)).ReadOnly = true;
            schema.Columns.Add(SchemaTableOptionalColumn.BaseServerName, typeof(string)).ReadOnly = true;
            schema.Columns.Add(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool)).ReadOnly = true;
            schema.Columns.Add(SchemaTableOptionalColumn.IsHidden, typeof(bool)).ReadOnly = true;
            schema.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool)).ReadOnly = true;
            schema.Columns.Add(SchemaTableOptionalColumn.IsRowVersion, typeof(bool)).ReadOnly = true;

            // null marks columns that will change for each row
            object[] schemaRow = {
                    true,					// 00- AllowDBNull
					null,					// 01- BaseColumnName
					string.Empty,			// 02- BaseSchemaName
					string.Empty,			// 03- BaseTableName
					null,					// 04- ColumnName
					null,					// 05- ColumnOrdinal
					int.MaxValue,			// 06- ColumnSize
					typeof(string),			// 07- DataType
					false,					// 08- IsAliased
					false,					// 09- IsExpression
					false,					// 10- IsKey
					false,					// 11- IsLong
					false,					// 12- IsUnique
					DBNull.Value,			// 13- NumericPrecision
					DBNull.Value,			// 14- NumericScale
					(int) DbType.String,	// 15- ProviderType

					string.Empty,			// 16- BaseCatalogName
					string.Empty,			// 17- BaseServerName
					false,					// 18- IsAutoIncrement
					false,					// 19- IsHidden
					true,					// 20- IsReadOnly
					false					// 21- IsRowVersion
			  };

            int pos = 0;
            for (int i = 0; i < FieldCount; i++)
            {
                schemaRow[1] = GetName(i); // Base column name
                schemaRow[4] = GetName(i); // Column name
                schemaRow[5] = pos; // Column ordinal
                schemaRow[7] = CurrentRow == null ? typeof(string) : GetValue(i).GetType();
                schema.Rows.Add(schemaRow);
                pos++;
            }

            return schema;
        }
#endif
        #endregion

    }
}
