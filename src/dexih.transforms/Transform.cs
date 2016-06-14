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
using static dexih.transforms.TableColumn;

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
            PreLoadCache = 2
        }

        public enum EEncryptionMethod
        {
            NoEncryption = 0,
            EncryptSecureFields = 1,
            DecryptSecureFields = 2
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

        #region Generic Properties

        /// <summary>
        /// The main source of data.
        /// </summary>
        public Transform PrimaryTransform;

        /// <summary>
        /// The reference transform (such as join, or compare table).
        /// </summary>
        public Transform ReferenceTransform { get; set; }

        //Generic transform contains properties for a list of Functions, Fields and simple Mappings 
        public List<Function> Functions { get; set; } //functions used for complex mapping, conditions.
        public List<ColumnPair> ColumnPairs { get; set; } //fields pairs, used for simple mappings.
        public List<JoinPair> JoinPairs { get; set; } //fields pairs, used for table and service joins.
        public bool PassThroughColumns { get; set; } //indicates that any non-mapped columns should be mapped to the target.
        public List<Sort> SortFields { get; set; } //indicates fields for the sort transform.

        #endregion

        #region Virtual Properties
        public virtual List<Sort> RequiredSortFields() { return null; }
        public virtual List<Sort> RequiredReferenceSortFields() { return null; }

        public virtual bool RequiresSort { get; } = false; //indicates the transform must have sorted input 
        #endregion

        #region Abstract Properties

        public abstract bool InitializeOutputFields();
        public abstract string Details();
        protected abstract ReturnValue<object[]> ReadRecord();
        public abstract ReturnValue ResetTransform();

        #endregion

        #region Initialization 

        /// <summary>
        /// Sets the data readers for the transform.  Ensure the transform properties have been set prior to running this.
        /// </summary>
        /// <param name="primaryTransform">The primary input transform</param>
        /// <param name="referenceTransform">The secondary input, such as join table, target table, lookup table etc.</param>
        /// <returns></returns>
        public virtual bool SetInTransform(Transform primaryTransform, Transform referenceTransform = null)
        {

            //if the transform requires a sort and input data it not sorted, then add a sort transform.
            if (RequiresSort)
            {
                bool sortMatch = SortFieldsMatch(RequiredSortFields(), primaryTransform.CacheTable.OutputSortFields);

                if (!sortMatch)
                {
                    TransformSort sortTransform = new TransformSort(primaryTransform, RequiredSortFields());
                    PrimaryTransform = sortTransform;
                }
                else
                {
                    PrimaryTransform = primaryTransform;
                }

                if (referenceTransform != null)
                {
                    sortMatch = SortFieldsMatch(RequiredSortFields(), referenceTransform.CacheTable.OutputSortFields);

                    if (!sortMatch)
                    {
                        TransformSort sortTransform = new TransformSort(referenceTransform, RequiredReferenceSortFields());
                        ReferenceTransform = sortTransform;
                    }
                    else
                    {
                        ReferenceTransform = referenceTransform;
                    }
                }
            }
            else
            {
                bool sortMatch = SortFieldsMatch(RequiredSortFields(), primaryTransform.CacheTable.OutputSortFields);

                if (ReferenceTransform != null)
                {
                    sortMatch &= SortFieldsMatch(RequiredSortFields(), referenceTransform.CacheTable.OutputSortFields);
                }

                PrimaryTransform = primaryTransform;
                ReferenceTransform = referenceTransform;
            }
            InitializeOutputFields();
            Reset();
            return true;
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

        /// <summary>
        /// Opens underlying connections passing sort and filter requests through.
        /// </summary>
        /// <param name="filters">Requested filters for underlying transform to execute.</param>
        /// <param name="sorts">Requested sort for underlying transform to execute.</param>
        /// <returns></returns>
        public virtual async Task<ReturnValue> Open(List<Filter> filters = null, List<Sort> sorts = null)
        {
            if(PrimaryTransform != null)
                return await PrimaryTransform.Open(filters, sorts);
            return new ReturnValue(true);
        }

        #endregion

        #region Caching
        /// <summary>
        /// The method to cache data within the transform.  If NoCache is set, the data can only be read in a forward only method (Lookup and other functions are not available).
        /// </summary>
        public virtual ECacheMethod CacheMethod { get; protected set; } //indicates the data will be stored in memory.  This allows lookup and other operations to work

        /// <summary>
        /// The maximum number of rows to allow in the cache.  Use "0" for unlimited cache size.
        /// </summary>
        public int CacheMaxRows { get; protected set; } = 0;

        /// <summary>
        /// Sets the caching method and maximum number of rows.
        /// </summary>
        /// <param name="method">The method to cache data within the transform.  If NoCache is set, the data can only be read in a forward only method (Lookup and other functions are not available).</param>
        /// <param name="maxRows">The maximum number of rows to allow in the cache.  Use "0" for unlimited cache size.</param>
        public virtual void SetCacheMethod(ECacheMethod method, int maxRows = 0)
        {
            CacheMethod = method;
            CacheMaxRows = maxRows;
        }

        /// <summary>
        /// Table containing the cached reader data.
        /// </summary>
        public virtual Table CacheTable { get; protected set; }

        /// <summary>
        /// Indicates if the cache is complete or at maximum capacity
        /// </summary>
        public bool IsCacheFull { get; protected set; } 
        #endregion
        
        #region Encryption 
        /// <summary>
        /// Indictes the encryption method.  
        /// </summary>
        public EEncryptionMethod EncryptionMethod { get; protected set; }
        private string EncryptionKey { get; set; }

        /// <summary>
        /// Sets the method for the transform to encrypt data.  If encryption method is set to encrypt, then all columns with the SecurityFlag set will be encrypted or hashed as specified.  If encryption method is set to decrypt when columns with the security flag set to encrypt will be decrypted (note: hashed columns are one-way and cannot be decrypted.).
        /// </summary>
        /// <param name="encryptionMethod"></param>
        /// <param name="key"></param>
        public void SetEncryptionMethod(EEncryptionMethod encryptionMethod, string key)
        {
            EncryptionMethod = encryptionMethod;
            EncryptionKey = key;
        }

        public void SetColumnSecurityFlag(string columnName, ESecurityFlag securityFlag)
        {
            if (CacheTable == null)
                throw new Exception("Security flag can not be set as no CacheTable has been defined.");

            var column = CacheTable[columnName];

            if(column == null)
                throw new Exception("Security flag can not be set as the column " + columnName + " was not found in the table.");

            column.SecurityFlag = securityFlag;
        }

        private void EncryptRow(object[] row)
        {
            switch (EncryptionMethod)
            {
                case EEncryptionMethod.EncryptSecureFields:
                    for (int i = 0; i < CacheTable.Columns.Count; i++)
                    {
                        switch (CacheTable.Columns[i].SecurityFlag)
                        {
                            case TableColumn.ESecurityFlag.Encrypt:
                                row[i] = EncryptString.Encrypt(row[i].ToString(), EncryptionKey);
                                break;
                            case TableColumn.ESecurityFlag.OneWayHash:
                                row[i] = PasswordHash.CreateHash(row[i].ToString());
                                break;
                        }
                    }
                    break;
                case EEncryptionMethod.DecryptSecureFields:
                    for (int i = 0; i < CacheTable.Columns.Count; i++)
                    {
                        switch (CacheTable.Columns[i].SecurityFlag)
                        {
                            case TableColumn.ESecurityFlag.Encrypt:
                                row[i] = EncryptString.Decrypt(row[i].ToString(), EncryptionKey);
                                break;
                        }
                    }
                    break;
            }

        }


        #endregion



        #region Performance Diagnostics 
        //diagnostics to record the processing time for the transformation.
        public Stopwatch TransformTimer { get; set; }

        /// <summary>
        /// This is a recursive function that goes through each of the transforms and returns timer values when it gets to a connection.
        /// </summary>
        /// <param name="recordsRead"></param>
        /// <param name="elapsedMilliseconds"></param>
        /// <returns></returns>
        public virtual void ReadThroughput(ref int recordsRead, ref long elapsedMilliseconds)
        {
            PrimaryTransform?.ReadThroughput(ref recordsRead, ref elapsedMilliseconds);

            PrimaryTransform.ReferenceTransform?.ReadThroughput(ref recordsRead, ref elapsedMilliseconds);
        }

        /// <summary>
        /// The number of timer ticks during the read function for this transform (underlying transform performance is substracted from this one.).
        /// </summary>
        /// <returns></returns>
        public long TransformTimerTicks()
        {
            long ticks = TransformTimer.ElapsedTicks;

            if (PrimaryTransform != null)
                ticks = ticks - PrimaryTransform.TransformTimerTicks();
            if (ReferenceTransform != null)
                ticks = ticks - ReferenceTransform.TransformTimerTicks();

            return ticks;
        }

        #endregion


        #region Record Navigation

        /// <summary>
        /// Inidicates if the source reader has completed, without moving to the next record.
        /// </summary>
        public bool IsReaderFinished { get; protected set; }

        private bool isResetting = false; //flag to indicate reset is underway.
        private object[] CurrentRow; //stores data for the current row.
        protected int CurrentRowNumber = -1; //current row number


        /// <summary>
        /// Resets the transform and any source transforms.
        /// </summary>
        /// <returns></returns>
        public ReturnValue Reset()
        {
            if (!isResetting) //stops recursive looops where intertwinned transforms are resetting each other
            {
                isResetting = true;

                ReturnValue returnValue;

                returnValue = ResetTransform();

                if (!returnValue.Success)
                    return returnValue;

                if (PrimaryTransform != null)
                {
                    returnValue = PrimaryTransform.Reset();
                    if (!returnValue.Success)
                        return returnValue;
                }

                if (ReferenceTransform != null)
                {
                    returnValue = ReferenceTransform.Reset();
                    if (!returnValue.Success)
                        return returnValue;
                }

                IsReaderFinished = false;

                isResetting = false;
                return new ReturnValue(true);
            }

            return new ReturnValue(false);
        }

        //Set the reader to a specific row.  If the rows has exceeded MaxRows this will only start from the beginning of the cache.   A read() is required folowing this to get data.
        public void SetRowNumber(int rowNumber = 0)
        {
            if (rowNumber <= CacheTable.Data.Count)
                CurrentRowNumber = rowNumber - 1;
            else
                throw new Exception("SetRowNumber failed, as the row exceeded the number of rows in the cache");
        }

        /// <summary>
        /// Allows a specific row in the cache to be accessed. 
        /// </summary>
        /// <param name="rowNumber">The row number from the cache</param>
        /// <param name="values">An array length(FieldCount) that will contain the values from the row.</param>
        public void RowPeek(int rowNumber, object[] values)
        {
            if (rowNumber >= CacheTable.Data.Count)
                throw new Exception("RowPeek failed, as the row exceeded the number of rows in the cache");

            CacheTable.Data[rowNumber].CopyTo(values, 0);
        } 
        #endregion

        #region Lookup
        /// <summary>
        /// Performs a row lookup based on the filters.  For mutliple rows, only the first will be returned.
        /// The lookup will first attempt to retrieve a value from the cache (if cachemethod is set to PreLoad cache or OnDemandCache), and then a direct lookup if the transform supports it.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        public virtual async Task<ReturnValue<object[]>> LookupRow(List<Filter> filters)
        {
            if (CacheMethod == ECacheMethod.PreLoadCache)
            {
                //preload all records.
                await Task.Run(() =>
                {
                    while (Read()) ;
                });

                return CacheTable.LookupSingleRow(filters);
            }
            else if (CacheMethod == ECacheMethod.OnDemandCache)
            {
                //lookup in the cache.
                var lookupResult = CacheTable.LookupSingleRow(filters);
                if (lookupResult.Success)
                    return lookupResult;

                if (CanLookupRowDirect)
                {
                    //not found in the cache, attempt a direct lookup.
                    lookupResult = await LookupRowDirect(filters);

                    if (lookupResult.Success)
                    {
                        if (EncryptionMethod != EEncryptionMethod.NoEncryption)
                            EncryptRow(lookupResult.Value);

                        CacheTable.Data.Add(lookupResult.Value);
                    }
                }
                else
                {
                    //not found in the cache, keep reading until it's found.
                    return await Task.Run(() =>
                    {
                        while (Read())
                        {
                            //does a lookup, using the record count to only check the latest record.
                            if (lookupResult.Success == true)
                                return lookupResult;
                        }

                        return new ReturnValue<object[]>(false, "Lookup not found.", null);
                    });
                }
            }

            //if no caching is specified, run a direct lookup.
            var lookupReturn = await LookupRowDirect(filters);
            if (lookupReturn.Success)
            {
                if (EncryptionMethod != EEncryptionMethod.NoEncryption)
                    EncryptRow(lookupReturn.Value);
            }

            return lookupReturn;
        }

        public virtual bool CanLookupRowDirect { get; } = false;

        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        public virtual async Task<ReturnValue<object[]>> LookupRowDirect(List<Filter> filters)
        {
            return await Task.Run(() => new ReturnValue<object[]>(false, "Lookup can not be performed unless transform caching is set on.", null));
        } 
        #endregion

        #region DbDataReader Implementation

        public override bool Read()
        {
            //starts  a timer that can be used to measure downstream transform and database performance.
            TransformTimer.Start();

            CurrentRowNumber++;

            //check cache for a row first.
            if(CacheMethod == ECacheMethod.OnDemandCache || CacheMethod == ECacheMethod.PreLoadCache)
            {
                if(CurrentRowNumber < CacheTable.Data.Count)
                {
                    CurrentRow = CacheTable.Data[CurrentRowNumber];
                    TransformTimer.Stop();
                    return true;
                }
            }

            if (IsReaderFinished == true)
            {
                CurrentRow = null;
                TransformTimer.Stop();
                return false;
            }

            var returnValue = ReadRecord();

            if (returnValue.Success)
            {
                if (EncryptionMethod != EEncryptionMethod.NoEncryption)
                    EncryptRow(returnValue.Value);

                CurrentRow = returnValue.Value;
            }

            if (returnValue.Success == false)
                IsReaderFinished = true;

            //add the row to the cache
            if (returnValue.Success == true && (CacheMethod == ECacheMethod.OnDemandCache || CacheMethod == ECacheMethod.PreLoadCache))
                CacheTable.Data.Add(CurrentRow);

            TransformTimer.Stop();
            return returnValue.Success;
        }

        public override int FieldCount => CacheTable.Columns.Count; 
        public override int GetOrdinal(string columnName) => CacheTable.GetOrdinal(columnName);
        public override string GetName(int i) => CacheTable.Columns[i].ColumnName;
        public override object this[string name]
        {
            get
            {
                int ordinal = GetOrdinal(name);
                if (ordinal < 0)
                    throw new Exception("The column " + name + " could not be found in the table.");

                return GetValue(GetOrdinal(name));
            }
        }
        public override object this[int i] => GetValue(i);

        public override int Depth
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool IsClosed => PrimaryTransform.IsClosed;

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
            if (values.Length > CurrentRow.Length)
                throw new Exception("Could not GetValues as the input array was length " + values.Length.ToString() + " which is greater than the current number of fields " + CurrentRow.Length.ToString() + ".");

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
                PrimaryTransform.Dispose(disposing);
                if (ReferenceTransform != null)
                    ReferenceTransform.Dispose(disposing);
            }
            base.Dispose(disposing);
        }

        public override bool NextResult()
        {
            return Read();
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException("This feature is not currently implemnted.");
        }

        public override bool HasRows => PrimaryTransform.HasRows;

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

            for (int i = 0; i < CacheTable.Columns.Count; i++)
            {
                var col = CacheTable.Columns[i];
                schemaRow[0] = col.AllowDbNull; // Base column name
                schemaRow[1] = col.ColumnName; // Base column name
                schemaRow[4] = col.ColumnName; // Column name
                schemaRow[5] = i; // Column ordinal
                schemaRow[6] = col.MaxLength > 0 ? col.MaxLength : int.MaxValue;
                schemaRow[7] = DataType.GetType(col.DataType);
                schemaRow[10] = col.DeltaType == EDeltaType.SurrogateKey ? true : false;
                schemaRow[12] = col.DeltaType == EDeltaType.SurrogateKey ? true : false;
                schemaRow[13] = col.Precision == null ? DBNull.Value : (object)col.Precision;
                schemaRow[14] = col.Scale == null ? DBNull.Value : (object)col.Scale;
                schema.Rows.Add(schemaRow);
            }

            return schema;
        }
#else
        
#endif
        #endregion

    }
}
