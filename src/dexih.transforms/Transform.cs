using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks;
using dexih.functions;
using System.Linq;
using static dexih.functions.TableColumn;
using System.Collections.ObjectModel;
using System.Threading;
using System.Text;
using dexih.functions.Mappings;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.Crypto;
using dexih.transforms.Exceptions;

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
            EncryptDecryptSecureFields = 1,
            MaskSecureFields = 2
        }

        #endregion

        protected Transform()
        {
            //intialize standard objects.
//            ColumnPairs = new List<ColumnPair>();
//            JoinPairs = new List<Join>();
//            FilterPairs = new List<FilterPair>();
//            AggregatePairs = new List<AggregatePair>();
//            Functions = new List<TransformFunction>();
            TransformTimer = new Stopwatch();
        }

        #region Generic Properties
		[JsonConverter(typeof(StringEnumConverter))]
        public enum EDuplicateStrategy
        {
            Abend,
            First,
            Last,
            All
        }

		/// <summary>
		/// Optional: Any name describing the transform.
		/// </summary>
		/// <value>The name.</value>
		public string Name { get; set; }

        /// <summary>
        /// The main source of data.
        /// </summary>
		public Transform PrimaryTransform { get; set; }

        /// <summary>
        /// The reference transform (such as join, or compare table).
        /// </summary>
        public Transform ReferenceTransform { get; set; }

        // Generic transform contains properties for a list of Functions, Fields and simple Mappings
        public Mappings Mappings { get; set; }

//        public List<TransformFunction> Functions { get; set; } //functions used for complex mapping, conditions.
//        public List<ColumnPair> ColumnPairs { get; set; } //fields pairs, used for simple mappings.
//        public List<Join> JoinPairs { get; set; } //fields pairs, used for table and service joins.
//        public List<FilterPair> FilterPairs { get; set; } //fields pairs, used for simple filters
//        public List<AggregatePair> AggregatePairs { get; set; } //fields pairs, used for simple filters

        public TableColumn JoinSortField { get; set; }
        public EDuplicateStrategy? JoinDuplicateStrategy { get; set; } = EDuplicateStrategy.Abend;

//        public virtual bool PassThroughColumns { get; set; } //indicates that any non-mapped columns should be mapped to the target.
        public virtual List<Sort> SortFields => PrimaryTransform?.SortFields; //indicates fields for the sort transform.

        public string ReferenceTableAlias { get; set; } //used as an alias for joined tables when the same table is joined multiple times.

        public Connection ReferenceConnection { get; set; } //database connection reference (for start readers only).

        //indicates if the transform is on the primary stream.
        public bool IsPrimaryTransform { get; set; } = true;

        //indicates if the transform is a base reader.
        public bool IsReader { get; set; } = true;

        public long AuditKey { get; set; }

        #endregion

        #region Statistics

        //statistics for this transform
        public long TransformRowsSorted { get; protected set; }
        public long TransformRowsPreserved { get; protected set; }
        public long TransformRowsIgnored { get; protected set; }
        public long TransformRowsRejected { get; protected set; }
        public long TransformRowsFiltered { get; protected set; }
        public long TransformRowsReadPrimary { get; protected set; }
        public long TransformRowsReadReference { get; protected set; }

        //statistics for all child transforms.
        public long TotalRowsSorted => TransformRowsSorted + PrimaryTransform?.TotalRowsSorted ?? 0 + ReferenceTransform?.TotalRowsSorted ?? 0;
        public long TotalRowsPreserved => TransformRowsPreserved + PrimaryTransform?.TotalRowsPreserved ?? 0 + ReferenceTransform?.TotalRowsPreserved ?? 0;
        public long TotalRowsIgnored => TransformRowsIgnored + PrimaryTransform?.TotalRowsIgnored ?? 0 + ReferenceTransform?.TotalRowsIgnored ?? 0;
        public long TotalRowsRejected => TransformRowsRejected + PrimaryTransform?.TotalRowsRejected ?? 0 + ReferenceTransform?.TotalRowsRejected ?? 0;
        public long TotalRowsFiltered => TransformRowsFiltered + PrimaryTransform?.TotalRowsFiltered ?? 0 + ReferenceTransform?.TotalRowsFiltered ?? 0;
        public long TotalRowsReadPrimary => TransformRowsReadPrimary + (PrimaryTransform?.TotalRowsReadPrimary ?? 0);
        public long TotalRowsReadReference => TransformRowsReadReference + ReferenceTransform?.TotalRowsReadReference ?? 0 + ReferenceTransform?.TransformRowsReadPrimary ?? 0;

        private object _maxIncrementalValue = null;
        private int _incrementalColumnIndex = -1;
        private ETypeCode _incrementalColumnType;

        private Dictionary<SelectQuery, ICollection<object[]>> _lookupCache;

        public object GetMaxIncrementalValue()
        {
            return IsReader ? _maxIncrementalValue : PrimaryTransform.GetMaxIncrementalValue();
        }

        public virtual Transform GetProfileResults()
        {
            return IsReader ? null : PrimaryTransform.GetProfileResults();
        }

        /// <summary>
        /// Create as a profile results table.
        /// </summary>
        /// <returns></returns>
        public Table GetProfileTable(string profileTableName)
        {
            if (string.IsNullOrEmpty(profileTableName)) return null;

            var profileResults = new Table(profileTableName, "");
            profileResults.Columns.Add(new TableColumn("AuditKey", ETypeCode.Int64, EDeltaType.CreateAuditKey));
            profileResults.Columns.Add(new TableColumn("Profile", ETypeCode.String));
            profileResults.Columns.Add(new TableColumn("ColumnName", ETypeCode.String));
            profileResults.Columns.Add(new TableColumn("IsSummary", ETypeCode.Boolean));
            profileResults.Columns.Add(new TableColumn("Value", ETypeCode.String) { AllowDbNull = true });
            profileResults.Columns.Add(new TableColumn("Count", ETypeCode.Int32) { AllowDbNull = true });

            return profileResults;
        }

        #endregion

        #region Virtual Properties
        public virtual List<Sort> RequiredSortFields() { return null; }
        public virtual List<Sort> RequiredReferenceSortFields() { return null; }
        public virtual bool RequiresSort { get; } = false; //indicates the transform must have sorted input 

        #endregion

        #region Abstract Properties

        public abstract bool InitializeOutputFields();
        public abstract string Details();
        protected abstract Task<object[]> ReadRecord(CancellationToken cancellationToken);
        public abstract bool ResetTransform();

        #endregion

        #region Initialization 

        /// <summary>
        /// Sets the data readers for the transform.  Ensure the transform properties have been set prior to running this.
        /// </summary>
        /// <param name="primaryTransform">The primary input transform</param>
        /// <param name="referenceTransform">The secondary input, such as join table, target table, lookup table etc.</param>
        /// <returns></returns>
        public bool SetInTransform(Transform primaryTransform, Transform referenceTransform = null)
        {
            PrimaryTransform = primaryTransform;
            ReferenceTransform = referenceTransform;

            //if the transform requires a sort and input data it not sorted, then add a sort transform.
            if (RequiresSort)
            {
                var sortMatch = SortFieldsMatch(RequiredSortFields(), primaryTransform.CacheTable.OutputSortFields);

                if (!sortMatch)
                {
                    var sortTransform = new TransformSort(primaryTransform, RequiredSortFields());
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
                        var sortTransform = new TransformSort(referenceTransform, RequiredReferenceSortFields());
                        ReferenceTransform = sortTransform;
                    }
                    else
                    {
                        ReferenceTransform = referenceTransform;
                    }
                }
            }

            Mappings?.Initialize(primaryTransform.CacheTable, referenceTransform?.CacheTable);

            InitializeOutputFields();
            Reset();
            
            //IsReader indicates if this is a base transform.
            IsReader = primaryTransform == null ? true : false;
            if (primaryTransform != null)
                primaryTransform.IsPrimaryTransform = true;
            if (referenceTransform != null)
                referenceTransform.IsPrimaryTransform = false;

           
            return true;
        }

        /// <summary>
        /// This function will confirm that the ActualSort is equivalent to the RequiredSort.  Note: ascending/descending is ignored as this makes no difference for the transforms.
        /// </summary>
        /// <param name="requiredSort"></param>
        /// <param name="actualSort"></param>
        /// <returns></returns>
        public bool SortFieldsMatch(List<Sort> requiredSort, List<Sort> actualSort)
        {
            if (requiredSort == null && actualSort == null)
                return true;

            if (requiredSort == null || actualSort == null)
                return false;

            if (requiredSort.Count < actualSort.Count)
                return false;

            var match = true;

            using (var actualSortEnumerator = actualSort.GetEnumerator())
            {
                foreach (var requiredField in requiredSort)
                {
                    if(!actualSortEnumerator.MoveNext())
                    {
                        match = false;
                        break;
                    }
                    var actualField = actualSortEnumerator.Current;



                    if (requiredField.Column.TableColumnName() == actualField.Column.TableColumnName())
                    {
                        continue;
                    }

                    if (requiredField.Column.Name == actualField.Column.Name)
                    {
                        continue;
                    }

                    match = false;
                    break;
                }
            }

            return match;

        }

        /// <summary>
        /// Opens underlying connections passing sort and filter requests through.
        /// </summary>
        /// <param name="auditKey"></param>
        /// <param name="query"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>True is successful, False is unsuccessful.</returns>
        public virtual async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            var result = true;

            if (PrimaryTransform != null)
            {
                result = result && await PrimaryTransform.Open(auditKey, query, cancellationToken);
                if (!result)
                    return result;
            }

            if (ReferenceTransform != null)
            {
                result = result && await ReferenceTransform.Open(auditKey, null, cancellationToken);
            }

            return result;
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
        /// Sets the method for the transform to encrypt data.  
        /// If encryption method is set to encrypt, then all columns with the SecurityFlag set will be encrypted or hashed as specified.  
        /// If encryption method is set to decrypt then columns with the security flag set to encrypt will be decrypted (note: hashed columns are one-way and cannot be decrypted.).
        /// </summary>
        /// <param name="encryptionMethod"></param>
        /// <param name="key"></param>
        public void SetEncryptionMethod(EEncryptionMethod encryptionMethod, string key)
        {
            if(CacheTable.Columns.All(c => c.SecurityFlag == ESecurityFlag.None))
            {
                EncryptionMethod = EEncryptionMethod.NoEncryption;
            }
            else
            {
                EncryptionMethod = encryptionMethod;

                if (EncryptionMethod == EEncryptionMethod.EncryptDecryptSecureFields && string.IsNullOrEmpty(key))
                {
                    throw new TransformException("The encryption could not be enabled as there is no encryption key set.");
                }

                EncryptionKey = key;
            }

            PrimaryTransform?.SetEncryptionMethod(encryptionMethod, key);
            ReferenceTransform?.SetEncryptionMethod(encryptionMethod, key);
        }

        public void SetColumnSecurityFlag(string columnName, ESecurityFlag securityFlag)
        {
            if (CacheTable == null)
                throw new Exception("Security flag can not be set as no CacheTable has been defined.");

            var column = CacheTable[columnName];

            if (column == null)
                throw new Exception("Security flag can not be set as the column " + columnName + " was not found in the table.");

            column.SecurityFlag = securityFlag;
        }

        protected string FastEncrypt(object value)
        {
            if (value is null) return null;
            return EncryptString.Encrypt(value.ToString(), EncryptionKey, 5);
        }

        protected string FastDecrypt(object value)
        {
            if (value is null) return null;
            return EncryptString.Decrypt(value.ToString(), EncryptionKey, 5);
        }
        
        protected string StrongEncrypt(object value)
        {
            if (value is null) return null;
            return EncryptString.Encrypt(value.ToString(), EncryptionKey, 1000);
        }

        protected string StrongDecrypt(object value)
        {
            if (value is null) return null;
            return EncryptString.Decrypt(value.ToString(), EncryptionKey, 1000);
        }

        protected string OneWayHash(object value)
        {
            if (value is null) return null;
            return HashString.CreateHash(value.ToString());
        }

        protected bool OneWayHashCompare(object hashedValue, object value)
        {
            if (value is null) return false;
            return HashString.ValidateHash(value.ToString(), hashedValue.ToString());
        }
        

        private void EncryptRow(object[] row)
        {
            switch (EncryptionMethod)
            {
                case EEncryptionMethod.EncryptDecryptSecureFields:
                    var columnCount = CacheTable.Columns.Count;
                    for (var i = 0; i < columnCount; i++)
                    {
                        switch (CacheTable.Columns[i].SecurityFlag)
                        {
                            case ESecurityFlag.StrongEncrypt:
                                row[i] = new EncryptedObject(row[i], StrongEncrypt(row[i]));
                                break;
                            case ESecurityFlag.OneWayHash:
                                row[i] = new EncryptedObject(row[i], OneWayHash(row[i]));
                                break;
                            case ESecurityFlag.StrongDecrypt:
                                row[i] = new EncryptedObject(row[i], StrongDecrypt(row[i]));
                                break;
                            case ESecurityFlag.FastEncrypt:
                                row[i] = new EncryptedObject(row[i], FastEncrypt(row[i]));
                                break;
                            case ESecurityFlag.FastDecrypt:
                                row[i] = new EncryptedObject(row[i], FastDecrypt(row[i]));
                                break;
                            case ESecurityFlag.Hide:
                                row[i] = null;
                                break;
                        }
                    }
                    break;
                case EEncryptionMethod.MaskSecureFields:
                    columnCount = CacheTable.Columns.Count;
                    for (var i = 0; i < columnCount; i++)
                    {
                        switch (CacheTable.Columns[i].SecurityFlag)
                        {
                            case ESecurityFlag.FastEncrypt:
                            case ESecurityFlag.FastEncrypted:
                            case ESecurityFlag.StrongEncrypt:
                            case ESecurityFlag.StrongEncrypted:
								row[i] = "(Encrypted)";
								break;
							case ESecurityFlag.OneWayHash:
                            case ESecurityFlag.OnWayHashed:
								row[i] = "(Hashed)";
								break;
							case ESecurityFlag.Hide:
								row[i] = "(Hidden)";
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

        ///// <summary>
        ///// This is a recursive function that goes through each of the transforms and returns timer values when it gets to a connection.
        ///// </summary>
        ///// <param name="recordsRead"></param>
        ///// <param name="elapsedMilliseconds"></param>
        ///// <returns></returns>
        //public virtual void ReadThroughput(ref long recordsRead, ref long elapsedTicks)
        //{
        //    if (PrimaryTransform == null)
        //    {
        //        elapsedTicks += TimerTicks();
        //        recordsRead += TransformRowsReadPrimary;
        //    }
        //    else
        //        PrimaryTransform?.ReadThroughput(ref recordsRead, ref elapsedTicks);

        //    if(ReferenceTransform != null)
        //        ReferenceTransform?.ReadThroughput(ref recordsRead, ref elapsedTicks);
        //}

        /// <summary>
        /// The number of timer ticks specifically for this transform.
        /// </summary>
        /// <returns></returns>
        public TimeSpan TransformTimerTicks()
        {
            var ticks = TimerTicks();

            if (PrimaryTransform != null)
                ticks = ticks - PrimaryTransform.TimerTicks();
            if (ReferenceTransform != null)
                ticks = ticks - ReferenceTransform.TimerTicks();

            return ticks;
        }

        /// <summary>
        /// The aggregates the number of timer ticks for any underlying base readers.  This provides a view of how long database/read operations are taking.
        /// </summary>
        /// <returns></returns>
        public TimeSpan ReaderTimerTicks()
        {
            if (IsReader)
                return TransformTimer.Elapsed;
            else
            {
				var ticks = PrimaryTransform?.ReaderTimerTicks()??TimeSpan.FromTicks(0) + ReferenceTransform?.ReaderTimerTicks()??TimeSpan.FromTicks(0);
                return ticks;
            }
        }

        /// <summary>
        /// The aggregates the number of timer ticks for this and underlying transforms, excluding the time taken for base readers. 
        /// </summary>
        /// <returns></returns>
        public TimeSpan ProcessingTimerTicks()
        {
            return TransformTimer.Elapsed - ReaderTimerTicks();
        }

        /// <summary>
        /// The total timer ticks for this transform.  This includes any underlying processing.
        /// </summary>
        /// <returns></returns>
        public TimeSpan TimerTicks() => TransformTimer.Elapsed;


        public string PerformanceSummary()
        {
            var performance = new StringBuilder();

            if (PrimaryTransform != null)
                performance.AppendLine(PrimaryTransform.PerformanceSummary());

			var timeSpan = TransformTimerTicks();

            if (timeSpan.Ticks == 0)
            {
                performance.AppendLine(
                    $"{Details()} - Not used.");
            }
            else
            {
                performance.AppendLine($"{Details()} - Time: {timeSpan:c}, Rows: {TotalRowsReadPrimary}, Performance: {(TotalRowsReadPrimary/timeSpan.TotalSeconds):F} rows/second");
            }


            if (ReferenceTransform != null)
                performance.AppendLine("\tReference: " + ReferenceTransform.PerformanceSummary());

            return performance.ToString();
        }

        #endregion

        #region Record Navigation

        /// <summary>
        /// Inidicates if the source reader has completed, without moving to the next record.
        /// </summary>
        public bool IsReaderFinished { get; protected set; }

        private bool _isResetting = false; //flag to indicate reset is underway.
        public object[] CurrentRow { get; protected set; } //stores data for the current row.
        private bool _currentRowCached;
        protected int CurrentRowNumber = -1; //current row number

        /// <summary>
        /// Resets the transform and any source transforms.
        /// </summary>
        /// <returns></returns>
        public bool Reset()
        {
            if (!_isResetting) //stops recursive looops where intertwinned transforms are resetting each other
            {
                _isResetting = true;

                var returnValue = true;

                _isFirstRead = true;
                IsCacheFull = false;
                IsReaderFinished = false;
                // CacheTable.Data.Clear();
                CurrentRowNumber = -1;

                //reset stats.
                TransformRowsSorted = 0;
                TransformRowsFiltered = 0;
                TransformRowsIgnored = 0;
                TransformRowsPreserved = 0;
                TransformRowsReadPrimary = 0;
                TransformRowsReadReference = 0;
                TransformRowsRejected = 0;

                returnValue = ResetTransform();

                //if (!returnValue.Success)
                //    return returnValue;

                if (PrimaryTransform != null)
                {
                    returnValue = returnValue && PrimaryTransform.Reset();
                    //if (!returnValue.Success)
                    //    return returnValue;
                }

                if (ReferenceTransform != null)
                {
                    returnValue = returnValue && ReferenceTransform.Reset();
                    //if (!returnValue.Success)
                    //    return returnValue;
                }

                IsReaderFinished = false;

                _isResetting = false;
                return returnValue;
            }

            return false;
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
        /// <param name="filters">Lookup filters</param>
        /// <param name="duplicateStrategy">Action to take when duplicate rows are returned.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual async Task<IEnumerable<object[]>> Lookup(SelectQuery query, EDuplicateStrategy duplicateStrategy, CancellationToken cancellationToken)
        {

            if (_lookupCache != null && _lookupCache.ContainsKey(query))
            {
                return _lookupCache[query];
            }
            
            var lookupResult = new List<object[]>();
            var initResult = await InitializeLookup(AuditKey, query, cancellationToken);

            switch (duplicateStrategy)
            {
                case EDuplicateStrategy.First:
                    if (await ReadAsync(cancellationToken))
                    {
                        lookupResult.Add(CurrentRow);
                    }

                    break;
                case EDuplicateStrategy.Last:
                    object[] lastRow = null;
                    while (await ReadAsync(cancellationToken))
                    {
                        lastRow = CurrentRow;
                    }

                    if (lastRow != null)
                    {
                        lookupResult.Add(lastRow);
                    }

                    break;
                case EDuplicateStrategy.All:
                    while (await ReadAsync(cancellationToken))
                    {
                        lookupResult.Add(CurrentRow);
                    }

                    break;
                case EDuplicateStrategy.Abend:
                    if (await ReadAsync(cancellationToken))
                    {
                        lookupResult.Add(CurrentRow);

                        if (await ReadAsync(cancellationToken))
                        {
                            throw new TransformException(
                                "The lookup row failed as multiple rows were returned and the duplicate strategy is to abend.");
                        }
                    }

                    break;
            }

            if (CacheMethod == ECacheMethod.OnDemandCache || CacheMethod == ECacheMethod.PreLoadCache)
            {
                if (_lookupCache == null)
                {
                    _lookupCache = new Dictionary<SelectQuery, ICollection<object[]>>();
                }

                _lookupCache.Add(query, lookupResult);
            }

            if (EncryptionMethod != EEncryptionMethod.NoEncryption)
            {
                foreach (var row in lookupResult)
                {
                    EncryptRow(row);
                }
            }

            return lookupResult;

//            if (!initResult)
//            {
//                throw new TransformException("The lookup failed to inialize.");
//            }
//
//            // preload all the records into memory on the first call.
//            if (CacheMethod == ECacheMethod.PreLoadCache)
//            {
//                //preload all records.
//                while (await ReadAsync(cancellationToken))
//                {
//                }
//
//                if(duplicateStrategy == EDuplicateStrategy.First)
//                {
//                    var result = CacheTable.LookupSingleRow(filters);
//                    lookupResult = result == null ? null : new[] { result };
//                }
//                else
//                {
//                    lookupResult = CacheTable.LookupMultipleRows(filters);
//                }
//            }
//            else if (CacheMethod == ECacheMethod.OnDemandCache)
//            {
//                if (duplicateStrategy == EDuplicateStrategy.First)
//                {
//                    var result = CacheTable.LookupSingleRow(filters);
//                    lookupResult = result == null ? null : new[] { result };
//                }
//                else
//                {
//                    lookupResult = CacheTable.LookupMultipleRows(filters);
//                }
//
//                if (lookupResult == null)
//                {
//                    if (CanLookupRowDirect)
//                    {
//                        //not found in the cache, attempt a direct lookup.
//                        lookupResult = await LookupRowDirect(filters, duplicateStrategy, cancellationToken);
//
//                        if (lookupResult != null && lookupResult.Any())
//                        {
//                            if (EncryptionMethod != EEncryptionMethod.NoEncryption)
//                            {
//                                foreach(var row in lookupResult)
//                                {
//                                    EncryptRow(row);
//                                }
//                            }
//
//                            CacheTable.Data.AddRange(lookupResult);
//                            _currentRowCached = true;
//                        }
//                    }
//                    else
//                    {
//                        //not found in the cache, keep reading until it's found.
//                        if (duplicateStrategy == EDuplicateStrategy.First || duplicateStrategy == EDuplicateStrategy.Last)
//                        {
//                            while (await ReadAsync(cancellationToken))
//                            {
//                                if (CacheTable.RowMatch(filters, CurrentRow))
//                                {
//                                    lookupResult = new[] { CurrentRow };
//                                    if (duplicateStrategy == EDuplicateStrategy.First)
//                                    {
//                                        break;
//                                    }
//                                }
//                            }
//                        } 
//                        else
//                        {
//                            // if the lookup is multiple records, scan entire dataset.
//                            while (await ReadAsync(cancellationToken)) continue;
//                            lookupResult = CacheTable.LookupMultipleRows(filters);
//
//                        }
//                    }
//                }
//            }
//            else
//            {
//                //if no caching is specified, run a direct lookup.
//                lookupResult = await LookupRowDirect(filters, duplicateStrategy, cancellationToken);
//                if (lookupResult != null)
//                {
//                    if (EncryptionMethod != EEncryptionMethod.NoEncryption)
//                    {
//                        foreach (var row in lookupResult)
//                        {
//                            EncryptRow(row);
//                        }
//                    }
//                }
//            }
//
//            if(lookupResult == null || !lookupResult.Any())
//            {
//                return null;
//            }
//
//            switch (duplicateStrategy)
//            {
//                case EDuplicateStrategy.First:
//                    return new[] { lookupResult.First() };
//                case EDuplicateStrategy.Last:
//                    return new[] { lookupResult.Last() };
//                case EDuplicateStrategy.All:
//                    return lookupResult;
//                case EDuplicateStrategy.Abend:
//                    if (lookupResult.Count() > 1)
//                    {
//                        throw new TransformException("The lookup row failed as multiple rows were returned at the duplicate strategy is to abend when this happens.");
//                    }
//                    return lookupResult;
//            }
//
//            return null;
        }

        /// <summary>
        /// Recurses through the transforms to the primary transform, and sets the lookup filters.
        /// </summary>
        /// <param name="auditKey"></param>
        /// <param name="filters"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual async Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            
            // update the input value on any input columns.
            foreach (var column in CacheTable.Columns.Where(c => c.IsInput))
            {
                var filter = query.Filters.FirstOrDefault(c => c.Column1.Name == column.Name);
                if (filter != null)
                {
                    column.DefaultValue = filter.Value2;
                }
            }

            if (PrimaryTransform == null)
            {
                return false;
            }
            else
            {
                return await PrimaryTransform.InitializeLookup(auditKey, query, cancellationToken);
            }
        }

        /// <summary>
        /// Used to allow connections to close files and other objects after a lookup is complete.
        /// </summary>
        /// <returns></returns>
        public virtual bool FinalizeLookup()
        {
            return true;
        }


//        /// <summary>
//        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
//        /// </summary>
//        /// <param name="filters"></param>
//        /// <param name="duplicateStrategy"></param>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        public virtual Task<ICollection<object[]>> LookupRowDirect(ICollection<Filter> filters, EDuplicateStrategy duplicateStrategy, CancellationToken cancellationToken)
//        {
//            throw new TransformException($"The transform {Name} does not support direct lookups.");
//            
////            if(SetLookupFilters(filters))
////            {
////                Reset();
////                var query = new SelectQuery() { Filters = filters };
////                await Open(AuditKey, query, cancellationToken);
////
////                ICollection<object[]> lookupResult = null;
////                //not found in the cache, keep reading until it's found.
////
////                if (duplicateStrategy == EDuplicateStrategy.First || duplicateStrategy == EDuplicateStrategy.Last)
////                {
////                    while (await ReadAsync(cancellationToken))
////                    {
////                        if (CacheTable.RowMatch(filters, CurrentRow))
////                        {
////                            lookupResult = new[] { CurrentRow };
////                            if(duplicateStrategy == EDuplicateStrategy.First)
////                            {
////                                break;
////                            }
////                        }
////                    }
////                }
////                else
////                {
////                    var result = new List<object[]>();
////
////                    // if the lookup is multiple records, scan entire dataset.
////                    while (await ReadAsync(cancellationToken))
////                    {
////                        if (CacheTable.RowMatch(filters, CurrentRow))
////                        {
////                            result.Add(CurrentRow);
////                        }
////                    }
////
////                    lookupResult = result;
////                }
////
////                return lookupResult;
////            }
////            else
////            {
////                throw new TransformException("The lookup can not be performed as the data source does not support direct lookups.");
////            }
//        }

        #endregion

        #region DbDataReader Implementation

        private bool _isFirstRead = true;

        public override bool Read()
        {
            return ReadAsync(CancellationToken.None).Result;
        }

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            //starts  a timer that can be used to measure downstream transform and database performance.
            TransformTimer.Start();

            if (_isFirstRead)
            {
                if (IsReader && IsPrimaryTransform)
                {
                    //get the incremental column (if it exists)
                    var incrementalCol = CacheTable.Columns.Where(c => c.IsIncrementalUpdate).ToArray();
                    if (incrementalCol.Length == 1)
                    {
                        _incrementalColumnIndex = CacheTable.GetOrdinal(incrementalCol[0].Name);
                        _incrementalColumnType = incrementalCol[0].DataType;
                    }
                    else if (incrementalCol.Length > 1)
                    {
                        throw new Exception("Cannot run the transform as two columns have been defined with IncrementalUpdate flags.");
                    }
                    else
                        _incrementalColumnIndex = -1;
                }

                _isFirstRead = false;
            }

            CurrentRowNumber++;

            //check cache for a row first.
            if (CacheMethod == ECacheMethod.OnDemandCache || CacheMethod == ECacheMethod.PreLoadCache)
            {
                if (CurrentRowNumber < CacheTable.Data.Count)
                {
                    CurrentRow = CacheTable.Data[CurrentRowNumber];
                    TransformTimer.Stop();
                    return true;
                }
            }

            if (IsReaderFinished)
            {
                CurrentRow = null;
                TransformTimer.Stop();
                return false;
            }

            _currentRowCached = false;

            try
            {
                var returnRecord = await ReadRecord(cancellationToken);

                if (returnRecord != null)
                {
                    if (EncryptionMethod != EEncryptionMethod.NoEncryption)
                        EncryptRow(returnRecord);

                    CurrentRow = returnRecord;
                }
                else
                {
                    CurrentRow = null;
                    IsReaderFinished = true;
                }

            }
            // cancelled or transformexcpetion, bubble the exception up.
            catch (Exception ex) when (ex is OperationCanceledException || ex is TransformException)
            {
                IsReaderFinished = true;
                throw;
            }
            catch (Exception ex)
            {
                IsReaderFinished = true;
                throw new TransformException($"The transform {Name} failed to process record. {ex.Message}", ex);
            }


            if(IsReader && IsPrimaryTransform && _incrementalColumnIndex != -1 && CurrentRow != null)
            {
                try
                {
                    var compresult = Compare(_incrementalColumnType, CurrentRow[_incrementalColumnIndex], _maxIncrementalValue);
                    if (compresult == ECompareResult.Greater)
                    {
                        _maxIncrementalValue = CurrentRow[_incrementalColumnIndex];
                    }
                }
                catch (Exception ex)
                {
                    IsReaderFinished = true;
                    throw new TransformException($"The transform {Name} failed comparing the incremental update column.  " + ex.Message, ex);
                }

            }

			if (IsReader && !IsPrimaryTransform && !IsReaderFinished)
                TransformRowsReadReference++;

            //if this is a primary (i.e. starting reader), increment the rows read.
            if (IsReader && !IsReaderFinished) 
				TransformRowsReadPrimary++;

            //add the row to the cache
            if (CurrentRow != null && !_currentRowCached && (CacheMethod == ECacheMethod.OnDemandCache || CacheMethod == ECacheMethod.PreLoadCache))
                CacheTable.Data.Add(CurrentRow);

            TransformTimer.Stop();
            return !IsReaderFinished;
        }

        public override int FieldCount => CacheTable.Columns.Count;

        public override int GetOrdinal(string name)
        {
            return CacheTable.GetOrdinal(name);
        }

        public int GetOrdinal(TableColumn column)
		{
			var ordinal = GetOrdinal(column.TableColumnName());
			if (ordinal < 0)
			{
				ordinal = GetOrdinal(column.Name);
			}

			return ordinal;
		}

        public override string GetName(int i)
        {
            return CacheTable.Columns[i].Name;
        }

        public override object this[string name]
        {
            get
            {
                var ordinal = GetOrdinal(name);
                if (ordinal < 0)
                    throw new Exception("The column " + name + " could not be found in the table.");

                return GetValue(ordinal);
            }
        }
        public override object this[int ordinal] => GetValue(ordinal);

        public object this[TableColumn column]
        {
            get
            {
				var ordinal = GetOrdinal(column);

				if(ordinal < 0)
				{
					throw new Exception("The column " + column.TableColumnName() + " could not be found in the table.");
				}
					
                return this[ordinal];
            }
        }

        public override int Depth => throw new NotImplementedException();

        public override bool IsClosed => PrimaryTransform?.IsClosed??!IsReaderFinished;

        public override int RecordsAffected => throw new NotImplementedException();

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
            return GetValue(i)?.ToString()??"";
        }

        public override object GetValue(int i)
        {
            if (i < CurrentRow.Length)
            {
                return CurrentRow[i];
            }

            throw new ArgumentOutOfRangeException(
                $"The GetValue failed as the column at position {i} was greater than the number of columns {CurrentRow.Length}.");
        }

        public override int GetValues(object[] values)
        {
            if (values.Length > CurrentRow.Length)
            {
                throw new Exception("Could not GetValues as the input array was length " + values.Length +
                                    " which is greater than the current number of fields " +
                                    CurrentRow.Length + ".");
            }

            for (var i = 0; i < values.GetLength(0); i++)
            {
                values[i] = CurrentRow[i];
            }

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
                PrimaryTransform?.Dispose();
                ReferenceTransform?.Dispose();

                Reset();
            }
            base.Dispose(disposing);
        }

        public override bool NextResult()
        {
            return Read();
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException("This feature is not currently implemented.");
        }

        public override bool HasRows => PrimaryTransform?.HasRows??IsReaderFinished;

#if NET462
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
    }

#else

    }

    public static class TransformExtensions
    {
        public static bool CanGetColumnSchema(this Transform reader)
        {
            return true;
        }

        public static ReadOnlyCollection<DbColumn> GetColumnSchema(this Transform reader)
        {
            var transform = reader;

            var columnSchema = new List<DbColumn>();

            var ordinal = 0;
            foreach(var col in transform.CacheTable.Columns)
            {
                var column = new TransformColumn(
                    col.AllowDbNull,
                    "",
                    col.Name,
                    "",
                    "",
                    transform.CacheTable.Name,
                    col.Name,
                    ordinal,
                    col.MaxLength > 0 ? col.MaxLength : int.MaxValue,
                    Dexih.Utils.DataType.DataType.GetType(col.DataType),
                    col.DataType.ToString(),
                    false,
                    false,
                    false,
                    false,
                    false,
                    col.DeltaType == EDeltaType.SurrogateKey,
                    col.DataType == ETypeCode.Int64,
                    false,
                    col.DeltaType == EDeltaType.SurrogateKey,
                    col.Precision,
                    col.Scale
                    );
                columnSchema.Add(column);
            }

            return new ReadOnlyCollection<DbColumn>(columnSchema);
        }
    }

    public class TransformColumn : DbColumn
    {
        public TransformColumn(
                    bool? allowDbNull,
        string baseCatalogName,
        string baseColumnName ,
        string baseSchemaName ,
        string baseServerName ,
        string baseTableName ,
        string columnName ,
        int? columnOrdinal ,
        int? columnSize ,
        Type dataType ,
        string dataTypeName ,
        bool? isAliased ,
        bool? isAutoIncrement ,
        bool? isExpression ,
        bool? isHidden ,
        bool? isIdentity ,
        bool? isKey ,
        bool? isLong ,
        bool? isReadOnly ,
        bool? isUnique ,
        int? numericPrecision ,
        int? numericScale 
            )
        {
            AllowDBNull = allowDbNull;
            BaseCatalogName = baseCatalogName;
            BaseColumnName = baseColumnName;
            BaseSchemaName = baseSchemaName;
            BaseServerName = baseServerName;
            BaseTableName = baseTableName;
            ColumnName = columnName;
            ColumnOrdinal = columnOrdinal;
            ColumnSize = columnSize;
            DataType = dataType;
            DataTypeName = dataTypeName;
            IsAliased = isAliased;
            IsAutoIncrement = isAutoIncrement;
            IsExpression = isExpression;
            IsHidden = isHidden;
            IsIdentity = isIdentity;
            IsKey = isKey;
            IsLong = isLong;
            IsReadOnly = isReadOnly;
            IsUnique = isUnique;
            NumericPrecision = numericPrecision;
            NumericScale = numericScale;
        }
    }
#endif
    #endregion


}
