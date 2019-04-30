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
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.Crypto;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
using Newtonsoft.Json.Linq;

namespace dexih.transforms
{

    /// <summary>
    /// Transform is the abstract class which all other transforms and connection should implement
    /// </summary>
    public abstract class Transform : DbDataReader
    {
        #region Events
        
        // tracks rows read
        public delegate void ReaderProgress(long rows);
        public event ReaderProgress OnReaderProgress;
        
        #endregion
        
        #region Enums
        public enum ECacheMethod
        {
            NoCache = 0,
            DemandCache = 1,
            // PreLoadCache = 2,
            LookupCache = 3
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

        protected long AuditKey { get; set; }

        public long MaxInputRows { get; set; }
        public long MaxOutputRows { get; set; }

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
        public long TransformRows { get; protected set; }

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

        public virtual long AutoIncrementValue => PrimaryTransform?.AutoIncrementValue ?? 0;

        #endregion

        #region Abstract Properties

        public abstract string TransformName { get; }
        public abstract string TransformDetails { get; }

        protected abstract Task<object[]> ReadRecord(CancellationToken cancellationToken = default);
        public abstract bool ResetTransform();

        #endregion

        #region Initialization

        /// <summary>
        /// Sets the data readers for the transform.  Ensure the transform properties have been set prior to running this.
        /// </summary>
        /// <param name="primaryTransform">The primary input transform</param>
        /// <param name="referenceTransform">The secondary input, such as join table, target table, lookup table etc.</param>
        /// <param name="mapAllReferenceColumns"></param>
        /// <returns></returns>
        public bool SetInTransform(Transform primaryTransform, Transform referenceTransform = null, bool mapAllReferenceColumns = true)
        {
            PrimaryTransform = primaryTransform;
            ReferenceTransform = referenceTransform;

            Reset(false, false);

//            if (Mappings == null)
//            {
//                Mappings = new Mappings();
//            }

            // if the primary transform has a higher output rows setting than the input rows setting, then pass it down.
            if (PrimaryTransform != null && (PrimaryTransform.MaxOutputRows == 0 || PrimaryTransform.MaxOutputRows > MaxInputRows)) PrimaryTransform.MaxOutputRows = MaxInputRows;

            CacheTable = InitializeCacheTable(mapAllReferenceColumns); // Mappings.Initialize(PrimaryTransform?.CacheTable, ReferenceTransform?.CacheTable, ReferenceTransform?.ReferenceTableAlias, mapAllReferenceColumns);

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

            
            //IsReader indicates if this is a base transform.
            IsReader = primaryTransform == null;
            if (primaryTransform != null)
                primaryTransform.IsPrimaryTransform = true;
            if (referenceTransform != null)
                referenceTransform.IsPrimaryTransform = false;
           
            return true;
        }

        /// <summary>
        /// This adjusts the node level which the transform should be applied to
        /// </summary>
        /// <param name="primaryTransform">The primary transform containing inbound data.</param>
        /// <param name="referenceTransform">The reference transform (for joins and concat).</param>
        /// <param name="baseMappings">The mappings to be applied to the transform.</param>
        /// <param name="columnPath">Array of columns which lead to the node</param>
        /// <param name="pathNumber">Used to recurse through the columnPath</param>
        /// <returns></returns>
        public Transform CreateNodeMapping(Transform primaryTransform, Transform referenceTransform, Mappings baseMappings, TableColumn[] columnPath, int pathNumber = 0)
        {
            var column = columnPath[pathNumber];
            var table = primaryTransform.CacheTable;
            var nodeColumn = table[column.Name, column.ColumnGroup];
            // create a new node mapping
            var mapNode = new MapNode(nodeColumn, table);
            var nodeTransform = mapNode.Transform;

            // if we are at the last node, then this is where the base mappings need to be applied to.
            if (columnPath.Length -1 == pathNumber)
            {
                // set the transform mappings
                Mappings = baseMappings;

                // set the transform mappings, using the transform from the new node
                SetInTransform(nodeTransform, referenceTransform);

                // the mapNode output transform contains 
                mapNode.OutputTransform = this;

            }
            else
            {
                var childTable = new Table("Node");
                foreach (var childColumn in nodeColumn.ChildColumns)
                {
                    childTable.Columns.Add(childColumn);
                }
                
                foreach (var childColumn in table.Columns)
                {
                    childTable.Columns.Add(childColumn);
                }
                
                var childTransform = CreateNodeMapping(nodeTransform, referenceTransform, baseMappings, columnPath, pathNumber + 1);
                mapNode.OutputTransform = childTransform;
            }

            // create a mapping transform which maps node.
            var transform = new TransformMapping();
            var nodeMappings = new Mappings {mapNode};
            transform.Mappings = nodeMappings;
            transform.SetInTransform(primaryTransform);

            return transform;

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
        /// Initializes the CacheTable.  This can be overridden for transforms which have
        /// different column outputs.
        /// </summary>
        /// <param name="mapAllReferenceColumns"></param>
        /// <returns></returns>
        protected virtual Table InitializeCacheTable(bool mapAllReferenceColumns)
        {
            if (Mappings == null)
            {
                return PrimaryTransform.CacheTable.Copy();
            }

            return Mappings.Initialize(PrimaryTransform?.CacheTable, ReferenceTransform?.CacheTable, ReferenceTransform?.ReferenceTableAlias, mapAllReferenceColumns);
        }

        public Task<bool> Open(CancellationToken cancellationToken = default)
        {
            return Open(0, null, cancellationToken);
        }

        public Task<bool> Open(SelectQuery selectQuery, CancellationToken cancellationToken = default)
        {
            return Open(0, selectQuery, cancellationToken);
        }

        /// <summary>
        /// Opens underlying connections passing sort and filter requests through.
        /// </summary>
        /// <param name="auditKey"></param>
        /// <param name="selectQuery">Query to apply (note only filters are used)</param>
        /// <param name="cancellationToken"></param>
        /// <returns>True is successful, False is unsuccessful.</returns>
        public virtual async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            var primaryOpen = PrimaryTransform != null ? PrimaryTransform.Open(auditKey, selectQuery, cancellationToken) : Task.FromResult(true);
            var referenceOpen = ReferenceTransform != null ? ReferenceTransform.Open(auditKey, selectQuery, cancellationToken) : Task.FromResult(true);

            var result = await primaryOpen && await referenceOpen;
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
        public Table CacheTable { get; protected set; }

        /// <summary>
        /// Indicates if the cache is complete or at maximum capacity
        /// </summary>
        public bool IsCacheFull { get; protected set; }
        
        /// <summary>
        /// Gets a transform instance that can be used by a different thread to simultaneously read the same transform.
        /// </summary>
        /// <returns></returns>
        public Transform GetThread()
        {
            if (CacheMethod == ECacheMethod.NoCache)
            {
                CacheMethod = ECacheMethod.DemandCache;
            }

            return new TransformThread(this);
        }

        private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);

        public async Task<object[]> ReadThreadSafe(int row, CancellationToken cancellationToken = default)
        {
            // wait for any other threads to read data.
            await _semaphoreSlim.WaitAsync(cancellationToken);

            try
            {

                while (row >= CacheTable.Data.Count)
                {

                    var result = await ReadAsync(cancellationToken);
                    if (!result) return null;
                }
            }
            finally
            {
                _semaphoreSlim.Release();
            }

            return row > CacheTable.Data.Count ? null : CacheTable.Data[row];
        }

        public Transform GetSourceReader()
        {
            if (IsReader)
            {
                return this;
            }
            else
            {
                return PrimaryTransform;
            }
        }

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

        private void SetNodes(object[] row)
        {
            for (var i = 0; i < CacheTable.Columns.Count; i++)
            {
                var column = CacheTable.Columns[i];
                if (!column.IsParent && column.DataType == ETypeCode.Node)
                {
                    if (row[i] == null) continue;
                    
                    var transform = (Transform) row[i];

                    if (transform is TransformNode transformNode)
                    {
                        transformNode.SetParentTable(CacheTable);
                        transformNode.SetParentRow(row);
                    }
                    else
                    {
                        var newNode = new TransformNode {PrimaryTransform = transform};
                        newNode.SetTable(transform.CacheTable, CacheTable);
                        newNode.SetParentRow(row);
                        newNode.Open().Wait();
                        row[i] = newNode;
                    }
                }
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


        public List<TransformPerformance> PerformanceSummary()
        {
            List<TransformPerformance> performance;

            if (PrimaryTransform != null)
            {
                performance = PrimaryTransform.PerformanceSummary();
            }
            else
            {
                performance = new List<TransformPerformance>();
            }

            var timeSpan = TransformTimerTicks();

            var item = new TransformPerformance(TransformName + "(" + Name + ")", TransformDetails,
                TotalRowsReadPrimary, timeSpan.TotalSeconds);

            if (ReferenceTransform != null)
            {
                var childPerformance = ReferenceTransform.PerformanceSummary();
                item.Children = childPerformance;
            }

            performance.Add(item);
            
            return performance.ToList();
        }

        /// <summary>
        /// Gets a string containing details of the transform performance.
        /// </summary>
        /// <returns></returns>
        public string PerformanceDetails()
        {
            var performanceSummary = PerformanceSummary();
            var value = new StringBuilder();

            void WritePerformance(List<TransformPerformance> performance, int depth)
            {
                foreach (var item in performance)
                {
                    value.AppendLine($"{new string('\t', depth)} Transform: {item.TransformName}, Action: {item.Action}, Rows: {item.Rows}, Seconds: {item.Seconds}");

                    if (item.Children != null && item.Children.Count > 0)
                    {
                        WritePerformance(item.Children, depth+1);
                    }
                }
            }

            WritePerformance(performanceSummary, 0);

            return value.ToString();
        }

        #endregion

        #region Record Navigation

        /// <summary>
        /// Indicates if the source reader has completed, without moving to the next record.
        /// </summary>
        public bool IsReaderFinished { get; protected set; }

        private bool _isResetting = false; //flag to indicate reset is underway.
        public object[] CurrentRow { get; private set; } //stores data for the current row.
        
        private bool _currentRowCached;
        protected int CurrentRowNumber = -1; //current row number

        /// <summary>
        /// Resets the transform and any source transforms.
        /// </summary>
        /// <returns></returns>
        public bool Reset(bool resetCache = false, bool resetIsOpen = true)
        {
            if (!_isResetting) //stops recursive loops where intertwined transforms are resetting each other
            {
                _isResetting = true;

                _isFirstRead = true;
                IsCacheFull = false;
                IsReaderFinished = false;
                if (resetCache)
                {
                    CacheTable?.Data.Clear();
                    _lookupCache?.Clear();
                }

                CurrentRowNumber = -1;

                //reset stats.
                TransformRowsSorted = 0;
                TransformRowsFiltered = 0;
                TransformRowsIgnored = 0;
                TransformRowsPreserved = 0;
                TransformRowsReadPrimary = 0;
                TransformRowsReadReference = 0;
                TransformRowsRejected = 0;

                var returnValue = ResetTransform();

                if (PrimaryTransform != null)
                {
                    returnValue = returnValue && PrimaryTransform.Reset(resetCache, resetIsOpen);
                }

                if (ReferenceTransform != null)
                {
                    returnValue = returnValue && ReferenceTransform.Reset(resetCache, resetIsOpen);
                }

                if(resetIsOpen) IsOpen = true;
//                IsReaderFinished = false;

                _isResetting = false;
                return returnValue;
            }

            return false;
        }

        //Set the reader to a specific row.  If the rows has exceeded MaxRows this will only start from the beginning of the cache.   A read() is required following this to get data.
        public void SetRowNumber(int rowNumber = 0)
        {
            if (rowNumber <= CacheTable.Data.Count)
            {
                CurrentRowNumber = rowNumber - 1;
            }
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
        /// <param name="query"></param>
        /// <param name="duplicateStrategy">Action to take when duplicate rows are returned.</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual async Task<IEnumerable<object[]>> Lookup(SelectQuery query, EDuplicateStrategy duplicateStrategy, CancellationToken cancellationToken = default)
        {

            // if the query has already been used, use the cache.
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
                    var rows = 0;
                    var maxRows = query?.Rows ?? -1;
                    while ((maxRows <= 0 || rows < maxRows) && await ReadAsync(cancellationToken))
                    {
                        lookupResult.Add(CurrentRow);
                        rows++;
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

            if (CacheMethod == ECacheMethod.LookupCache)
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
//                            CacheTable.AddRowRange(lookupResult);
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

        public async Task<JObject> LookupJson(SelectQuery query, EDuplicateStrategy duplicateStrategy,
            CancellationToken cancellationToken = default)
        {
            var rows = await Lookup(query, duplicateStrategy, cancellationToken);

            var jObject = new JObject();
            var jArray = new JArray();

            foreach (var row in rows)
            {
                var jRow = new JObject();
                foreach (var column in CacheTable.Columns)
                {
                    jRow.Add(column.Name, JToken.FromObject(row[GetOrdinal(column.Name)]));
                }

                jArray.Add(jRow);
            }

            jObject.Add("Success", true);
            jObject.Add(new JProperty("Data", jArray));
            return jObject;
        }

        /// <summary>
        /// Recurses through the transforms to the primary transform, and sets the lookup filters.
        /// </summary>
        /// <param name="auditKey"></param>
        /// <param name="filters"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual async Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            if (!IsOpen)
            {
                Reset();
                await Open(auditKey, query, cancellationToken);
            }

            Mappings?.SetInputColumns(query.InputColumns);

            if (PrimaryTransform == null)
            {
                // update the input value on any input columns.
                foreach (var column in CacheTable.Columns.Where(c => c.IsInput))
                {
                    var inputColumn = query.InputColumns?.SingleOrDefault(c => c.Name == column.Name);
                    if (inputColumn != null)
                    {
                        column.DefaultValue = inputColumn.DefaultValue;
                    }
                    else
                    {
                        var filter = query.Filters.FirstOrDefault(c =>
                            c.Column1.Name == column.Name && c.Operator == Filter.ECompare.IsEqual);
                        if (filter != null)
                        {
                            column.DefaultValue = filter.Value2;
                        }
                    }
                }
                
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
//        public virtual Task<ICollection<object[]>> LookupRowDirect(ICollection<Filter> filters, EDuplicateStrategy duplicateStrategy, CancellationToken cancellationToken = default)
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
        
        private bool _nextReadInProgress = false;
        private object[] _nextRow;

        /// <summary>
        /// Starts a read process from the underlying transforms, but does not update the current row.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        /// <exception cref="TransformException"></exception>
        public async Task<bool> ReadPrepareAsync(CancellationToken cancellationToken = default)
        {
            _nextReadInProgress = true;

            if (cancellationToken.IsCancellationRequested)
            {
                throw new TaskCanceledException();
            }

            try
            {

                //starts  a timer that can be used to measure downstream transform and database performance.
                TransformTimer.Start();

                _nextRow = null;

                if (_isFirstRead)
                {
                    if (Mappings != null)
                    {
                        await Mappings.Open();
                    }

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
                            throw new Exception(
                                "Cannot run the transform as two columns have been defined with incremental update flags.");
                        }
                        else
                            _incrementalColumnIndex = -1;
                    }

                    _isFirstRead = false;
                    IsReaderFinished = false;
                }

                CurrentRowNumber++;

                //check cache for a row first.
                if (CacheMethod == ECacheMethod.DemandCache)
                {
                    if (CurrentRowNumber < CacheTable.Data.Count)
                    {
                        _nextRow = CacheTable.Data[CurrentRowNumber];
                        _currentRowCached = true;
                        TransformTimer.Stop();
                        return true;
                    }
                }

                if (IsReaderFinished)
                {
                    TransformTimer.Stop();
                    _nextReadInProgress = false;
                    return false;
                }

                if (!IsOpen)
                {
                    throw new TransformException("The read operation failed as the transform is not open.");
                }

                _currentRowCached = false;

                try
                {
                    var returnRecord = await ReadRecord(cancellationToken);

                    if (returnRecord != null)
                    {
                        if (EncryptionMethod != EEncryptionMethod.NoEncryption)
                            EncryptRow(returnRecord);

                        SetNodes(returnRecord);

                        _nextRow = returnRecord;

                    }
                }
                // cancelled or transform exception, bubble the exception up.
                catch (Exception ex) when (ex is OperationCanceledException || ex is TransformException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new TransformException($"The transform {Name} failed to process record. {ex.Message}", ex);
                }


                if (IsReader && IsPrimaryTransform && _incrementalColumnIndex != -1 && _nextRow != null)
                {
                    try
                    {
                        var compareResult = Operations.GreaterThan(_incrementalColumnType,
                            _nextRow[_incrementalColumnIndex], _maxIncrementalValue);
                        if (compareResult)
                        {
                            _maxIncrementalValue = _nextRow[_incrementalColumnIndex];
                        }
                    }
                    catch (Exception ex)
                    {
                        Close();
                        throw new TransformException(
                            $"The transform {Name} failed comparing the incremental update column.  " + ex.Message, ex);
                    }
                }
            }
            finally
            {
                if (_nextRow == null || (MaxOutputRows > 0 && TransformRows >= MaxOutputRows))
                {
                    Close();
                }
                else
                {
                    TransformRows++;
                }

                if (IsReader && !IsPrimaryTransform && !IsReaderFinished)
                    TransformRowsReadReference++;

                //if this is a primary (i.e. starting reader), increment the rows read.
                if (IsReader && !IsReaderFinished)
                {
                    TransformRowsReadPrimary++;
                    OnReaderProgress?.Invoke(TransformRowsReadPrimary);
                }

                //add the row to the cache
                if (_nextRow != null && !_currentRowCached &&
                    (CacheMethod == ECacheMethod.DemandCache))
                    CacheTable.AddRow(_nextRow);

                TransformTimer.Stop();
                _nextReadInProgress = false;
            }

            return !IsReaderFinished;
        }

        /// <summary>
        /// Applies the prepared row to the current row.  Must call ReadPrepareAsync first.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="TransformException"></exception>
        public void ReadApply()
        {
            if (_nextReadInProgress)
            {
                throw new TransformException("The transform read could not be applied as there is still a ReadPrepareAsync process underway.");
            }

            CurrentRow = _nextRow;
        }

        public override bool Read()
        {
            return ReadAsync(CancellationToken.None).Result;
        }

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            var returnValue = await ReadPrepareAsync(cancellationToken);
            ReadApply();
            return returnValue;
        }

        /// <summary>
        /// Loads the incoming dataset into the transform cache, and resets the reader to the first row.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<Transform> CreateCachedTransform(CancellationToken cancellationToken = default)
        {
            var transform = new TransformCache(this);
            await transform.Open(AuditKey, null, cancellationToken);
            return transform;
        }

        /// <summary>
        /// Create a table with the data populated with the reader.  Transform should be open, and on first record.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<Table> CreateTableData(CancellationToken cancellationToken = default)
        {
            var table = CacheTable.Copy();

            while (await ReadAsync(cancellationToken))
            {
                table.AddRow(CurrentRow);
            }

            return table;
        }

        public override int FieldCount => CacheTable?.Columns.Count ?? -1;

        /// <summary>
        /// Gets the Field count excluding any columns from parent nodes.
        /// </summary>
        public int BaseFieldCount => CacheTable?.Columns.Count(c => !c.IsParent) ?? -1;

        /// <summary>
        /// Gets the current row excluding any columns from the parent nodes.
        /// </summary>
        /// <returns></returns>
        public object[] GetBaseCurrentRow()
        {
            if (CurrentRow == null)
            {
                return null;
            }

            var row = new object[BaseFieldCount];
            var baseCount = 0;
            for (var count = 0; count < FieldCount; count++)
            {
                if (!CacheTable.Columns[count].IsParent)
                {
                    row[baseCount++] = CurrentRow[count];
                }
            }

            return row;
        }


        /// <summary>
        /// Gets a table which excludes any column from parent nodes.
        /// </summary>
        /// <returns></returns>
        public Table GetBaseTable()
        {
            var baseFieldCount = BaseFieldCount;
            if (baseFieldCount == 0 || baseFieldCount == FieldCount) return CacheTable;

            var table = new Table(CacheTable.Name)
            {
                LogicalName = CacheTable.LogicalName
            };
            
            foreach (var column in CacheTable.Columns.Where(c => !c.IsParent))
            {
                table.Columns.Add(column);
            }

            return table;
        }

        public JObject GetRow()
        {
            var jObject = new JObject();
            foreach (var column in CacheTable.Columns)
            {
                jObject.Add(column.Name, JToken.FromObject(GetValue(column.Name)));
            }

            return jObject;
        }

        public override int GetOrdinal(string name) => CacheTable.GetOrdinal(name);

        public int GetOrdinal(EDeltaType deltaType) => CacheTable.GetOrdinal(deltaType);

        public int GetOrdinal(TableColumn column) => CacheTable.GetOrdinal(column);
        public int GetAutoIncrementOrdinal() => CacheTable.GetAutoIncrementOrdinal();

        public override string GetName(int i) => CacheTable.Columns[i].Name;

        // public object this[EDeltaType deltaType] => GetValue(deltaType);

        public override object this[string name] => GetValue(name);
        
        public override object this[int ordinal] => GetValue(ordinal);

        public object this[TableColumn column] => GetValue(column);

        public override int Depth => throw new NotImplementedException();

        public bool IsOpen { get; protected set; } = false;

        public override bool IsClosed => PrimaryTransform?.IsClosed??!IsReaderFinished;

        public override int RecordsAffected => throw new NotImplementedException();

        public override bool GetBoolean(int i)
        {
            return GetValue<bool>(i);
        }
        public override byte GetByte(int i)
        {
            return GetValue<byte>(i);
        }
        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException("GetBytes is not supported.");
        }
        public override char GetChar(int i)
        {
            return GetValue<char>(i);
        }
        public override long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException("GetChars is not supported.");
        }

        public override string GetDataTypeName(int i)
        {
            return GetValue(i)?.GetType().Name;
        }
        public override DateTime GetDateTime(int i)
        {
            return GetValue<DateTime>(i);
        }
        public override decimal GetDecimal(int i)
        {
            return GetValue<decimal>(i);
        }
        public override double GetDouble(int i)
        {
            return GetValue<double>(i);
        }
        public override Type GetFieldType(int i)
        {
            return GetValue(i).GetType();
        }
        public override float GetFloat(int i)
        {
            return GetValue<float>(i);
        }
        public override Guid GetGuid(int i)
        {
            return GetValue<Guid>(i);
        }
        public override short GetInt16(int i)
        {
            return GetValue<short>(i);
        }
        public override int GetInt32(int i)
        {
            return GetValue<int>(i);
        }
        public override long GetInt64(int i)
        {
            return GetValue<long>(i);
        }

        public override string GetString(int i)
        {
            return GetValue(i)?.ToString()??"";
        }

        public override object GetValue(int i)
        {
            if (CurrentRow == null)
            {
                throw new ArgumentOutOfRangeException(
                    $"Fails to get the value at position {i} as there is no current row available.");
            }

            if (i >= CurrentRow.Length)
            {
                throw new ArgumentOutOfRangeException(
                $"The GetValue failed as the column at position {i} was greater than the number of columns {CurrentRow?.Length}.");
            }

            return CurrentRow[i];            
        }
        
        
        /// <summary>
        /// Gets the value at the ordinal.
        /// Returns the defaultValue if the ordinal is outside the row range or is null
        /// </summary>
        /// <param name="i"></param>
        /// <param name="defaultValue"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T GetValue<T>(int i, T defaultValue = default)
        {
            var value = GetValue(i);

            if (value is null)
            {
                return defaultValue;
            }

            if (value is T tValue)
            {
                return tValue;
            }

            return Operations.Parse<T>(value);
        }

        public override int GetValues(object[] values)
        {
            if (CurrentRow == null)
            {
                throw new Exception("Could not GetValues as there is no current row.");
            }

            if (values.Length > CurrentRow.Length)
            {
                throw new Exception("Could not GetValues as the input array was length " + values.Length +
                                    " which is greater than the current number of fields " +
                                    CurrentRow.Length + ".");
            }

            for (var i = 0; i < values.GetLength(0); i++)
            {
                values[i] = GetValue(i);
            }

            return values.GetLength(0);
        }

        public object GetValue(TableColumn column)
        {
            var ordinal = GetOrdinal(column);

            if(ordinal < 0)
            {
                throw new Exception("The column " + column.TableColumnName() + " could not be found in the table.");
            }
					
            return this[ordinal];
        }

        public object GetValue(string name)
        {
            var ordinal = GetOrdinal(name);
            if (ordinal < 0)
                throw new Exception("The column " + name + " could not be found in the table.");

            return GetValue(ordinal);
        }

        public T GetValue<T>(string name, T defaultValue = default)
        {
            var ordinal = GetOrdinal(name);
            return GetValue<T>(ordinal, defaultValue);
        }

        public object GetValue(EDeltaType deltaType)
        {
            var ordinal = GetOrdinal(deltaType);
            return ordinal >= 0 ? GetValue(ordinal) : null;
        }

        public T GetValue<T>(EDeltaType deltaType, T defaultValue = default)
        {
            var ordinal = GetOrdinal(deltaType);
            return GetValue<T>(ordinal, defaultValue);
        }

        public long GetAutoIncrementValue()
        {
            var ordinal = GetAutoIncrementOrdinal();
            return ordinal >= 0 ? GetValue(ordinal, 0) : 0;
        }

        public DeltaValues GetDeltaValues()
        {
            T GetValueOrNull<T>(EDeltaType deltaType, T defaultValue = default)
            {
                var ordinal = GetOrdinal(deltaType);
                if (ordinal < 0 || CurrentRow == null) return defaultValue;
                return GetValue<T>(ordinal);
            }

            var deltaValues = new DeltaValues(
                GetValueOrNull(EDeltaType.DatabaseOperation, 'C'),
                GetAutoIncrementValue(),
                GetValueOrNull(EDeltaType.IsCurrentField, true),
                GetValueOrNull<DateTime>(EDeltaType.CreateDate),
                GetValueOrNull<DateTime>(EDeltaType.UpdateDate),
                GetValueOrNull<DateTime>(EDeltaType.ValidFromDate),
                GetValueOrNull<DateTime>(EDeltaType.ValidToDate),
                GetValueOrNull<long>(EDeltaType.CreateAuditKey),
                GetValueOrNull<long>(EDeltaType.UpdateAuditKey),
                GetValueOrNull<int>(EDeltaType.Version)
            );

            return deltaValues;
        }


        public override bool IsDBNull(int i)
        {
            return GetValue(i) is DBNull;
        }

        public override void Close()
        {
            if (!IsReaderFinished && IsOpen)
            {
                PrimaryTransform?.Close();
                ReferenceTransform?.Close();
                CloseConnections();
                IsReaderFinished = true;
                IsOpen = false;
            }

            // Reset();
        }

        protected virtual void CloseConnections()
        {
        }

        public override bool NextResult()
        {
            return Read();
        }

        public override IEnumerator GetEnumerator()
        {
            return new TransformEnumerator(this);
        }
        
        public class TransformEnumerator : IEnumerator<object[]>
        {
            private readonly Transform _transform;
            
            public TransformEnumerator(Transform transform)
            {
                _transform = transform.GetThread();
                _transform.Open().Wait();
            }

            public bool MoveNext()
            {
                return _transform.Read();
            }

            public void Reset()
            {
                _transform.Reset();
            }

            public object[] Current => _transform.GetBaseCurrentRow();

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                _transform.Close();
            }
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
                    col.IsAutoIncrement(),
                    col.DataType == ETypeCode.Int64,
                    false,
                    col.IsAutoIncrement(),
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
