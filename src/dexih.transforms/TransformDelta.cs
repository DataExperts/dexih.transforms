using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    
    [Transform(
        Name = "Delta",
        Description = "Compare incoming data against the target table to produce a delta.",
        TransformType = TransformAttribute.ETransformType.Delta
    )]
    public sealed class TransformDelta : Transform
    {
        public TransformDelta(Transform inReader, Transform referenceTransform, EUpdateStrategy deltaType, long autoIncrementValue, bool addDefaultRow, DeltaValues parentDelta = default)
        {
            DeltaType = deltaType;
            _autoIncrementValue = autoIncrementValue;
            _parentDelta = parentDelta;

            AddDefaultRow = addDefaultRow;

            DoUpdate = false;
            DoDelete = false;
            DoPreserve = false;

            if (deltaType == EUpdateStrategy.AppendUpdate || deltaType == EUpdateStrategy.AppendUpdateDelete || deltaType == EUpdateStrategy.AppendUpdateDeletePreserve || deltaType == EUpdateStrategy.AppendUpdatePreserve)
                DoUpdate = true;

            if (deltaType == EUpdateStrategy.AppendUpdateDelete || deltaType == EUpdateStrategy.AppendUpdateDeletePreserve)
                DoDelete = true;

            if (deltaType == EUpdateStrategy.AppendUpdateDeletePreserve || deltaType == EUpdateStrategy.AppendUpdatePreserve)
                DoPreserve = true;

            // Mappings = new Mappings();

            SetInTransform(inReader, referenceTransform, false);

            foreach (var column in referenceTransform.CacheTable.Columns)
            {
                if (CacheTable.TryGetColumn(column.Name, out var newColumn))
                {
                    column.CopyProperties(newColumn);
                }
            }
        }

        [JsonConverter(typeof (StringEnumConverter))]
        public enum EUpdateStrategy
        {
            Reload, //truncates the table and reloads
            Append, //inserts records.  use if the data feed is always new data.
            AppendUpdate, //inserts new records, and updates records.  use if the data feed is new and existing data.
            AppendUpdateDelete, //inserts new records, updates existing records, and (logically) deletes removed records.  use to maintain an exact copy of the data feed.
            AppendUpdatePreserve, //inserts new records, updates existing records, and preserves the changes.
            AppendUpdateDeletePreserve // inserts new records, updates existing records, (logically) deletes removed records.
        }

        private readonly DateTime _defaultValidFromDate = new DateTime(1900, 01, 01);
        private readonly DateTime _defaultValidToDate = new DateTime(2099, 12, 31, 23, 59, 59);

        private readonly DeltaValues _parentDelta;
        private bool _firstRead;
        private bool _truncateComplete;
        private bool _defaultRowAdded;
        private bool _referenceOpen;
        private bool _primaryOpen;

        private TableColumn _colValidFrom;
        private TableColumn _colCreateDate;
        private TableColumn _colAutoIncrement;
        private TableColumn _colIsCurrentField;
        private TableColumn _colCreateAuditKey;

        //preload ordinals to improve performance.
        private int _sourceOperationOrdinal;
        private int _rejectedReasonOrdinal;
        private int _validationStatusOrdinal;
        private int _sourceSurrogateKeyOrdinal;
        private int _validFromOrdinal;
        private int _validToOrdinal;
        private int _isCurrentOrdinal;
        private int _versionOrdinal;

        private int _sourceValidFromOrdinal;
        private int _sourceValidToOrdinal;
        private int _sourceIsCurrentOrdinal;

        private int _referenceSurrogateKeyOrdinal;
        private int _referenceIsValidOrdinal;
        private int _referenceCreateAuditOrdinal;
        private int _referenceCreateDateOrdinal;
        private int _referenceValidToOrdinal;
        private int _referenceValidFromOrdinal;

        private List<(int ordinal, int primaryOrdinal, int referenceOrdinal)> _naturalKeyOrdinals; // contains ordinals to map source ordinal to target ordinal for all natural keys.
        private List<(int ordinal, int primaryOrdinal, int referenceOrdinal)> _trackingOrdinals; // contains ordinals to map source ordinal to target ordinal for all tracking fields.

        private int _columnCount;

        private long _autoIncrementValue;

        private EUpdateStrategy DeltaType { get; set; }
        public override long AutoIncrementValue => _autoIncrementValue;
        public bool AddDefaultRow { get; set; }
        

        private bool DoUpdate { get; set; }
        private bool DoDelete { get; set; }
        private bool DoPreserve { get; set; }

        private object[] PreserveRow { get; set; }
        private object[] NextPrimaryRow { get; set; }

        private List<int> _sourceOrdinals;

        private DateTime _currentDateTime;

        public override string TransformName { get; } = "Delta";
        public override string TransformDetails => $"{DeltaType} on {ReferenceTransform.Name}";

        protected override Table InitializeCacheTable(bool mapAllReferenceColumns)
        {
            // var table = Mappings.Initialize(PrimaryTransform.CacheTable, ReferenceTransform?.CacheTable, ReferenceTransform?.ReferenceTableAlias, mapAllReferenceColumns);

            var table = ReferenceTransform.CacheTable.Copy();

            //add the operation type, which indicates whether record are C-create/U-update or D-Deletes
            if (table.Columns.SingleOrDefault(c => c.DeltaType == TableColumn.EDeltaType.DatabaseOperation) == null)
            {
                table.Columns.Insert(0, new TableColumn("Operation", ETypeCode.Char)
                {
                    DeltaType = TableColumn.EDeltaType.DatabaseOperation
                });
            }
            
            // add any node columns
            foreach (var column in PrimaryTransform.CacheTable.Columns.Where(c => c.DataType == ETypeCode.Node))
            {
                if (table.Columns.GetOrdinal(column) == -1)
                {
                    var newColumn = column.Copy();
                    table.Columns.Add(newColumn);    
                }
            }
            
            table.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;
            return table;
        }

        
        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;
            bool returnValue;

            if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
            {
                returnValue = await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);
            }
            else
            {
                if (selectQuery == null)
                    selectQuery = new SelectQuery();

                selectQuery.Sorts = RequiredSortFields();

                returnValue = await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);
            }

            if (ReferenceTransform == null)
                throw new Exception("There must be a target table specified.");

            

            //get the available audit columns
            SetAuditColumns();

            _firstRead = true;
            _defaultRowAdded = false;
            _truncateComplete = false;
            _primaryOpen = true;
            _referenceOpen = true;

            //do some integrity checks
            if (DoPreserve && _colAutoIncrement == null)
                throw new Exception("The delta transform requires the table to have a single surrogate key field for row preservation to be possible.");

            if (DoUpdate && CacheTable.Columns.All(c => c.DeltaType != TableColumn.EDeltaType.NaturalKey))
                throw new Exception("The delta transform requires the table to have at least one natrual key field for updates to be possible.");

//            //set surrogate key to the key field.  This will indicate that the surrogate key should be used when update/deleting records.
//            if(_colSurrogateKey != null)
//                CacheTable.KeyFields = new List<string>() { _colSurrogateKey.Name };

            //preload the source-target ordinal mapping to improve performance.
            _sourceOrdinals = new List<int>();
            var columnCount = CacheTable.Columns.Count;
            for (var referenceOrdinal = 1; referenceOrdinal < columnCount; referenceOrdinal++)
            {
                _sourceOrdinals.Add(PrimaryTransform.GetOrdinal(CacheTable.Columns[referenceOrdinal]));
            }
            
            return returnValue;
        }

        public override bool RequiresSort
        {            
           get
           {
               //if detla is load or reload, we don't need any filter/sorts
                if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
                {
                    return false;
                }
               return true;
           }
        }

        private void SetAuditColumns()
        {
            //add the audit columns if they don't exist
            //get some of the key fields to save looking up for each row.
            _colValidFrom = CacheTable.GetColumn(TableColumn.EDeltaType.ValidFromDate);
            _colCreateDate = CacheTable.GetColumn(TableColumn.EDeltaType.CreateDate);
            _colAutoIncrement = CacheTable.GetAutoIncrementColumn();
            _colIsCurrentField = CacheTable.GetColumn(TableColumn.EDeltaType.IsCurrentField);
            _colCreateAuditKey = CacheTable.GetColumn(TableColumn.EDeltaType.CreateAuditKey);
            _sourceOperationOrdinal = PrimaryTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.DatabaseOperation);
            _validationStatusOrdinal = PrimaryTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.ValidationStatus);

            _rejectedReasonOrdinal = PrimaryTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.RejectedReason);
            if(CacheTable.GetColumn(TableColumn.EDeltaType.RejectedReason) == null)
            {
                var rejectColumn =
                    new TableColumn("RejectedReason", ETypeCode.String, TableColumn.EDeltaType.RejectedReason)
                    {
                        AllowDbNull = true
                    };
                CacheTable.Columns.Add(rejectColumn);
            }

            _validFromOrdinal = CacheTable.GetOrdinal(TableColumn.EDeltaType.ValidFromDate);
            _validToOrdinal = CacheTable.GetOrdinal(TableColumn.EDeltaType.ValidToDate);
            _isCurrentOrdinal = CacheTable.GetOrdinal(TableColumn.EDeltaType.IsCurrentField);
            _versionOrdinal = CacheTable.GetOrdinal(TableColumn.EDeltaType.Version);

            _sourceSurrogateKeyOrdinal = PrimaryTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.SourceSurrogateKey);
            _sourceValidFromOrdinal = GetSourceColumnOrdinal(TableColumn.EDeltaType.ValidFromDate);
            _sourceValidToOrdinal = GetSourceColumnOrdinal(TableColumn.EDeltaType.ValidToDate);
            _sourceIsCurrentOrdinal = PrimaryTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.IsCurrentField);

            _columnCount = CacheTable.Columns.Count;

            _referenceIsValidOrdinal = ReferenceTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.IsCurrentField);
            _referenceSurrogateKeyOrdinal = ReferenceTransform.CacheTable.GetAutoIncrementOrdinal();
            _referenceCreateAuditOrdinal = ReferenceTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.CreateAuditKey);
            _referenceCreateDateOrdinal = ReferenceTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.CreateDate);
            _referenceValidToOrdinal = ReferenceTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.ValidToDate);
            _referenceValidFromOrdinal = ReferenceTransform.CacheTable.GetOrdinal(TableColumn.EDeltaType.ValidFromDate);
            
            _naturalKeyOrdinals = new List<(int, int, int)>();
            _trackingOrdinals = new List<(int, int, int)>();
            
            foreach (var col in CacheTable.Columns)
            {
                var ordinal = CacheTable.GetOrdinal(col);
                var referenceOrdinal = ReferenceTransform.GetOrdinal(col);

                var primaryOrdinal = -1;
                switch (col.DeltaType)
                {
                    case TableColumn.EDeltaType.ValidToDate:
                        primaryOrdinal = _sourceValidToOrdinal;
                        break;
                    case TableColumn.EDeltaType.ValidFromDate:
                        primaryOrdinal = _sourceValidFromOrdinal;
                        break;
                    default:
                        primaryOrdinal = PrimaryTransform.CacheTable.GetOrdinal(col.Name); //ignore any comparisons on columns that do not exist in source.
                        break;
                }
                
                if (ordinal >= 0 && primaryOrdinal >= 0 && referenceOrdinal >= 0)
                {
                    switch (col.DeltaType)
                    {
                        case TableColumn.EDeltaType.TrackingField:
                            _trackingOrdinals.Add((ordinal, primaryOrdinal, referenceOrdinal));
                            break;
                        case TableColumn.EDeltaType.NaturalKey:
                            _naturalKeyOrdinals.Add((ordinal, primaryOrdinal, referenceOrdinal));
                            break;
                    }
                }
            }
        }
        
        public int GetSourceColumnOrdinal(TableColumn.EDeltaType deltaType)
        {
            var sourceOrdinal = PrimaryTransform.CacheTable.GetOrdinal(deltaType);
            // if the source ValidFrom was not found on delta type, use the target table valid from and attempt to match on name.
            if (sourceOrdinal < 0)
            {
                var targetColumn = CacheTable.GetColumn(deltaType);
                if (targetColumn != null)
                {
                    sourceOrdinal = PrimaryTransform.CacheTable.GetOrdinal(targetColumn.Name);
                }
            }

            return sourceOrdinal;
        }

        private bool CompareFields(object sourceValue, TableColumn referenceColumn, object referenceValue)
        {
            var source = sourceValue;
            var reference = referenceValue;

            if (sourceValue is EncryptedObject sourceObject)
            {
                switch (referenceColumn.SecurityFlag)
                {
                    case TableColumn.ESecurityFlag.FastEncrypted:
                        reference = FastDecrypt(referenceValue);
                        source = sourceObject.OriginalValue; 
                        break;
                    case TableColumn.ESecurityFlag.StrongEncrypted:
                        reference = StrongDecrypt(referenceValue);
                        source = sourceObject.OriginalValue; 
                        break;
                    case TableColumn.ESecurityFlag.OnWayHashed:
                        return OneWayHashCompare(referenceValue, sourceObject.OriginalValue);
                }
            }

            return Operations.Compare(referenceColumn.DataType, source, reference) == 0;
        }

        private char ParentOperation()
        {
            if (_sourceOperationOrdinal >= 0)
            {
                if (PrimaryTransform.CurrentRow == null)
                {
                    return 'C';
                }
                return PrimaryTransform.GetValue<char>(_sourceOperationOrdinal);
            }

            return _parentDelta?.Operation ?? 'C';
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            object[] newRow = null;
            _currentDateTime = DateTime.Now; //this is created here an ensure all datetime records in the row match exactly.

            //if the delta is set to reload.  Set the first row as an operation T="truncate table"
            if (_truncateComplete == false && (DeltaType == EUpdateStrategy.Reload || _parentDelta?.Operation == 'T'))
            {
                newRow = CreateTruncateRow();
                _truncateComplete = true;
                return newRow;
            }

            //very first action is to add a defaultRow
            if (!_defaultRowAdded && AddDefaultRow)
            {
                _defaultRowAdded = true;

                newRow = CreateDefaultRow();

                //if this is a truncate job, always add the default row
                if (DeltaType == EUpdateStrategy.Reload)
                    return newRow;

                //query the reference transform to check if the row already exists.
                var filters = new List<Filter>();

                //lookup the default value by the surrogate key (always = 0) or natural key if a target surrogate key does not exist.
                if (_colAutoIncrement != null)
                {
                    filters.Add(new Filter(_colAutoIncrement, Filter.ECompare.IsEqual, "0"));
                }
                else
                {
                    if (!_naturalKeyOrdinals.Any())
                    {
                        throw new TransformException("The delta transform cannot run as there are no natural key columns specified on the target table.");
                    }

                    foreach (var ordinal in _naturalKeyOrdinals)
                    {
                        var col = CacheTable.Columns[ordinal.ordinal];
                        if (col.DefaultValue == null)
                        {
                            throw new TransformException("The delta transform cannot run null default value on the column " + col.Name + ".   Edit the table columns and specify a default value, or switch off of the generate default row setting");
                        }
                        filters.Add(new Filter(col, Filter.ECompare.IsEqual, col.DefaultValue));
                    }
                }

                var query = new SelectQuery() { Filters = filters };

                var referenceOpenResult = await ReferenceTransform.Open(AuditKey, query, cancellationToken);
                if (!referenceOpenResult)
                {
                    throw new TransformException("Failed to open the target table reader.");
                }

                //if no row in the reference transform, then return the created default value.
                if (!await ReferenceTransform.ReadAsync(cancellationToken))
                {
                    return newRow;
                }

                //if the default row exists, compare the tracking columns to determine if an update is neccessary.
                var isMatch = true;
                foreach(var ordinal in _trackingOrdinals)
                {
                    try
                    {
                        var result = CompareFields(ReferenceTransform[ordinal.referenceOrdinal], CacheTable.Columns[ordinal.ordinal], newRow[ordinal.ordinal]);
                        if (!result)
                        {
                            isMatch = false;
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new TransformException($"The delta transform {Name} failed as column {CacheTable.Columns[ordinal.ordinal].Name} contains incompatible values.", ex, ReferenceTransform[ordinal.referenceOrdinal], newRow[ordinal.ordinal]);
                    }
                }

                //columns do not match, so do an update
                if (!isMatch)
                {
                    newRow[0] = 'U';
                    return newRow;
                }

                //rows is ignored.
                TransformRowsIgnored++;
            }

            //second action is to read a record from the primary transform.
            if (_firstRead)
            {
                _firstRead = false;

                //read a row from the primary and target table
                _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);

                //create a filter that will be passed (if supported to the database).  Improves performance.
                var filters = new List<Filter>();

                //first add a where IsCurrentField = true
                //if (colIsCurrentField != null)
                //    filters.Add(new Filter(colIsCurrentField, Filter.ECompare.IsEqual, true));

                //second add a where natural key is greater than the first record key.  (excluding where delete detection is on).
                if (_primaryOpen && !DoDelete)
                {
                    foreach (var ordinal in _naturalKeyOrdinals)
                    {
                        filters.Add(new Filter(CacheTable.Columns[ordinal.ordinal], Filter.ECompare.GreaterThanEqual, PrimaryTransform[ordinal.primaryOrdinal]));
                    }
                }

                var query = new SelectQuery() { Filters = filters };

                if (DoUpdate || DoDelete || DoPreserve)
                {
                    ReferenceTransform.Close();
                    await ReferenceTransform.Open(AuditKey, query, cancellationToken);
                    _referenceOpen = await ReferenceRead(cancellationToken);
                }
                else
                    _referenceOpen = false;
            }

            // if an operation is set on a parent node, then take appropriate action.
            var parentOperation = ParentOperation();
            switch (parentOperation)
            {
                case 'R':
                    //if row is marked reject, then just push it through.
                    newRow = CreateOutputRow('R');
                    _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                    return newRow;
                case 'U':
                    if (!_parentDelta.IsCurrent)
                    {
                        if (_primaryOpen)
                        {
                            newRow = CreateOutputRow('U');    
                            if(_isCurrentOrdinal >= 0) newRow[_isCurrentOrdinal] = _parentDelta.IsCurrent;
                        }
                        else
                        {
                            if (!_primaryOpen)
                            {
                                if (!_referenceOpen)
                                    return null;
                                
                                //if there are still more records in the target table, then everything else is a delete.
                                newRow = CreateDeleteRow();
                                _referenceOpen = await ReferenceRead(cancellationToken);
                                return newRow;
                            }
                        }
                        
                        if(_validToOrdinal >= 0) newRow[_validToOrdinal] = _parentDelta.ValidTo;
                        _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                        return newRow;
                    }

                    break;
                case 'D':
                    newRow = CreateOutputRow('D');
                    _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                    return newRow;
            }

            //if there are no updates. logic is simple, just push the source records through to the target.
            if (!DoUpdate)
            {
                if (_primaryOpen)
                {
                    newRow = CreateOutputRow('C');
                    _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                    return newRow;
                }
                return null;
            }

            //if there is a saved row (due to a preserve operation splitting a row into update/insert operations) write it out
            if (PreserveRow != null)
            {
                var returnValue = PreserveRow;
                PreserveRow = null;
                return returnValue;
            }

            object validFrom =  null;
            var readReferenceIfChanged = false;

            //this loop continues when there are matching source target rows.
            while (true)
            {
                //if the primary table has finished reading any remaining rows in the target will be deletes.
                if (!_primaryOpen)
                {
                    if (!DoDelete || !_referenceOpen)
                        return null; //not checking deletes, then finish.
                                     //if there are still more records in the target table, then everything else is a delete.
                    newRow = CreateDeleteRow();
                    _referenceOpen = await ReferenceRead(cancellationToken);
                    return newRow;
                }

                //check if the natural key in the source & target are less/match/greater to determine operation
                var compareResult = -1;

                if (!_referenceOpen)
                {
                    //if target reader has finished, then the natural key compare will always be not-equal.
                    compareResult = -1;
                }
                else
                {
                    foreach (var ordinal in _naturalKeyOrdinals)
                    {
                        try
                        {
                            compareResult = Operations.Compare(CacheTable.Columns[ordinal.ordinal].DataType,
                                PrimaryTransform[ordinal.primaryOrdinal], ReferenceTransform[ordinal.referenceOrdinal]);
                            if (compareResult != 0)
                                break;
                        }
                        catch (Exception ex)
                        {
                            throw new TransformException(
                                $"The delta transform failed comparing incompatible values on column {CacheTable.Columns[ordinal.ordinal]}.  {ex.Message}",
                                PrimaryTransform[CacheTable.Columns[ordinal.ordinal].Name], ReferenceTransform[ReferenceTransform.GetName(ordinal.referenceOrdinal)]);
                        }
                    }

                    if(compareResult != 0 && readReferenceIfChanged)
                    {
                        _referenceOpen = await ReferenceRead(cancellationToken);
                        readReferenceIfChanged = false;
                        continue;
                    }
                }

                readReferenceIfChanged = false;

                //if the primary greater in sort order than the target, then the target row has been deleted.
                if (compareResult == 1 && DoDelete)
                {
                    newRow = CreateDeleteRow();
                    _referenceOpen = await ReferenceRead(cancellationToken);
                    return newRow;
                }

                //if compare result is greater, and not checking deletes.  Move the target table to the next row and test again.
                if (compareResult == 1)
                {
                    _referenceOpen = await ReferenceRead(cancellationToken);
                    continue;
                }

                //if not checking deletes and not equal, than this is a new row.  
                if (compareResult != 0)
                {
                    newRow = CreateOutputRow('C');
                    _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                }
                else
                {
                    // if the source row has a valid from less than the target row, then ignore the source row.
                    if (_sourceValidFromOrdinal >= 0 && _referenceValidFromOrdinal >= 0)
                    {
                        var compare = Operations.Compare(_colValidFrom.DataType, PrimaryTransform[_sourceValidFromOrdinal], ReferenceTransform[_referenceValidFromOrdinal]);
                        if (compare == -1)
                        {
                            _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                            TransformRowsIgnored++;
                            continue;
                        }
                    }
                    
                    //the final possibility, is the natrual key is a match, then check for a changed tracking column
                    var isMatch = true;
                    foreach (var ordinal in _trackingOrdinals)
                    {
                        try
                        {
                            var compareResult2 = CompareFields(PrimaryTransform[ordinal.primaryOrdinal], CacheTable.Columns[ordinal.ordinal], ReferenceTransform[ordinal.referenceOrdinal]);
                            if (!compareResult2)
                            {
                                isMatch = false;
                                break;
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new TransformException($"The delta transform failed comparing incompatible values on column {CacheTable.Columns[ordinal.ordinal].Name}.  {ex.Message}", PrimaryTransform[ordinal.primaryOrdinal], ReferenceTransform[ordinal.referenceOrdinal]);
                        }
                    }

                    if (isMatch)
                    {
                        //if we have a full record match, then the record is to be skipped, and the next record read.
                        TransformRowsIgnored++;

                        _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);

                        if (_primaryOpen && _sourceValidFromOrdinal >= 0 && _referenceValidToOrdinal >=0)
                        {
                            var isGreaterThan = Operations.GreaterThanOrEqual(_colValidFrom.DataType, PrimaryTransform[_sourceValidFromOrdinal], ReferenceTransform[_referenceValidToOrdinal]);
                            if (isGreaterThan)
                            {
                                _referenceOpen = await ReferenceRead(cancellationToken);
                            }
                            else
                            {
                                // set a marker to read another record from the reference table if the natural key has changed.
                                readReferenceIfChanged = true;
                            }
                        }
                        else
                        {
                            _referenceOpen = await ReferenceRead(cancellationToken);
                        }

                        continue; //continue the loop
                    }

                    //if the record has changed and preserve history is on, then there will be two output operations.
                    if (DoPreserve)
                    {
                        //store this in the preserve field, so it is written on the next read operation.
                        PreserveRow = CreateOutputRow('C');
                        newRow = CreateDeleteRow(PreserveRow);
                        
                        if (_versionOrdinal >= 0)
                        {
                            PreserveRow[_versionOrdinal] = (int)newRow[_versionOrdinal] + 1;
                        }
                    }
                    else
                    {
                        newRow = CreateOutputRow('U');

                        //keep the surrogate key, create date, and create audit.  update the rest.
                        if (_colAutoIncrement != null)
                        {
                            newRow[CacheTable.GetOrdinal(_colAutoIncrement.Name)] = ReferenceTransform[_referenceSurrogateKeyOrdinal];
                        }

                        if (_colCreateAuditKey != null)
                        {
                            newRow[CacheTable.GetOrdinal(_colCreateAuditKey.Name)] = ReferenceTransform[_referenceCreateAuditOrdinal];
                        }

                        if (_colCreateDate != null)
                        {
                            newRow[CacheTable.GetOrdinal(_colCreateDate.Name)] = ReferenceTransform[_referenceCreateDateOrdinal];
                        }
                    }

                    //move primary and target readers to the next record.
                    _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                    _referenceOpen = await ReferenceRead(cancellationToken);
                }


                //the delta always puts a sort against natural key, so duplicates rows will be next to each other.
                //check the newRow against the previous row in the file to deduplicate any matching natural keys.
                if (PreserveRow == null && _primaryOpen && CompareNewRowPrevious(newRow))
                {
                    //if the row is a match against the tracking keys then just ignore it.
                    if (CompareNewRowPreviousValues(newRow) || !DoPreserve)
                    {
                        if(validFrom == null && _validFromOrdinal >= 0)
                        {
                            validFrom = newRow[_validFromOrdinal];
                        }

                        TransformRowsIgnored++;
                        continue;
                    }

                    //if the previous row is a match, and the tracking field values are different, then add the row as a preserved row
                    TransformRowsPreserved++;
                    if (_isCurrentOrdinal >= 0)
                    {
                        newRow[_isCurrentOrdinal] = false;
                    }

                    if (validFrom != null && _validFromOrdinal >= 0)
                    {
                        newRow[_validFromOrdinal] = validFrom;
                    }

                    // if there is a valid_from mapped form the source, map it to the valid_to when preserving the records.
                    if (_validToOrdinal >= 0 && _sourceValidFromOrdinal >= 0 && _sourceValidToOrdinal < 0)
                    {
                        newRow[_validToOrdinal] = PrimaryTransform[_sourceValidFromOrdinal];
                    } else if(_validToOrdinal >=0 && _validFromOrdinal >= 0)

                    newRow[0] = 'C';
                }

                if (validFrom != null && _validFromOrdinal >= 0)
                {
                    newRow[_validFromOrdinal] = validFrom;
                }

                return newRow;
            }
        }

        //reads reference rows, ignoring any rows where iscurrent = false and the surrogateKey = 0, or validfrom == validto
        private async Task<bool> ReferenceRead(CancellationToken cancellationToken = default)
        {
            while (await ReferenceTransform.ReadAsync(cancellationToken))
            {
                if(_colAutoIncrement != null)
                {
                    try
                    {
                        var returnValue = ReferenceTransform[_referenceSurrogateKeyOrdinal];

                        //surogate key = 0, ignore as this is the defaulted value.
                        if (Equals(returnValue, 0))
                        {
                            continue;
                        }
                    }
                    catch(Exception ex)
                    {
                        throw new TransformException($"The delta transform {Name} failed as the surrogate key column is expected to have a numerical value.  {ex.Message}. ", ex, ReferenceTransform[_referenceSurrogateKeyOrdinal]);
                    }
                }

                //TODO: This routine skips records in the source with isCurrent = false, however this causes problem with source file that has invalid records.
                if (_colIsCurrentField == null)
                {
                    return true;
                }
                {
                    try
                    {
                        var returnValue = Operations.Parse<bool>(ReferenceTransform[_referenceIsValidOrdinal]);

                        //IsCurrent = false, continue to next record.
                        if (!returnValue)
                        {
                            continue;
                        }
                        return true;
                    }
                    catch(Exception ex)
                    {
                        throw new TransformException($"The delta transform {Name} failed as the column {_colIsCurrentField.Name} is expected to have a boolean value.  {ex.Message}.", ex, ReferenceTransform[_referenceIsValidOrdinal]);
                    }
                }
            }
            return false;
        }




        private bool CompareNewRowPrevious(object[] newRow)
        {
            //check if the natural key in the source & target are less/match/greater to determine operation
            foreach (var ordinal in _naturalKeyOrdinals)
            {
                if(!Equals(PrimaryTransform[ordinal.primaryOrdinal], newRow[ordinal.ordinal]))
                    return false;
            }

            return true;
        }

        private bool CompareNewRowPreviousValues(object[] newRow)
        {
            //the final possibility, is the natural key is a match, check for changed tracking columns
            var isMatch = true;
            foreach (var ordinal in _trackingOrdinals) 
            {
                try
                {
                    var result = Operations.Equal(CacheTable.Columns[ordinal.ordinal].DataType, PrimaryTransform[ordinal.primaryOrdinal],
                        newRow[ordinal.ordinal]);

                    if (!result)
                    {
                        isMatch = false;
                        break;
                    }
                }
                catch (Exception ex)
                {
                    throw new TransformException(
                        $"The delta transform failed comparing incompatible values on column {CacheTable.Columns[ordinal.ordinal].Name}.  {ex.Message}",
                        PrimaryTransform[ordinal.primaryOrdinal], newRow[ordinal.referenceOrdinal]);
                }
            }

            return isMatch;
        }

        private object[] CreateTruncateRow()
        {
            var newRow = new object[_columnCount];
            newRow[0] = 'T';

            if (DoPreserve)
                TransformRowsPreserved++;

            for (var i = 1; i < _columnCount; i++)
            {
                if (CacheTable.Columns[i].DataType == ETypeCode.Node)
                {
                    // create an empty reader if there are any child nodes.
                    var childTable = new Table(CacheTable.Columns[i].Name, CacheTable.Columns[i].ChildColumns);
                    newRow[i] = new ReaderMemory(childTable);
                }
            }

            return newRow;
        }

        private object[] CreateDeleteRow(object[] nextRow = null)
        {
            var newRow = new object[_columnCount];
            newRow[0] = DoPreserve ? 'U' : 'D';

            if (DoPreserve)
                TransformRowsPreserved++;

            for (var i = 1; i < _columnCount; i++)
            {
                switch(CacheTable.Columns[i].DeltaType)
                {
                    case TableColumn.EDeltaType.ValidToDate:
                        if (nextRow != null && _colValidFrom != null)
                            newRow[i] = nextRow[_validFromOrdinal];
                        else
                            newRow[i] = _currentDateTime;
                        break;
                    case TableColumn.EDeltaType.IsCurrentField:
                        newRow[i] = false;
                        break;
                    case TableColumn.EDeltaType.UpdateDate:
                        newRow[i] = _currentDateTime;
                        break;
                    case TableColumn.EDeltaType.UpdateAuditKey:
                        newRow[i] = AuditKey;
                        break;
                    default:
                        var ordinal = ReferenceTransform.GetOrdinal(CacheTable.Columns[i].Name);
                        if (ordinal > -1)
                            newRow[i] = ReferenceTransform[ordinal];
                        break;
                }

                if (CacheTable.Columns[i].DataType == ETypeCode.Node)
                {
                    // create an empty reader if there are any child nodes.
                    var childTable = new Table(CacheTable.Columns[i].Name, CacheTable.Columns[i].ChildColumns);
                    newRow[i] = new ReaderMemory(childTable);
                }
            }

            return newRow;
        }

        private object[] CreateOutputRow(char operation)
        {
            var newRow = new object[_columnCount];

            newRow[0] = operation;

            for (var referenceOrdinal = 1; referenceOrdinal < _columnCount; referenceOrdinal++)
            {
                //check if a matching source field exists (-1 will be returned if it doesn't)
                var sourceOrdinal = _sourceOrdinals[referenceOrdinal - 1];

                switch (CacheTable.Columns[referenceOrdinal].DeltaType)
                {
                    case TableColumn.EDeltaType.ValidFromDate:
                        if (_sourceValidFromOrdinal == -1 && sourceOrdinal == -1)
                            newRow[referenceOrdinal] = _currentDateTime;
                        else if(sourceOrdinal >= 0)
                            newRow[referenceOrdinal] = PrimaryTransform[sourceOrdinal];
                        else
                            newRow[referenceOrdinal] = PrimaryTransform[_sourceValidFromOrdinal];

                        break;
                    case TableColumn.EDeltaType.ValidToDate:
                        if (_sourceValidToOrdinal == -1 && sourceOrdinal == -1)
                            newRow[referenceOrdinal] = _defaultValidToDate;
                        else if(sourceOrdinal >= 0)
                            newRow[referenceOrdinal] = PrimaryTransform[sourceOrdinal];
                        else
                            newRow[referenceOrdinal] = PrimaryTransform[_sourceValidToOrdinal];
                        break;
                    case TableColumn.EDeltaType.CreateDate:
                    case TableColumn.EDeltaType.UpdateDate:
                        newRow[referenceOrdinal] = _currentDateTime;
                        break;
                    case TableColumn.EDeltaType.IsCurrentField:
                        if (_sourceIsCurrentOrdinal == -1 && sourceOrdinal == -1)
                            newRow[referenceOrdinal] = true;
                        else if (sourceOrdinal >= 0)
                            newRow[referenceOrdinal] = PrimaryTransform[_sourceIsCurrentOrdinal];
                        else
                            newRow[referenceOrdinal] = PrimaryTransform[_sourceIsCurrentOrdinal];
                        break;
                    case TableColumn.EDeltaType.Version:
                        newRow[referenceOrdinal] = 1;
                        break;
                    case TableColumn.EDeltaType.AutoIncrement:
                        _autoIncrementValue = Operations.Increment(_autoIncrementValue); //increment now that key has been used.
                        newRow[referenceOrdinal] = AutoIncrementValue;
                        break;
                    case TableColumn.EDeltaType.SourceSurrogateKey:
                        if (_colAutoIncrement == null && sourceOrdinal == -1)
                            newRow[referenceOrdinal] = 0;
                        else if (sourceOrdinal >= 0)
                            newRow[referenceOrdinal] = PrimaryTransform[sourceOrdinal];
                        else if (_sourceSurrogateKeyOrdinal == -1)
                            newRow[referenceOrdinal] = 0;
                        else
                            newRow[referenceOrdinal] = PrimaryTransform[_sourceSurrogateKeyOrdinal];
                        break;
                    case TableColumn.EDeltaType.CreateAuditKey:
                    case TableColumn.EDeltaType.UpdateAuditKey:
                    case TableColumn.EDeltaType.AzurePartitionKey:
                        newRow[referenceOrdinal] = AuditKey;
                        break;
                    case TableColumn.EDeltaType.IgnoreField:
                        //do nothing
                        break;
                    case TableColumn.EDeltaType.ValidationStatus:
                        if(_validationStatusOrdinal >= 0)
                            newRow[referenceOrdinal] = PrimaryTransform[_validationStatusOrdinal];
                        break;
                    case TableColumn.EDeltaType.RejectedReason:
                        if (_rejectedReasonOrdinal >= 0)
                            newRow[referenceOrdinal] = PrimaryTransform[_rejectedReasonOrdinal];
                        break;
                    default:
                        if (sourceOrdinal > -1)
                        {
                            newRow[referenceOrdinal] = PrimaryTransform[sourceOrdinal];
                        }
                        else
                        {
                            newRow[referenceOrdinal] = CacheTable.Columns[referenceOrdinal].DefaultValue;
                        }
                        break;
                }
            }

            for (var referenceOrdinal = 1; referenceOrdinal < _columnCount; referenceOrdinal++)
            {
                if (!CacheTable[referenceOrdinal].IsParent && newRow[referenceOrdinal] is TransformNode transformNode)
                {
                    var newNode = new TransformNode {PrimaryTransform = transformNode};
                    newNode.SetTable(transformNode.CacheTable, CacheTable);
                    newNode.SetParentRow(newRow);
                    newNode.Open().Wait();
                    newRow[referenceOrdinal] = newNode;
                }
            }

            return newRow;
        }

        private object[] CreateDefaultRow()
        {
            var newRow = new object[_columnCount];

            newRow[0] = 'C';

            var timer = new Stopwatch();
            for (var referenceOrdinal = 1; referenceOrdinal < _columnCount; referenceOrdinal++)
            {
                var referenceColumn = CacheTable.Columns[referenceOrdinal];

                if (referenceColumn != null)
                {
                    switch (referenceColumn.DeltaType)
                    {
                        case TableColumn.EDeltaType.ValidFromDate:
                            if (referenceColumn.DefaultValue.ObjectIsNullOrBlank())
                                newRow[referenceOrdinal] = _defaultValidFromDate;
                            else
                                newRow[referenceOrdinal] = referenceColumn.DefaultValue;
                            break;
                        case TableColumn.EDeltaType.ValidToDate:
                            if (referenceColumn.DefaultValue.ObjectIsNullOrBlank())
                                newRow[referenceOrdinal] = _defaultValidToDate;
                            else
                                newRow[referenceOrdinal] = referenceColumn.DefaultValue;
                            break;
                        case TableColumn.EDeltaType.CreateDate:
                        case TableColumn.EDeltaType.UpdateDate:
                            newRow[referenceOrdinal] = _currentDateTime;
                            break;
                        case TableColumn.EDeltaType.IsCurrentField:
                            newRow[referenceOrdinal] = true;
                            break;
                        case TableColumn.EDeltaType.AutoIncrement:
                            newRow[referenceOrdinal] = 0;
                            break;
                        case TableColumn.EDeltaType.SourceSurrogateKey:
                            newRow[referenceOrdinal] = 0;
                            break;
                        case TableColumn.EDeltaType.CreateAuditKey:
                        case TableColumn.EDeltaType.UpdateAuditKey:
                        case TableColumn.EDeltaType.AzurePartitionKey:
                            newRow[referenceOrdinal] = AuditKey;
                            break;
                        case TableColumn.EDeltaType.IgnoreField:
                            //do nothing
                            break;
                        case TableColumn.EDeltaType.NaturalKey:
                            if (referenceColumn.DefaultValue.ObjectIsNullOrBlank())
                                throw new Exception("A default column could not be created as the column \"" + referenceColumn.Name + "\" is part of the natural key and has a default value of null.  Edit the target table columns and set the default value to a non-null value to continue.");
                            else
                                newRow[referenceOrdinal] = referenceColumn.DefaultValue;
                            break;
                        default:
                            newRow[referenceOrdinal] = referenceColumn.DefaultValue;
                            break;
                    }
                }
            }

            return newRow;
        }

        private class JoinKeyComparer : IComparer<object[]>
        {
            public int Compare(object[] x, object[] y)
            {
                for (var i = 0; i < x.Length; i++)
                {
                    var compareResult = ((IComparable)x[i]).CompareTo((IComparable)y[i]);

                    if (compareResult == 0)
                    {
                        continue;
                    }

                    return compareResult;
                }
                return 0;
            }
        }

        public override bool ResetTransform()
        {
            _firstRead = true;
            _referenceOpen = true;
            _primaryOpen = true;
            CurrentRowNumber = -1;

            return true;
        }


        public override List<Sort> RequiredSortFields()
        {
            var fields = new List<Sort>();

            if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
            {
            }
            else
            {
                foreach (var col in ReferenceTransform.CacheTable.GetColumns(TableColumn.EDeltaType.NaturalKey))
                {
                    var primaryColumn = PrimaryTransform.CacheTable.Columns[col.Name];
                    if (primaryColumn == null)
                    {
                        throw new Exception($"The delta could not run as the target table contains a column {col.Name} that does not have a matching input column.");
                    }
                    fields.Add(new Sort(primaryColumn));
                }
                var validFrom = ReferenceTransform.CacheTable.GetColumn(TableColumn.EDeltaType.ValidFromDate);
                if (validFrom != null)
                {
                    var primaryValidFrom = PrimaryTransform.CacheTable.Columns[validFrom.Name];
                    if(primaryValidFrom == null)
                    {
                        primaryValidFrom = PrimaryTransform.CacheTable.GetColumn(TableColumn.EDeltaType.ValidFromDate);
                    }

                    if (primaryValidFrom != null)
                    {
                        fields.Add(new Sort(primaryValidFrom));
                    }
                }
            }

            return fields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            var fields = new List<Sort>();

            if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
            {
            }
            else
            {
                // sure sort columns are same order or primary transform
                
                foreach (var col in ReferenceTransform.CacheTable.GetColumns(TableColumn.EDeltaType.NaturalKey))
                {
//                    var referenceColumn = PrimaryTransform.CacheTable.Columns[col.Name];
//                    if (referenceColumn == null)
//                    {
//                        throw new Exception($"The delta could not run as the target table contains a column {col.Name} that does not have a matching input column.");
//                    }
                    fields.Add(new Sort(col));
                }
                var validFrom = ReferenceTransform.CacheTable.GetColumn(TableColumn.EDeltaType.ValidFromDate);
                if (validFrom != null)
                {
                    fields.Add(new Sort(validFrom));
                }
            }

            return fields;
        }
    }
}
