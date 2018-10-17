using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.transforms.Transforms;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms
{
    [Transform(
        Name = "Delta",
        Description = "Compare incoming data against the target table to produce a delta.",
        TransformType = TransformAttribute.ETransformType.Delta
    )]
    public sealed class TransformDelta : Transform
    {
        public TransformDelta(Transform inReader, Transform referenceTransform, EUpdateStrategy deltaType, long surrogateKey, bool addDefaultRow)
        {
            DeltaType = deltaType;
            SurrogateKey = surrogateKey;

            // create a copy of the target table without the schem or any deltaType = Ignore columns
            CacheTable = referenceTransform.CacheTable.Copy(true, true);
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

            SetInTransform(inReader, referenceTransform);
        }

        [JsonConverter(typeof (StringEnumConverter))]
        public enum EUpdateStrategy
        {
            Reload, //truncates the table and reloads
            Append, //inserts records.  use if the data feed is always new data.
            //Bulk, //same is insert, however uses database bulk load feature.
            AppendUpdate, //inserts new records, and updates records.  use if the data feed is new and existing data.
            AppendUpdateDelete, //inserts new records, updates existing records, and (logically) deletes removed records.  use to maintain an exact copy of the data feed.
            AppendUpdatePreserve, //inserts new records, updates existing records, and preserves the changes.
            AppendUpdateDeletePreserve // inserts new records, updates existing records, (logically) deletes removed records.
        }

        private readonly DateTime _defaultValidFromDate = new DateTime(1900, 01, 01);
        private readonly DateTime _defaultValidToDate = new DateTime(2099, 12, 31, 23, 59, 59);

        private bool _firstRead;
        private bool _truncateComplete;
        private bool _defaultRowAdded;
        private bool _referenceOpen;
        private bool _primaryOpen;

        private TableColumn _colValidFrom;
        private TableColumn _colValidTo;
        private TableColumn _colCreateDate;
        private TableColumn _colUpdateDate;
        private TableColumn _colSurrogateKey;
        private TableColumn _colIsCurrentField;
        private TableColumn _colSourceSurrogateKey;
        private TableColumn _colCreateAuditKey;
        private TableColumn _colUpdateAuditKey;

        //preload ordinals to improve performance.
        private int _databaseOperationOrdinal;
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
        private int _referenceVersionOrdinal;
        private int _referenceCreateAuditOrdinal;
        private int _referenceCreateDateOrginal;
        private int _referenceValidToOridinal;
        private int _referenceValidFromOridinal;

        private int _columnCount;

        private TableColumn[] _colNatrualKey;

        private EUpdateStrategy DeltaType { get; set; }
        public long SurrogateKey { get; set; }
        public bool AddDefaultRow { get; set; }
        

        private bool DoUpdate { get; set; }
        private bool DoDelete { get; set; }
        private bool DoPreserve { get; set; }

        private object[] PreserveRow { get; set; }
        private object[] NextPrimaryRow { get; set; }

        private List<int> _sourceOrdinals;

        private DateTime _currentDateTime;

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            bool returnValue;

            if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
            {
                returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);
                
                }
            else
            {
                if (query == null)
                    query = new SelectQuery();

                query.Sorts = RequiredSortFields();

                returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);
            }

                        if (ReferenceTransform == null)
                throw new Exception("There must be a target table specified.");

            //add the operation type, which indicates whether record are C-create/U-update or D-Deletes
            if (CacheTable.Columns.SingleOrDefault(c => c.DeltaType == TableColumn.EDeltaType.DatabaseOperation) == null)
            {
                CacheTable.Columns.Insert(0, new TableColumn("Operation", ETypeCode.Byte)
                {
                    DeltaType = TableColumn.EDeltaType.DatabaseOperation
                });
            }

            //get the available audit columns
            SetAuditColumns();

            _firstRead = true;
            _defaultRowAdded = false;
            _truncateComplete = false;
            _primaryOpen = true;
            _referenceOpen = true;

            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            //do some integrity checks
            if (DoPreserve && _colSurrogateKey == null)
                throw new Exception("The delta transform requires the table to have a single surrogate key field for row preservations to be possible.");

            if (DoUpdate && CacheTable.Columns.All(c => c.DeltaType != TableColumn.EDeltaType.NaturalKey))
                throw new Exception("The delta transform requires the table to have at least ont natrual key field for updates to be possible.");

            //set surrogate key to the key field.  This will indicate that the surrogate key should be used when update/deleting records.
            if(_colSurrogateKey != null)
                CacheTable.KeyFields = new List<string>() { _colSurrogateKey.Name };

            //preload the source-target ordinal mapping to improve performance.
            _sourceOrdinals = new List<int>();
            var columnCount = CacheTable.Columns.Count;
            for (var referenceOrdinal = 1; referenceOrdinal < columnCount; referenceOrdinal++)
            {
                _sourceOrdinals.Add(PrimaryTransform.GetOrdinal(CacheTable.Columns[referenceOrdinal].Name));
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
            _colValidFrom = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidFromDate);
            _colValidTo = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidToDate);
            _colCreateDate = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.CreateDate);
            _colUpdateDate = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.UpdateDate);
            _colSurrogateKey = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);
            _colIsCurrentField = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.IsCurrentField);
            _colSourceSurrogateKey = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.SourceSurrogateKey);
            _colCreateAuditKey = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.CreateAuditKey);
            _colUpdateAuditKey = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.UpdateAuditKey);
            _databaseOperationOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);
            _validationStatusOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidationStatus);
            _colNatrualKey = CacheTable.Columns.Where(c=>c.DeltaType == TableColumn.EDeltaType.NaturalKey).ToArray();

            _rejectedReasonOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.RejectedReason);
            if(CacheTable.GetDeltaColumn(TableColumn.EDeltaType.RejectedReason) == null)
            {
                CacheTable.AddColumn("RejectedReason", ETypeCode.String, TableColumn.EDeltaType.RejectedReason);
            }

            _validFromOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidFromDate);
            _validToOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidToDate);
            _isCurrentOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.IsCurrentField);
            _versionOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.Version);

            _sourceSurrogateKeyOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.SourceSurrogateKey);
            _sourceValidFromOrdinal = GetSourceColumnOrdinal(TableColumn.EDeltaType.ValidFromDate);
            _sourceValidToOrdinal = GetSourceColumnOrdinal(TableColumn.EDeltaType.ValidToDate);
            _sourceIsCurrentOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.IsCurrentField);

            _columnCount = CacheTable.Columns.Count;

            _referenceIsValidOrdinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.IsCurrentField);
            _referenceVersionOrdinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.Version);
            _referenceSurrogateKeyOrdinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.SurrogateKey);
            _referenceCreateAuditOrdinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.CreateAuditKey);
            _referenceCreateDateOrginal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.CreateDate);
            _referenceValidToOridinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidToDate);
            _referenceValidFromOridinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidFromDate);

        }

        public int GetSourceColumnOrdinal(TableColumn.EDeltaType deltaType)
        {
            var sourceOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(deltaType);
            // if the source ValidFrom was not found on delta type, use the target table valid from and attempt to match on name.
            if (sourceOrdinal < 0)
            {
                var targetColumn = CacheTable.GetDeltaColumn(deltaType);
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

            return Compare(referenceColumn.DataType, source, reference) == ECompareResult.Equal;
        }


        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            object[] newRow = null;
            _currentDateTime = DateTime.Now; //this is created here an ensure all datetime records in the row match exactly.

            //if the delta is set to reload.  Set the first row as an operation T="truncate table"
            if (DeltaType == EUpdateStrategy.Reload && _truncateComplete == false)
            {
                newRow = new object[_columnCount];
                newRow[0] = 'T';

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

                //lookup the default value by the surrogate key (always = 0) or natrual key if a target surrogate key does not exist.
                if (_colSurrogateKey != null)
                {
                    filters.Add(new Filter(_colSurrogateKey, Filter.ECompare.IsEqual, "0"));
                }
                else
                {
                    var naturalKey = CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey);

                    if (!naturalKey.Any())
                    {
                        throw new TransformException("The delta transform cannot run as there are no natural key columns specified on the target table.");
                    }

                    foreach (var col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey))
                    {
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
                foreach (var col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.TrackingField))
                {
                    var referenceOrdinal = CacheTable.GetOrdinal(col.Name);
                    try
                    {
                        var result = CompareFields(ReferenceTransform[col.Name], col, newRow[referenceOrdinal]);
                        if (!result)
                        {
                            isMatch = false;
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new TransformException($"The delta transform {Name} failed as column {col.Name} contains incompatible values.", ex, ReferenceTransform[col.Name], newRow[referenceOrdinal]);
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

                //second add a where natrual key is greater than the first record key.  (excluding where delete detection is on).
                if (_primaryOpen && !DoDelete)
                {
                    foreach (var col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey))
                    {
                        var referenceOrdinal = PrimaryTransform.GetOrdinal(col.Name); //ignore any comparisons on columns that do not exist in source.
                        if (referenceOrdinal > -1)
                        {
                            filters.Add(new Filter(col, Filter.ECompare.GreaterThanEqual, PrimaryTransform[col.Name]));
                        }
                    }
                }

                var query = new SelectQuery() { Filters = filters };

                if (DoUpdate || DoDelete || DoPreserve)
                {
                    ReferenceTransform.Dispose();
                    await ReferenceTransform.Open(AuditKey, query, cancellationToken);
                    _referenceOpen = await ReferenceRead(cancellationToken);
                }
                else
                    _referenceOpen = false;
            }

            //if row is marked reject, then just push it through.
            if (_primaryOpen && _databaseOperationOrdinal >= 0 && (char)PrimaryTransform[_databaseOperationOrdinal] == 'R')
            {
                newRow = CreateOutputRow('R');
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

            Object validFrom =  null;
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

                //check if the natrual key in the source & target are less/match/greater to determine operation
                var compareResult = ECompareResult.Less;

                if (!_referenceOpen)
                {
                    //if target reader has finished, then the natural key compare will always be not-equal.
                    compareResult = ECompareResult.Less;
                }
                else
                {
                    foreach (var col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey))
                    {
                        var referenceOrdinal = PrimaryTransform.GetOrdinal(col.Name); //ignore any comparisons on columns that do not exist in source.
                        if (referenceOrdinal > -1)
                        {
                            try
                            {
                                compareResult = Compare(col.DataType, PrimaryTransform[col.Name], ReferenceTransform[col.Name]);
                                if (compareResult != ECompareResult.Equal)
                                    break;
                            }
                            catch (Exception ex)
                            {
                                throw new TransformException($"The delta transform failed comparing incompatible values on column {col.Name}.  {ex.Message}", PrimaryTransform[col.Name], ReferenceTransform[col.Name]);
                            }
                        }
                    }

                    if(compareResult != ECompareResult.Equal && readReferenceIfChanged)
                    {
                        _referenceOpen = await ReferenceRead(cancellationToken);
                        readReferenceIfChanged = false;
                        continue;
                    }
                }

                readReferenceIfChanged = false;

                //if the primary greater in sort order than the target, then the target row has been deleted.
                if (compareResult == ECompareResult.Greater && DoDelete)
                {
                    newRow = CreateDeleteRow();
                    _referenceOpen = await ReferenceRead(cancellationToken);
                    return newRow;
                }

                //if compare result is greater, and not checking deletes.  Move the target table to the next row and test again.
                if (compareResult == ECompareResult.Greater)
                {
                    _referenceOpen = await ReferenceRead(cancellationToken);
                    continue;
                }

                //if not checking deletes and not equal, than this is a new row.  
                if (compareResult != ECompareResult.Equal)
                {
                    newRow = CreateOutputRow('C');
                    _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                }
                else
                {
                    // if the source row has a valid from less than the target row, then ignore the source row.
                    if (_sourceValidFromOrdinal >= 0 && _referenceValidFromOridinal >= 0)
                    {
                        var compare = Compare(_colValidFrom.DataType, PrimaryTransform[_sourceValidFromOrdinal], ReferenceTransform[_referenceValidFromOridinal]);
                        if (compare == ECompareResult.Less)
                        {
                            _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                            TransformRowsIgnored++;
                            continue;
                        }
                    }
                    
                    //the final possibility, is the natrual key is a match, then check for a changed tracking column
                    var isMatch = true;
                    foreach (var col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.TrackingField))
                    {
                        var referenceOrdinal = PrimaryTransform.GetOrdinal(col.Name); //ignore any comparisons on columns that do not exist in source.
                        if (referenceOrdinal > -1)
                        {
                            try
                            {
                                var compareResult2 = CompareFields(PrimaryTransform[col.Name], col, ReferenceTransform[col.Name]);
                                if (!compareResult2)
                                {
                                    isMatch = false;
                                    break;
                                }
                            }
                            catch (Exception ex)
                            {
                                throw new TransformException($"The delta transform failed comparing incompatible values on column {col.Name}.  {ex.Message}", PrimaryTransform[col.Name], ReferenceTransform[col.Name]);
                            }
                        }
                    }

                    if (isMatch)
                    {
                        //if we have a full record match, then the record is to be skipped, and the next record read.
                        TransformRowsIgnored++;

                        _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);

                        if (_primaryOpen && _sourceValidFromOrdinal >= 0 && _referenceValidToOridinal >=0)
                        {
                            var compareResult3 = Compare(_colValidFrom.DataType, PrimaryTransform[_sourceValidFromOrdinal], ReferenceTransform[_referenceValidToOridinal]);
                            if (compareResult3 == ECompareResult.Greater || compareResult3 == ECompareResult.Equal)
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
                            PreserveRow[_versionOrdinal] = (dynamic)newRow[_versionOrdinal] + 1;
                        }
                    }
                    else
                    {
                        newRow = CreateOutputRow('U');

                        //keep the surrogate key, create date, and create audit.  update the rest.
                        if (_colSurrogateKey != null)
                        {
                            newRow[CacheTable.GetOrdinal(_colSurrogateKey.Name)] = ReferenceTransform[_referenceSurrogateKeyOrdinal];
                        }

                        if (_colCreateAuditKey != null)
                        {
                            newRow[CacheTable.GetOrdinal(_colCreateAuditKey.Name)] = ReferenceTransform[_referenceCreateAuditOrdinal];
                        }

                        if (_colCreateDate != null)
                        {
                            newRow[CacheTable.GetOrdinal(_colCreateDate.Name)] = ReferenceTransform[_referenceCreateDateOrginal];
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
        private async Task<bool> ReferenceRead(CancellationToken cancellationToken)
        {
            while (await ReferenceTransform.ReadAsync(cancellationToken))
            {
                if(_colSurrogateKey != null)
                {
                    try
                    {
                        var returnValue = TryParse(ETypeCode.Int64, ReferenceTransform[_referenceSurrogateKeyOrdinal]);

                        //surogate key = 0, ignore as this is the defaulted value.
                        if ((long)returnValue == 0)
                        {
                            continue;
                        }
                    }
                    catch(Exception ex)
                    {
                        throw new TransformException($"The delta transform {Name} failed as the surrogate key column is expected to have a numerical value.  {ex.Message}. ", ex, ReferenceTransform[_referenceSurrogateKeyOrdinal]);
                    }
                }

                //TODO: This routine skips records in the source with isucrrent = false, however this causes problem with source file that has invalid records.
                if (_colIsCurrentField == null)
                {
                    return true;
                }
                {
                    try
                    {
                        var returnValue = TryParse(ETypeCode.Boolean, ReferenceTransform[_referenceIsValidOrdinal]);

                        //IsCurrent = false, continue to next record.
                        if (!(bool)returnValue)
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

        private int MatchingSourceOrdinal(TableColumn col)
        {
            var sourceOrdinal = -1;
            switch (col.DeltaType)
            {
                case TableColumn.EDeltaType.ValidToDate:
                    sourceOrdinal = _sourceValidToOrdinal;
                    break;
                case TableColumn.EDeltaType.ValidFromDate:
                    sourceOrdinal = _sourceValidFromOrdinal;
                    break;
                default:
                    sourceOrdinal = PrimaryTransform.CacheTable.GetOrdinal(col.Name); //ignore any comparisons on columns that do not exist in source.
                    break;
            }

            return sourceOrdinal;
        }


        private bool CompareNewRowPrevious(object[] newRow)
        {
            //check if the natrual key in the source & target are less/match/greater to determine operation
            foreach (var col in _colNatrualKey)
            {
                var sourceOrdinal = MatchingSourceOrdinal(col);

                if (sourceOrdinal > -1)
                {
                    var referenceOrdinal = CacheTable.GetOrdinal(col.Name);
                    if(!Equals(PrimaryTransform[sourceOrdinal], newRow[referenceOrdinal]))
                        return false;
                }
            }

            return true;
        }

        private bool CompareNewRowPreviousValues(object[] newRow)
        {
            //the final possibility, is the natrual key is a match, check for changed tracking columns
            var isMatch = true;
            foreach (var col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.TrackingField)) // || c.DeltaType == TableColumn.EDeltaType.ValidFromDate || c.DeltaType == TableColumn.EDeltaType.ValidToDate))
            {
                var sourceOrdinal = MatchingSourceOrdinal(col);
                if (sourceOrdinal > -1)
                {
                    var referenceOrdinal = CacheTable.GetOrdinal(col.Name);
                    try
                    {
                        var result = Compare(col.DataType, PrimaryTransform[sourceOrdinal], newRow[referenceOrdinal]);

                        if (result != ECompareResult.Equal)
                        {
                            isMatch = false;
                            break;
                        }
                    }
                    catch(Exception ex)
                    {
                        throw new TransformException($"The delta transform failed comparing incompatible values on column {col.Name}.  {ex.Message}", PrimaryTransform[sourceOrdinal], newRow[referenceOrdinal]);
                    }
                }
            }

            return isMatch;
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
            }

            return newRow;
        }

        private object[] CreateOutputRow(char operation)
        {
            var newRow = new object[_columnCount];

            newRow[0] = operation;

            var timer = new Stopwatch();
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
                    case TableColumn.EDeltaType.SurrogateKey:
                        SurrogateKey++; //increment now that key has been used.
                        newRow[referenceOrdinal] = SurrogateKey;
                        break;
                    case TableColumn.EDeltaType.SourceSurrogateKey:
                        if (_colSurrogateKey == null && sourceOrdinal == -1)
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
                        timer.Start();

                        if (sourceOrdinal > -1)
                        {
                            newRow[referenceOrdinal] = PrimaryTransform[sourceOrdinal];
                        }
                        else
                        {
                            newRow[referenceOrdinal] = CacheTable.Columns[referenceOrdinal].DefaultValue;
                        }
                        timer.Stop();
                        break;
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
                        case TableColumn.EDeltaType.SurrogateKey:
                            SurrogateKey++; //increment now that key has been used.
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

        public override string Details()
        {
            return "Delta: " + DeltaType.ToString();
        }

        public override List<Sort> RequiredSortFields()
        {
            var fields = new List<Sort>();

            if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
            {
            }
            else
            {
                foreach (var col in ReferenceTransform.CacheTable.GetColumnsByDeltaType(TableColumn.EDeltaType.NaturalKey))
                {
                    var primaryColumn = PrimaryTransform.CacheTable.Columns[col.Name];
                    if (primaryColumn == null)
                    {
                        throw new Exception($"The delta could not run as the target table contains a column {col.Name} that does not have a matching input column.");
                    }
                    fields.Add(new Sort(primaryColumn));
                }
                var validFrom = ReferenceTransform.CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidFromDate);
                if (validFrom != null)
                {
                    var primaryValidFrom = PrimaryTransform.CacheTable.Columns[validFrom.Name];
                    if(primaryValidFrom == null)
                    {
                        primaryValidFrom = PrimaryTransform.CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidFromDate);
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
                
                foreach (var col in ReferenceTransform.CacheTable.GetColumnsByDeltaType(TableColumn.EDeltaType.NaturalKey))
                {
//                    var referenceColumn = PrimaryTransform.CacheTable.Columns[col.Name];
//                    if (referenceColumn == null)
//                    {
//                        throw new Exception($"The delta could not run as the target table contains a column {col.Name} that does not have a matching input column.");
//                    }
                    fields.Add(new Sort(col));
                }
                var validFrom = ReferenceTransform.CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidFromDate);
                if (validFrom != null)
                {
                    fields.Add(new Sort(validFrom));
                }
            }

            return fields;
        }
    }
}
