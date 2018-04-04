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
        public TransformDelta(Transform inReader, Transform targetTransform, EUpdateStrategy deltaType, long surrogateKey, bool addDefaultRow)
        {
            DeltaType = deltaType;
            SurrogateKey = surrogateKey;

            // create a copy of the target table without the schem or any deltaType = Ignore columns
            CacheTable = targetTransform.CacheTable.Copy(true, true);
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

            SetInTransform(inReader, targetTransform);
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

        private bool _firstRead;
        private bool _truncateComplete;
        private bool _defaultRowAdded;
        private bool _targetOpen;
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
        private int _isCurrentOrdinal;

        private int _sourceValidFromOrdinal;
        private int _sourceValidToOrdinal;
        private int _sourceIsCurrentOrdinal;

        private int _referenceSurrogateKeyOrdinal;
        private int _referenceIsValidOrdinal;
        private int _referenceCreateAudit;
        private int _referenceCreateDate;

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

        public override bool PassThroughColumns => true;

        public override bool InitializeOutputFields()
        {
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
            _targetOpen = true;

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
            for (var targetOrdinal = 1; targetOrdinal < columnCount; targetOrdinal++)
            {
                _sourceOrdinals.Add(PrimaryTransform.GetOrdinal(CacheTable.Columns[targetOrdinal].Name));
            }

            return true;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
            {
                var returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);
                return returnValue;
                }
            else
            {
                if (query == null)
                    query = new SelectQuery();

                query.Sorts = RequiredSortFields();

                var returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);
                return returnValue;
            }

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
            _isCurrentOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.IsCurrentField);

            _sourceSurrogateKeyOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.SourceSurrogateKey);
            _sourceValidFromOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidFromDate);
            _sourceValidToOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidToDate);
            _sourceIsCurrentOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.IsCurrentField);
            _columnCount = CacheTable.Columns.Count;

            _referenceIsValidOrdinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.IsCurrentField);
            _referenceSurrogateKeyOrdinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.SurrogateKey);
            _referenceCreateAudit = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.CreateAuditKey);
            _referenceCreateDate = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.CreateDate);

        }

        private bool CompareFields(object sourceValue, TableColumn targetColumn, object targetValue)
        {
            var source = sourceValue;
            var target = targetValue;

            if (sourceValue is EncryptedObject sourceObject)
            {
                switch (targetColumn.SecurityFlag)
                {
                    case TableColumn.ESecurityFlag.FastEncrypted:
                        target = FastDecrypt(targetValue);
                        source = sourceObject.OriginalValue; 
                        break;
                    case TableColumn.ESecurityFlag.StrongEncrypted:
                        target = StrongDecrypt(targetValue);
                        source = sourceObject.OriginalValue; 
                        break;
                    case TableColumn.ESecurityFlag.OnWayHashed:
                        return OneWayHashCompare(targetValue, sourceObject.OriginalValue);
                }
            }

            return Compare(targetColumn.DataType, source, target) == ECompareResult.Equal;
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
                    var targetOrdinal = CacheTable.GetOrdinal(col.Name);
                    try
                    {
                        var result = CompareFields(ReferenceTransform[col.Name], col, newRow[targetOrdinal]);
                        if (!result)
                        {
                            isMatch = false;
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new TransformException($"The delta transform {Name} failed as column {col.Name} contains incompatible values.", ex, ReferenceTransform[col.Name], newRow[targetOrdinal]);
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
                        var targetOrdinal = PrimaryTransform.GetOrdinal(col.Name); //ignore any comparisons on columns that do not exist in source.
                        if (targetOrdinal > -1)
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
                    _targetOpen = await ReferenceRead(cancellationToken);
                }
                else
                    _targetOpen = false;
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

            //this loop continues when there are matching source target rows.
            while (true)
            {
                //if the primary table has finished reading any remaining rows in the target will be deletes.
                if (!_primaryOpen)
                {
                    if (!DoDelete || !_targetOpen)
                        return null; //not checking deletes, then finish.
                                     //if there are still more records in the target table, then everything else is a delete.
                    newRow = CreateDeleteRow();
                    _targetOpen = await ReferenceRead(cancellationToken);
                    return newRow;
                }

                //check if the natrual key in the source & target are less/match/greater to determine operation
                var compareResult = ECompareResult.Less;

                if (!_targetOpen)
                {
                    //if target reader has finished, theen the natrual key compare will always be not-equal.
                    compareResult = ECompareResult.Less;
                }
                else
                {
                    foreach (var col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey || c.DeltaType == TableColumn.EDeltaType.ValidToDate))
                    {
                        var targetOrdinal = PrimaryTransform.GetOrdinal(col.Name); //ignore any comparisons on columns that do not exist in source.
                        if (targetOrdinal > -1)
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

                }

                //if the primary greater in sort order than the target, then the target row has been deleted.
                if (compareResult == ECompareResult.Greater && DoDelete)
                {
                    newRow = CreateDeleteRow();
                    _targetOpen = await ReferenceRead(cancellationToken);
                    return newRow;
                }

                //if compare result is greater, and not checking deletes.  Move the target table to the next row and test again.
                if (compareResult == ECompareResult.Greater)
                {
                    _targetOpen = await ReferenceRead(cancellationToken);
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

                    //the final possibility, is the natrual key is a match, check for changed tracking columns
                    var isMatch = true;
                    foreach (var col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.TrackingField))
                    {
                        var targetOrdinal = ReferenceTransform.GetOrdinal(col.Name); //ignore any comparisons on columns that do not exist in source.
                        if (targetOrdinal > -1)
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

                    if (!isMatch)
                    {
                        //if the record has changed and preserve history is on, then there will be two output operations.
                        if (DoPreserve)
                        {
                            //store this in the preserve field, so it is written on the next read operation.
                            PreserveRow = CreateOutputRow('C');
                            newRow = CreateDeleteRow(PreserveRow);
                        }
                        else
                        {
                            newRow = CreateOutputRow('U');

                            //keep the surrogoate key, create date, and create audit.  update the rest.

                            if (_colSurrogateKey != null)
                                newRow[CacheTable.GetOrdinal(_colSurrogateKey.Name)] = ReferenceTransform[_referenceSurrogateKeyOrdinal];
                            if (_colCreateAuditKey != null)
                                newRow[CacheTable.GetOrdinal(_colCreateAuditKey.Name)] = ReferenceTransform[_referenceCreateAudit];
                            if (_colCreateDate != null)
                                newRow[CacheTable.GetOrdinal(_colCreateDate.Name)] = ReferenceTransform[_referenceCreateDate];
                        }

                        //move primary and target readers to the next record.
                        _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                        _targetOpen = await ReferenceRead(cancellationToken);
                    }
                    else
                    {
                        //if we have a full record match, then the record is to be skipped, and the next record read.
                        TransformRowsIgnored++;
                        _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                        _targetOpen = await ReferenceRead(cancellationToken);
                        continue; //continue the loop
                    }
                }


                //the delta always puts a sort against natural key, so duplicates rows will be next to each other.
                //check the newRow against the previous row in the file to deduplicate any matching natural keys.
                if (PreserveRow == null && _primaryOpen && CompareNewRowPrevious(newRow))
                {
                    //if the row is a match against the tracking keys then just ignore it.
                    if (CompareNewRowPreviousValues(newRow))
                        continue;
                    //if the previous row is a match, and the tracking field values are different, then either updated it or ignore it.
                    if (DoPreserve)
                    {
                        TransformRowsPreserved++;
                        if (_isCurrentOrdinal >= 0)
                            newRow[_isCurrentOrdinal] = false;
                        newRow[0] = 'C';
                    }
                    else
                    {
                        continue;
                    }

                    for (var i = 1; i < _columnCount; i++)
                    {
                        if (CacheTable.Columns[i].DeltaType == TableColumn.EDeltaType.IsCurrentField)
                            newRow[i] = false;
                    }
                }

                return newRow;
            }
        }

        //reads reference rows, ignoring any rows where iscurrent = false and the surrogateKey = 0
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
                    var targetOrdinal = CacheTable.GetOrdinal(col.Name);
                    if(!Equals(PrimaryTransform[sourceOrdinal], newRow[targetOrdinal]))
                        return false;
                }
            }

            return true;
        }

        private bool CompareNewRowPreviousValues(object[] newRow)
        {
            //the final possibility, is the natrual key is a match, check for changed tracking columns
            var isMatch = true;
            foreach (var col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.TrackingField || c.DeltaType == TableColumn.EDeltaType.ValidFromDate || c.DeltaType == TableColumn.EDeltaType.ValidToDate))
            {
                var sourceOrdinal = MatchingSourceOrdinal(col);
                if (sourceOrdinal > -1)
                {
                    var targetOrdinal = CacheTable.GetOrdinal(col.Name);
                    try
                    {
                        var result = Compare(col.DataType, PrimaryTransform[sourceOrdinal], newRow[targetOrdinal]);

                        if (result != ECompareResult.Equal)
                        {
                            isMatch = false;
                            break;
                        }
                    }
                    catch(Exception ex)
                    {
                        throw new TransformException($"The delta transform failed comparing incompatible values on column {col.Name}.  {ex.Message}", PrimaryTransform[sourceOrdinal], newRow[targetOrdinal]);

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
            for (var targetOrdinal = 1; targetOrdinal < _columnCount; targetOrdinal++)
            {
                //check if a matching source field exists (-1 will be returned if it doesn't)
                var sourceOrdinal = _sourceOrdinals[targetOrdinal - 1];

                switch (CacheTable.Columns[targetOrdinal].DeltaType)
                {
                    case TableColumn.EDeltaType.ValidFromDate:
                        if (_sourceValidFromOrdinal == -1 && sourceOrdinal == -1)
                            newRow[targetOrdinal] = _currentDateTime;
                        else if(sourceOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[sourceOrdinal];
                        else
                            newRow[targetOrdinal] = PrimaryTransform[_sourceValidFromOrdinal];

                        break;
                    case TableColumn.EDeltaType.ValidToDate:
                        if (_sourceValidToOrdinal == -1 && sourceOrdinal == -1)
                            newRow[targetOrdinal] = new DateTime(2099, 12, 31, 23, 59, 59);
                        else if(sourceOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[sourceOrdinal];
                        else
                            newRow[targetOrdinal] = PrimaryTransform[_sourceValidToOrdinal];
                        break;
                    case TableColumn.EDeltaType.CreateDate:
                    case TableColumn.EDeltaType.UpdateDate:
                        newRow[targetOrdinal] = _currentDateTime;
                        break;
                    case TableColumn.EDeltaType.IsCurrentField:
                        if (_sourceIsCurrentOrdinal == -1 && sourceOrdinal == -1)
                            newRow[targetOrdinal] = true;
                        else if (sourceOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[_sourceIsCurrentOrdinal];
                        else
                            newRow[targetOrdinal] = PrimaryTransform[_sourceIsCurrentOrdinal];
                        break;
                    case TableColumn.EDeltaType.SurrogateKey:
                        SurrogateKey++; //increment now that key has been used.
                        newRow[targetOrdinal] = SurrogateKey;
                        break;
                    case TableColumn.EDeltaType.SourceSurrogateKey:
                        if (_colSurrogateKey == null && sourceOrdinal == -1)
                            newRow[targetOrdinal] = 0;
                        else if (sourceOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[sourceOrdinal];
                        else if (_sourceSurrogateKeyOrdinal == -1)
                            newRow[targetOrdinal] = 0;
                        else
                            newRow[targetOrdinal] = PrimaryTransform[_sourceSurrogateKeyOrdinal];
                        break;
                    case TableColumn.EDeltaType.CreateAuditKey:
                    case TableColumn.EDeltaType.UpdateAuditKey:
                    case TableColumn.EDeltaType.AzurePartitionKey:
                        newRow[targetOrdinal] = AuditKey;
                        break;
                    case TableColumn.EDeltaType.IgnoreField:
                        //do nothing
                        break;
                    case TableColumn.EDeltaType.ValidationStatus:
                        if(_validationStatusOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[_validationStatusOrdinal];
                        break;
                    case TableColumn.EDeltaType.RejectedReason:
                        if (_rejectedReasonOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[_rejectedReasonOrdinal];
                        break;
                    default:
                        timer.Start();

                        if (sourceOrdinal > -1)
                        {
                            newRow[targetOrdinal] = PrimaryTransform[sourceOrdinal];
                        }
                        else
                        {
                            newRow[targetOrdinal] = CacheTable.Columns[targetOrdinal].DefaultValue;
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
            for (var targetOrdinal = 1; targetOrdinal < _columnCount; targetOrdinal++)
            {
                var targetColumn = CacheTable.Columns[targetOrdinal];

                if (targetColumn != null)
                {
                    switch (targetColumn.DeltaType)
                    {
                        case TableColumn.EDeltaType.ValidFromDate:
                            if (string.IsNullOrEmpty(targetColumn.DefaultValue))
                                newRow[targetOrdinal] = new DateTime(1900, 01, 01);
                            else
                                newRow[targetOrdinal] = targetColumn.DefaultValue;
                            break;
                        case TableColumn.EDeltaType.ValidToDate:
                            if (string.IsNullOrEmpty(targetColumn.DefaultValue))
                                newRow[targetOrdinal] = new DateTime(2099, 12, 31);
                            else
                                newRow[targetOrdinal] = targetColumn.DefaultValue;
                            break;
                        case TableColumn.EDeltaType.CreateDate:
                        case TableColumn.EDeltaType.UpdateDate:
                            newRow[targetOrdinal] = _currentDateTime;
                            break;
                        case TableColumn.EDeltaType.IsCurrentField:
                            newRow[targetOrdinal] = true;
                            break;
                        case TableColumn.EDeltaType.SurrogateKey:
                            SurrogateKey++; //increment now that key has been used.
                            newRow[targetOrdinal] = 0;
                            break;
                        case TableColumn.EDeltaType.SourceSurrogateKey:
                            newRow[targetOrdinal] = 0;
                            break;
                        case TableColumn.EDeltaType.CreateAuditKey:
                        case TableColumn.EDeltaType.UpdateAuditKey:
                        case TableColumn.EDeltaType.AzurePartitionKey:
                            newRow[targetOrdinal] = AuditKey;
                            break;
                        case TableColumn.EDeltaType.IgnoreField:
                            //do nothing
                            break;
                        case TableColumn.EDeltaType.NaturalKey:
                            if (string.IsNullOrWhiteSpace(targetColumn.DefaultValue))
                                throw new Exception("A default column could not be created as the column \"" + targetColumn.Name + "\" is part of the natural key and has a default value of null.  Edit the target table columns and set the default value to a non-null value to continue.");
                            else
                                newRow[targetOrdinal] = targetColumn.DefaultValue;
                            break;
                        default:
                            newRow[targetOrdinal] = targetColumn.DefaultValue;
                            break;
                    }
                }
            }

            return newRow;
        }

        public class JoinKeyComparer : IComparer<object[]>
        {
            public int Compare(object[] x, object[] y)
            {
                for (var i = 0; i < x.Length; i++)
                {
                    if (object.Equals(x[i], y[i])) continue;

                    var greater = false;

                    if (x[i] is byte)
                        greater = (byte)x[i] > (byte)y[i];
                    if (x[i] is sbyte)
                        greater = (sbyte)x[i] > (sbyte)y[i];
                    if (x[i] is ushort)
                        greater = (ushort)x[i] > (ushort)y[i];
                    if (x[i] is uint)
                        greater = (uint)x[i] > (uint)y[i];
                    if (x[i] is ulong)
                        greater = (ulong)x[i] > (ulong)y[i];
                    if (x[i] is short)
                        greater = (short)x[i] > (short)y[i];
                    if (x[i] is int)
                        greater = (int)x[i] > (int)y[i];
                    if (x[i] is long)
                        greater = (long)x[i] > (long)y[i];
                    if (x[i] is decimal)
                        greater = (decimal)x[i] > (decimal)y[i];
                    if (x[i] is double)
                        greater = (double)x[i] > (double)y[i];
                    if (x[i] is string)
                        greater = string.CompareOrdinal((string)x[i], (string)y[i]) > 0;
                    if (x[i] is bool)
                        greater = (bool)x[i] == false && (bool)y[i];
                    if (x[i] is DateTime)
                        greater = (DateTime)x[i] > (DateTime)y[i];

                    if (greater)
                        return 1;
                    return -1;
                }
                return 0;
            }
        }

        public override bool ResetTransform()
        {
            _firstRead = true;
            _targetOpen = true;
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
                var validTo = ReferenceTransform.CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidToDate);
                if (validTo != null)
                {
                    var primaryValidTo = PrimaryTransform.CacheTable.Columns[validTo.Name];
                    if (primaryValidTo != null)
                    {
                        fields.Add(new Sort(primaryValidTo));
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
                var validTo = ReferenceTransform.CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidToDate);
                if (validTo != null)
                    fields.Add(new Sort(validTo));
            }

            return fields;
        }
    }
}
