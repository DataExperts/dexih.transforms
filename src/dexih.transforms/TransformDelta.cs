using dexih.functions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    public class TransformDelta : Transform
    {
        public TransformDelta(Transform inReader, Transform targetTransform, EUpdateStrategy deltaType, Int64 surrogateKey, bool addDefaultRow)
        {
            DeltaType = deltaType;
            SurrogateKey = surrogateKey;
            CacheTable = targetTransform.CacheTable.Copy(true);
            AddDefaultRow = addDefaultRow;

            doUpdate = false;
            doDelete = false;
            doPreserve = false;

            if (deltaType == EUpdateStrategy.AppendUpdate || deltaType == EUpdateStrategy.AppendUpdateDelete || deltaType == EUpdateStrategy.AppendUpdateDeletePreserve || deltaType == EUpdateStrategy.AppendUpdatePreserve)
                doUpdate = true;

            if (deltaType == EUpdateStrategy.AppendUpdateDelete || deltaType == EUpdateStrategy.AppendUpdateDeletePreserve)
                doDelete = true;

            if (deltaType == EUpdateStrategy.AppendUpdateDeletePreserve || deltaType == EUpdateStrategy.AppendUpdatePreserve)
                doPreserve = true;

            SetInTransform(inReader, targetTransform);
        }

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

        bool _firstRead;
        bool _truncateComplete;
        bool _defaultRowAdded;
        bool _targetOpen;
        bool _primaryOpen;

        private TableColumn colValidFrom;
        private TableColumn colValidTo;
        private TableColumn colCreateDate;
        private TableColumn colUpdateDate;
        private TableColumn colSurrogateKey;
        private TableColumn colIsCurrentField;
        private TableColumn colSourceSurrogateKey;
        private TableColumn colCreateAuditKey;
        private TableColumn colUpdateAuditKey;

        //preload ordinals to improve performance.
        private int DatabaseOperationOrdinal;
        private int RejectedReasonOrdinal;
        private int ValidationStatusOrdinal;
        private int SourceSurrogateKeyOrdinal;
        private int ValidFromOrdinal;
        private int IsCurrentOrdinal;

        private int SourceValidFromOrdinal;
        private int SourceValidToOrdinal;
        private int SourceIsCurrentOrdinal;

        private int ReferenceSurrogateKeyOrdinal;
        private int ReferenceIsValidOrdinal;
        private int ReferenceCreateAudit;
        private int ReferenceCreateDate;

        private int ColumnCount;

        private TableColumn[] colNatrualKey;

        private EUpdateStrategy DeltaType { get; set; }
        public Int64 SurrogateKey { get; protected set; }
        public bool AddDefaultRow { get; set; }
        

        private bool doUpdate { get; set; }
        private bool doDelete { get; set; }
        private bool doPreserve { get; set; }

        private object[] _preserveRow { get; set; }
        private object[] _nextPrimaryRow { get; set; }

        private List<int> _sourceOrdinals;

        DateTime currentDateTime;

        public override bool PassThroughColumns => true;

        public override bool InitializeOutputFields()
        {
            if (ReferenceTransform == null)
                throw new Exception("There must be a target table specified.");

            //add the operation type, which indicates whether record are C-create/U-update or D-Deletes
            if (CacheTable.Columns.SingleOrDefault(c => c.DeltaType == TableColumn.EDeltaType.DatabaseOperation) == null)
            {
                CacheTable.Columns.Insert(0, new TableColumn("Operation", DataType.ETypeCode.Byte)
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
            if (doPreserve == true && colSurrogateKey == null)
                throw new Exception("The delta transform requires the table to have a single surrogate key field for row preservations to be possible.");

            if (doUpdate == true && CacheTable.Columns.Where(c=>c.DeltaType == TableColumn.EDeltaType.NaturalKey).Count() == 0)
                throw new Exception("The delta transform requires the table to have at least ont natrual key field for updates to be possible.");

            //set surrogate key to the key field.  This will indicate that the surrogate key should be used when update/deleting records.
            if(colSurrogateKey != null)
                CacheTable.KeyFields = new List<string>() { colSurrogateKey.ColumnName };

            //preload the source-target ordinal mapping to improve performance.
            _sourceOrdinals = new List<int>();
            int columnCount = CacheTable.Columns.Count;
            for (int targetOrdinal = 1; targetOrdinal < columnCount; targetOrdinal++)
            {
                _sourceOrdinals.Add(PrimaryTransform.GetOrdinal(CacheTable.Columns[targetOrdinal].ColumnName));
            }

            return true;
        }

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query)
        {
            AuditKey = auditKey;

            if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
            {
                var returnValue = await PrimaryTransform.Open(auditKey, query);
                return returnValue;
                }
            else
            {
                if (query == null)
                    query = new SelectQuery();

                query.Sorts = RequiredSortFields();

                var returnValue = await PrimaryTransform.Open(auditKey, query);
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
                else
                {
                    return true;
                }
            }
        }

        private void SetAuditColumns()
        {
            //add the audit columns if they don't exist
            //get some of the key fields to save looking up for each row.
            colValidFrom = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidFromDate);
            colValidTo = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidToDate);
            colCreateDate = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.CreateDate);
            colUpdateDate = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.UpdateDate);
            colSurrogateKey = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);
            colIsCurrentField = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.IsCurrentField);
            colSourceSurrogateKey = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.SourceSurrogateKey);
            colCreateAuditKey = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.CreateAuditKey);
            colUpdateAuditKey = CacheTable.GetDeltaColumn(TableColumn.EDeltaType.UpdateAuditKey);
            DatabaseOperationOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);
            ValidationStatusOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidationStatus);
            colNatrualKey = CacheTable.Columns.Where(c=>c.DeltaType == TableColumn.EDeltaType.NaturalKey).ToArray();

            RejectedReasonOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.RejectedReason);
            if(CacheTable.GetDeltaColumn(TableColumn.EDeltaType.RejectedReason) == null)
            {
                CacheTable.AddColumn("RejectedReason", DataType.ETypeCode.String, TableColumn.EDeltaType.RejectedReason);
            }

            SourceSurrogateKeyOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.SourceSurrogateKey);
            ValidFromOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidFromDate);
            IsCurrentOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.IsCurrentField);

            SourceValidFromOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidFromDate);
            SourceValidToOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidToDate);
            SourceIsCurrentOrdinal = PrimaryTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.IsCurrentField);
            ColumnCount = CacheTable.Columns.Count;

            ReferenceIsValidOrdinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.IsCurrentField);
            ReferenceSurrogateKeyOrdinal = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.SurrogateKey);
            ReferenceCreateAudit = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.CreateAuditKey);
            ReferenceCreateDate = ReferenceTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.CreateDate);

        }


        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            try
            {
                object[] newRow = null;
                currentDateTime = DateTime.Now; //this is created here an ensure all datetime records in the row match exactly.

                //if the delta is set to reload.  Set the first row as an operation T="truncate table"
                if (DeltaType == EUpdateStrategy.Reload && _truncateComplete == false)
                {
                    newRow = new object[ColumnCount];
                    newRow[0] = 'T';

                    _truncateComplete = true;
                    return new ReturnValue<object[]>(true, newRow);
                }

                //very first action is to add a defaultRow
                if(!_defaultRowAdded && AddDefaultRow)
                {
                    _defaultRowAdded = true;

                    newRow = CreateDefaultRow();

                    //if this is a truncate job, always add the default row
                    if (DeltaType == EUpdateStrategy.Reload)
                        return new ReturnValue<object[]>(true, newRow);

                    //query the reference transform to check if the row already exists.
                    List<Filter> filters = new List<Filter>();

                    //lookup the default value by the surrogate key (always = 0) or natrual key if a target surrogate key does not exist.
                    if (colSurrogateKey != null)
                    {
                        filters.Add(new Filter(colSurrogateKey, Filter.ECompare.IsEqual, "0"));
                    }
                    else
                    {
                        var naturalKey = CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey);

                        if(naturalKey.Count()==0)
                        {
                            return new ReturnValue<object[]>(false, "The datalink cannot run as there are no natural key columns specified on the target table.", null);
                        }

                        foreach (TableColumn col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey))
                        {
                            if(col.DefaultValue == null)
                            {
                                return new ReturnValue<object[]>(false, "The datalink could not be run due to a null default value on the column " + col.ColumnName + ".  Edit the table columns and specify a default value, or switch of the generate default row setting for the datalink", null);
                            }
                            filters.Add(new Filter(col, Filter.ECompare.IsEqual, col.DefaultValue));
                        }
                    }

                    SelectQuery query = new SelectQuery() { Filters = filters };

                    var referenceOpenResult = await ReferenceTransform.Open(AuditKey, query);
                    if (!referenceOpenResult.Success)
                        return new ReturnValue<object[]>(referenceOpenResult);

                    //if no row in the reference transform, then return the created default value.
                    if(!await ReferenceTransform.ReadAsync())
                    {
                        return new ReturnValue<object[]>(true, newRow);
                    }
                    else
                    {
                        //if the default row exists, compare the tracking columns to determine if an update is neccessary.
                        bool isMatch = true;
                        foreach (TableColumn col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.TrackingField))
                        {
                            int targetOrdinal = CacheTable.GetOrdinal(col.ColumnName);
                            var result = functions.DataType.Compare(col.DataType, ReferenceTransform[col.ColumnName], newRow[targetOrdinal]);
                            if (result.Success == false)
                                throw new Exception("Data type comparison error: " + result.Message, result.Exception);

                            if (result.Value != DataType.ECompareResult.Equal)
                            {
                                isMatch = false;
                                break;
                            }
                        }

                        //columns do not match, so do an update
                        if(!isMatch)
                        {
                            newRow[0] = 'U';
                            return new ReturnValue<object[]>(true, newRow);
                        }

                        //rows is ignored.
                        TransformRowsIgnored++;
                    }
                }

                //second action is to read a record from the primary transform.
                if (_firstRead)
                {
                    _firstRead = false;

                    //read a row from the primary and target table
                    _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);

                    //create a filter that will be passed (if supported to the database).  Improves performance.
                    List<Filter> filters = new List<Filter>();

                    //first add a where IsCurrentField = true
                    //if (colIsCurrentField != null)
                    //    filters.Add(new Filter(colIsCurrentField, Filter.ECompare.IsEqual, true));

                    //second add a where natrual key is greater than the first record key.  (excluding where delete detection is on).
                    if (_primaryOpen && !doDelete)
                    {
                        foreach (TableColumn col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey))
                        {
                            int targetOrdinal = PrimaryTransform.GetOrdinal(col.ColumnName); //ignore any comparisons on columns that do not exist in source.
                            if (targetOrdinal > -1)
                            {
                                filters.Add(new Filter(col, Filter.ECompare.GreaterThanEqual, PrimaryTransform[col.ColumnName]));
                            }
                        }
                    }

                    SelectQuery query = new SelectQuery() { Filters = filters };

                    if (doUpdate || doDelete || doPreserve)
                    {
                        ReferenceTransform.Dispose();
                        await ReferenceTransform.Open(AuditKey, query);
                        _targetOpen = await ReferenceRead(cancellationToken);
                    }
                    else
                        _targetOpen = false;
                }

                //if row is marked reject, then just push it through.
                if(_primaryOpen && DatabaseOperationOrdinal >= 0 && (char)PrimaryTransform[DatabaseOperationOrdinal] == 'R')
                {
                    newRow = CreateOutputRow('R');
                    _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                    return new ReturnValue<object[]>(true, newRow);
                }

                //if there are no updates. logic is simple, just push the source records through to the target.
                if (!doUpdate)
                {
                    if (_primaryOpen)
                    {
                        newRow = CreateOutputRow('C');
                        _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                        return new ReturnValue<object[]>(true, newRow);
                    }
                    else
                        return new ReturnValue<object[]>(false, null);
                }

                //if there is a saved row (due to a preserve operation splitting a row into update/insert operations) write it out
                if (_preserveRow != null)
                {
                    var returnValue = new ReturnValue<object[]>(true, _preserveRow);
                    _preserveRow = null;
                    return returnValue;
                }

                //this loop continues when there are matching source target rows.
                while (true)
                {
                    //if the primary table has finished reading any remaining rows in the target will be deletes.
                    if (!_primaryOpen)
                    {
                        if (!doDelete || !_targetOpen)
                            return new ReturnValue<object[]>(false, null); //not checking deletes, then finish.
                        else
                        {
                            //if there are still more records in the target table, then everything else is a delete.
                            newRow = CreateDeleteRow();
                            _targetOpen = await ReferenceRead(cancellationToken);
                            return new ReturnValue<object[]>(true, newRow);
                        }
                    }

                    //check if the natrual key in the source & target are less/match/greater to determine operation
                    DataType.ECompareResult compareResult = DataType.ECompareResult.Less;

                    if (!_targetOpen)
                    {
                        //if target reader has finished, theen the natrual key compare will always be not-equal.
                        compareResult = DataType.ECompareResult.Less;
                    }
                    else
                    {
                        foreach (TableColumn col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey || c.DeltaType == TableColumn.EDeltaType.ValidToDate))
                        {
                            int targetOrdinal = PrimaryTransform.GetOrdinal(col.ColumnName); //ignore any comparisons on columns that do not exist in source.
                            if (targetOrdinal > -1)
                            {
                                var result = functions.DataType.Compare(col.DataType, PrimaryTransform[col.ColumnName], ReferenceTransform[col.ColumnName]);
                                if (result.Success == false)
                                    throw new Exception("Data type comparison error: " + result.Message, result.Exception);

                                compareResult = result.Value;
                                if (compareResult != functions.DataType.ECompareResult.Equal)
                                    break;
                            }
                        }

                    }

                    //if the primary greater in sort order than the target, then the target row has been deleted.
                    if (compareResult == DataType.ECompareResult.Greater && doDelete)
                    {
                        newRow = CreateDeleteRow();
                        _targetOpen = await ReferenceRead(cancellationToken);
                        return new ReturnValue<object[]>(true, newRow);
                    }

                    //if compare result is greater, and not checking deletes.  Move the target table to the next row and test again.
                    if (compareResult == DataType.ECompareResult.Greater)
                    {
                        _targetOpen = await ReferenceRead(cancellationToken);
                        continue;
                    }


                    //if not checking deletes and not equal, than this is a new row.  
                    if (compareResult != DataType.ECompareResult.Equal)
                    {
                        newRow = CreateOutputRow('C');
                        _primaryOpen = await PrimaryTransform.ReadAsync(cancellationToken);
                    }
                    else
                    {

                        //the final possibility, is the natrual key is a match, check for changed tracking columns
                        bool isMatch = true;
                        foreach (TableColumn col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.TrackingField))
                        {
                            int targetOrdinal = ReferenceTransform.GetOrdinal(col.ColumnName); //ignore any comparisons on columns that do not exist in source.
                            if (targetOrdinal > -1)
                            {
                                var result = functions.DataType.Compare(col.DataType, PrimaryTransform[col.ColumnName], ReferenceTransform[col.ColumnName]);
                                if (result.Success == false)
                                    throw new Exception("Data type comparison error: " + result.Message, result.Exception);

                                if (result.Value != DataType.ECompareResult.Equal)
                                {
                                    isMatch = false;
                                    break;
                                }
                            }
                        }

                        if (!isMatch)
                        {
                            //if the record has changed and preserve history is on, then there will be two output operations.
                            if (doPreserve)
                            {
                                //store this in the preserve field, so it is written on the next read operation.
                                _preserveRow = CreateOutputRow('C');
                                newRow = CreateDeleteRow(_preserveRow);
                            }
                            else
                            {
                                newRow = CreateOutputRow('U');

                                //keep the surrogoate key, create date, and create audit.  update the rest.

                                if(colSurrogateKey != null )
                                    newRow[CacheTable.GetOrdinal(colSurrogateKey.ColumnName)] = ReferenceTransform[ReferenceSurrogateKeyOrdinal];
                                if(colCreateAuditKey != null)
                                    newRow[CacheTable.GetOrdinal(colCreateAuditKey.ColumnName)] = ReferenceTransform[ReferenceCreateAudit];
                                if(colCreateDate != null)
                                    newRow[CacheTable.GetOrdinal(colCreateDate.ColumnName)] = ReferenceTransform[ReferenceCreateDate];
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
                    if (_preserveRow == null && _primaryOpen && CompareNewRowPrevious(newRow))
                    {
                        //if the row is a match against the tracking keys then just ignore it.
                        if (CompareNewRowPreviousValues(newRow))
                            continue;
                        else
                        {
                            //if the previous row is a match, and the tracking field values are different, then either updated it or ignore it.
                            if (doPreserve)
                            {
                                TransformRowsPreserved++;
                                if (IsCurrentOrdinal >= 0)
                                    newRow[IsCurrentOrdinal] = false;
                                newRow[0] = 'C';
                            }
                            else
                            {
                                continue;
                            }

                            for (int i = 1; i < ColumnCount; i++)
                            {
                                if (CacheTable.Columns[i].DeltaType == TableColumn.EDeltaType.IsCurrentField)
                                    newRow[i] = false;
                            }
                        }
                    }

                    return new ReturnValue<object[]>(true, newRow);
                }
            }
            catch(Exception ex)
            {
                return new ReturnValue<object[]>(false, "Error reading delta: " + ex.Message, ex);
            }
        }

        //reads reference rows, ignoring any rows where iscurrent = false and the surrogateKey = 0
        private async Task<bool> ReferenceRead(CancellationToken cancellationToken)
        {
            while (await ReferenceTransform.ReadAsync(cancellationToken))
            {
                if(colSurrogateKey != null)
                {
                    var returnValue = DataType.TryParse(DataType.ETypeCode.Int64, ReferenceTransform[ReferenceSurrogateKeyOrdinal]);
                    if (!returnValue.Success)
                        throw new Exception("The surrogate key column is expected to have a numerical value, however the value " + ReferenceTransform[ReferenceSurrogateKeyOrdinal] + " was found.");

                    //surogate key = 0, ignore as this is the defaulted value.
                    if ((long)returnValue.Value == 0)
                    {
                        continue;
                    }
                }

                //TODO: This routine skips records in the source with isucrrent = false, however this causes problem with source file that has invalid records.
                if (colIsCurrentField == null)
                {
                    return true;
                }
                else
                {
                    var returnValue = DataType.TryParse(DataType.ETypeCode.Boolean, ReferenceTransform[ReferenceIsValidOrdinal]);
                    if (!returnValue.Success)
                        throw new Exception("The column " + colIsCurrentField.ColumnName + " is expected to have a boolean value, however the value " + ReferenceTransform[colIsCurrentField.ColumnName] + " was found.");

                    //IsCurrent = false, continue to next record.
                    if (!(bool)returnValue.Value)
                    {
                        continue;
                    }
                    else
                        return true;
                }

            }
            return false;
        }

        public int MatchingSourceOrdinal(TableColumn col)
        {
            int sourceOrdinal = -1;
            switch (col.DeltaType)
            {
                case TableColumn.EDeltaType.ValidToDate:
                    sourceOrdinal = SourceValidToOrdinal;
                    break;
                case TableColumn.EDeltaType.ValidFromDate:
                    sourceOrdinal = SourceValidFromOrdinal;
                    break;
                default:
                    sourceOrdinal = PrimaryTransform.CacheTable.GetOrdinal(col.ColumnName); //ignore any comparisons on columns that do not exist in source.
                    break;
            }

            return sourceOrdinal;
        }


        public bool CompareNewRowPrevious(object[] newRow)
        {
            //check if the natrual key in the source & target are less/match/greater to determine operation
            foreach (TableColumn col in colNatrualKey)
            {

                int sourceOrdinal = MatchingSourceOrdinal(col);

                if (sourceOrdinal > -1)
                {
                    int targetOrdinal = CacheTable.GetOrdinal(col.ColumnName);
                    if(!object.Equals(PrimaryTransform[sourceOrdinal], newRow[targetOrdinal]))
                        return false;
                }
            }

            return true;
        }

        public bool CompareNewRowPreviousValues(object[] newRow)
        {
            //the final possibility, is the natrual key is a match, check for changed tracking columns
            bool isMatch = true;
            foreach (TableColumn col in CacheTable.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.TrackingField || c.DeltaType == TableColumn.EDeltaType.ValidFromDate || c.DeltaType == TableColumn.EDeltaType.ValidToDate))
            {
                int sourceOrdinal = MatchingSourceOrdinal(col);
                if (sourceOrdinal > -1)
                {
                    int targetOrdinal = CacheTable.GetOrdinal(col.ColumnName);
                    var result = functions.DataType.Compare(col.DataType, PrimaryTransform[sourceOrdinal], newRow[targetOrdinal]);
                    if (result.Success == false)
                        throw new Exception("Data type comparison error: " + result.Message, result.Exception);

                    if (result.Value != DataType.ECompareResult.Equal)
                    {
                        isMatch = false;
                        break;
                    }
                }
            }

            return isMatch;
        }

        public object[] CreateDeleteRow(object[] nextRow = null)
        {
            object[] newRow = new object[ColumnCount];
            newRow[0] = doPreserve ? 'U' : 'D';

            if (doPreserve)
                TransformRowsPreserved++;

            for (int i = 1; i < ColumnCount; i++)
            {
                switch(CacheTable.Columns[i].DeltaType)
                {
                    case TableColumn.EDeltaType.ValidToDate:
                        if (nextRow != null && colValidFrom != null)
                            newRow[i] = nextRow[ValidFromOrdinal];
                        else
                            newRow[i] = currentDateTime;
                        break;
                    case TableColumn.EDeltaType.IsCurrentField:
                        newRow[i] = false;
                        break;
                    case TableColumn.EDeltaType.UpdateDate:
                        newRow[i] = currentDateTime;
                        break;
                    case TableColumn.EDeltaType.UpdateAuditKey:
                        newRow[i] = AuditKey;
                        break;
                    default:
                        int ordinal = ReferenceTransform.GetOrdinal(CacheTable.Columns[i].ColumnName);
                        if (ordinal > -1)
                            newRow[i] = ReferenceTransform[ordinal];
                        break;
                }
            }

            return newRow;
        }

        public object[] CreateOutputRow(char operation)
        {
            object[] newRow = new object[ColumnCount];

            newRow[0] = operation;

            Stopwatch timer = new Stopwatch();
            for (int targetOrdinal = 1; targetOrdinal < ColumnCount; targetOrdinal++)
            {
                //check if a matching source field exists (-1 will be returned if it doesn't)
                int sourceOrdinal = _sourceOrdinals[targetOrdinal - 1];

                switch (CacheTable.Columns[targetOrdinal].DeltaType)
                {
                    case TableColumn.EDeltaType.ValidFromDate:
                        if (SourceValidFromOrdinal == -1 && sourceOrdinal == -1)
                            newRow[targetOrdinal] = new DateTime(1900, 01, 01);
                        else if(sourceOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[sourceOrdinal];
                        else
                            newRow[targetOrdinal] = PrimaryTransform[SourceValidFromOrdinal];

                        break;
                    case TableColumn.EDeltaType.ValidToDate:
                        if (SourceValidToOrdinal == -1 && sourceOrdinal == -1)
                            newRow[targetOrdinal] = new DateTime(2099, 12, 31);
                        else if(sourceOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[sourceOrdinal];
                        else
                            newRow[targetOrdinal] = PrimaryTransform[SourceValidToOrdinal];
                        break;
                    case TableColumn.EDeltaType.CreateDate:
                    case TableColumn.EDeltaType.UpdateDate:
                        newRow[targetOrdinal] = currentDateTime;
                        break;
                    case TableColumn.EDeltaType.IsCurrentField:
                        if (SourceIsCurrentOrdinal == -1 && sourceOrdinal == -1)
                            newRow[targetOrdinal] = true;
                        else if (sourceOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[SourceIsCurrentOrdinal];
                        else
                            newRow[targetOrdinal] = PrimaryTransform[SourceIsCurrentOrdinal];
                        break;
                    case TableColumn.EDeltaType.SurrogateKey:
                        SurrogateKey++; //increment now that key has been used.
                        newRow[targetOrdinal] = SurrogateKey;
                        break;
                    case TableColumn.EDeltaType.SourceSurrogateKey:
                        if (colSurrogateKey == null && sourceOrdinal == -1)
                            newRow[targetOrdinal] = 0;
                        else if (sourceOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[sourceOrdinal];
                        else if (SourceSurrogateKeyOrdinal == -1)
                            newRow[targetOrdinal] = 0;
                        else
                            newRow[targetOrdinal] = PrimaryTransform[SourceSurrogateKeyOrdinal];
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
                        if(ValidationStatusOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[ValidationStatusOrdinal];
                        break;
                    case TableColumn.EDeltaType.RejectedReason:
                        if (RejectedReasonOrdinal >= 0)
                            newRow[targetOrdinal] = PrimaryTransform[RejectedReasonOrdinal];
                        break;
                    default:
                        timer.Start();

                        if (sourceOrdinal > -1)
                        newRow[targetOrdinal] = PrimaryTransform[sourceOrdinal];
                        timer.Stop();

                        break;
                }
            }

            return newRow;
        }

        public object[] CreateDefaultRow()
        {
            object[] newRow = new object[ColumnCount];

            newRow[0] = 'C';

            Stopwatch timer = new Stopwatch();
            for (int targetOrdinal = 1; targetOrdinal < ColumnCount; targetOrdinal++)
            {
                var targetColumn = CacheTable.Columns[targetOrdinal];

                if (targetColumn != null)
                {
                    switch (targetColumn.DeltaType)
                    {
                        case TableColumn.EDeltaType.ValidFromDate:
                            if (String.IsNullOrEmpty(targetColumn.DefaultValue))
                                newRow[targetOrdinal] = new DateTime(1900, 01, 01);
                            else
                                newRow[targetOrdinal] = targetColumn.DefaultValue;
                            break;
                        case TableColumn.EDeltaType.ValidToDate:
                            if (String.IsNullOrEmpty(targetColumn.DefaultValue))
                                newRow[targetOrdinal] = new DateTime(2099, 12, 31);
                            else
                                newRow[targetOrdinal] = targetColumn.DefaultValue;
                            break;
                        case TableColumn.EDeltaType.CreateDate:
                        case TableColumn.EDeltaType.UpdateDate:
                            newRow[targetOrdinal] = currentDateTime;
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
                                throw new Exception("A default column could not be created as the column \"" + targetColumn.ColumnName + "\" is part of the natural key and has a default value of null.  Edit the target table columns and set the default value to a non-null value to continue.");
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
                for (int i = 0; i < x.Length; i++)
                {
                    if (object.Equals(x[i], y[i])) continue;

                    bool greater = false;

                    if (x[i] is byte)
                        greater = (byte)x[i] > (byte)y[i];
                    if (x[i] is SByte)
                        greater = (SByte)x[i] > (SByte)y[i];
                    if (x[i] is UInt16)
                        greater = (UInt16)x[i] > (UInt16)y[i];
                    if (x[i] is UInt32)
                        greater = (UInt32)x[i] > (UInt32)y[i];
                    if (x[i] is UInt64)
                        greater = (UInt64)x[i] > (UInt64)y[i];
                    if (x[i] is Int16)
                        greater = (Int16)x[i] > (Int16)y[i];
                    if (x[i] is Int32)
                        greater = (Int32)x[i] > (Int32)y[i];
                    if (x[i] is Int64)
                        greater = (Int64)x[i] > (Int64)y[i];
                    if (x[i] is Decimal)
                        greater = (Decimal)x[i] > (Decimal)y[i];
                    if (x[i] is Double)
                        greater = (Double)x[i] > (Double)y[i];
                    if (x[i] is String)
                        greater = String.Compare((String)x[i], (String)y[i]) > 0;
                    if (x[i] is Boolean)
                        greater = (Boolean)x[i] == false && (Boolean)y[i] == true;
                    if (x[i] is DateTime)
                        greater = (DateTime)x[i] > (DateTime)y[i];

                    if (greater)
                        return 1;
                    else
                        return -1;
                }
                return 0;
            }
        }

        public override ReturnValue ResetTransform()
        {
            _firstRead = true;
            _targetOpen = true;
            _primaryOpen = true;

            return new ReturnValue(true);
        }

        public override string Details()
        {
            return "Delta:" + DeltaType.ToString();
        }

        public override List<Sort> RequiredSortFields()
        {
            List<Sort> fields = new List<Sort>();

            if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
            {
            }
            else
            {
                foreach (var col in PrimaryTransform.CacheTable.GetColumnsByDeltaType(TableColumn.EDeltaType.NaturalKey))
                {
                    fields.Add(new Sort(col));
                }
                var validTo = PrimaryTransform.CacheTable.GetDeltaColumn(TableColumn.EDeltaType.ValidToDate);
                if (validTo != null)
                    fields.Add(new Sort(validTo));
            }

            return fields;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            List<Sort> fields = new List<Sort>();

            if (DeltaType == EUpdateStrategy.Append || DeltaType == EUpdateStrategy.Reload)
            {
            }
            else
            {
                foreach (var col in ReferenceTransform.CacheTable.GetColumnsByDeltaType(TableColumn.EDeltaType.NaturalKey))
                {
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
