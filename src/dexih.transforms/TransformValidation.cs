using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Text;
using System.Threading;
using dexih.functions.Exceptions;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    [Transform(
        Name = "Validation",
        Description = "Validates and cleans/rejects data.",
        TransformType = TransformAttribute.ETransformType.Validation
    )]
    public class TransformValidation : Transform
    {
        public TransformValidation() { }

        public TransformValidation(Transform inReader, Mappings mappings, bool validateDataTypes)
        {
            Mappings = mappings ?? new Mappings();
            SetInTransform(inReader);
            ValidateDataTypes = validateDataTypes;
        }

        public bool ValidateDataTypes { get; set; } = true;

        private object[] _savedRejectRow; //used as a temporary store for the pass row when a pass and reject occur.

        private bool _lastRecord = false;

        private int _rejectReasonOrdinal;
        private int _operationOrdinal;
        private int _validationStatusOrdinal;

        private List<int> _mapFieldOrdinals;
        private int _primaryFieldCount;
        private int _columnCount;

        public override string TransformName { get; } = "Validation";
        public override string TransformDetails => "Validations:" + Mappings.OfType<MapValidation>().Count() + ", Functions: " + Mappings.OfType<MapFunction>().Count();

        protected override Table InitializeCacheTable(bool mapAllReferenceColumns)
        {
            var table = new Table("Validate");
            var mappingTable = Mappings.Initialize(PrimaryTransform?.CacheTable, ReferenceTransform?.CacheTable, ReferenceTransform?.ReferenceTableAlias, mapAllReferenceColumns);

            var sourceTable = PrimaryTransform?.CacheTable;
            
            //add the operation type, which indicates whether record is rejected 'R' or 'C/U/D' create/update/delete
            if (sourceTable?.Columns.SingleOrDefault(c => c.DeltaType == TableColumn.EDeltaType.DatabaseOperation) == null)
            {
                table.Columns.Add(new TableColumn("Operation")
                {
                    DeltaType = TableColumn.EDeltaType.DatabaseOperation,
                    AllowDbNull = true
                });
            } 

            foreach (var column in mappingTable.Columns)
            {
                table.Columns.Add(column);
            }

            if (sourceTable?.Columns.SingleOrDefault(c => c.DeltaType == TableColumn.EDeltaType.RejectedReason) == null)
            {
                table.Columns.Add(new TableColumn("RejectReason")
                {
                    DeltaType = TableColumn.EDeltaType.RejectedReason,
                    AllowDbNull = true
                });
            } 

            if (sourceTable?.Columns.SingleOrDefault(c => c.DeltaType == TableColumn.EDeltaType.ValidationStatus) == null)
            {
                table.Columns.Add(new TableColumn("ValidationStatus")
                {
                    DeltaType = TableColumn.EDeltaType.ValidationStatus,
                    AllowDbNull = true
                });
            } 

            return table;
        }
        
        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;
            
            var result = true;

            if (PrimaryTransform != null)
            {
                result = await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);
                if (!result) return false;
            }

            if (ReferenceTransform != null)
            {
                result = result && await ReferenceTransform.Open(auditKey, null, cancellationToken);
            }

            //store reject column details to improve performance.
            _rejectReasonOrdinal = CacheTable.GetOrdinal(TableColumn.EDeltaType.RejectedReason);
            _operationOrdinal = CacheTable.GetOrdinal(TableColumn.EDeltaType.DatabaseOperation);
            _validationStatusOrdinal = CacheTable.GetOrdinal(TableColumn.EDeltaType.ValidationStatus);

            _primaryFieldCount = PrimaryTransform?.FieldCount ?? 0;
            _columnCount = CacheTable.Columns.Count;
            _mapFieldOrdinals = new List<int>();

            for (var i = 0; i < _primaryFieldCount; i++)
            {
                _mapFieldOrdinals.Add(GetOrdinal(PrimaryTransform.GetName(i)));
            }

            return result;
        }

        public override bool RequiresSort => false;

        public override bool ResetTransform()
        {
            _lastRecord = false;
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            //the saved reject row is when a validation outputs two rows (pass & fail).
            if (_savedRejectRow != null)
            {
                var row = _savedRejectRow;
                _savedRejectRow = null;
                return row;
            }

            if (_lastRecord)
            {
                return null;
            }

            while (await PrimaryTransform.ReadAsync(cancellationToken))
            {
                var rejectReason = new StringBuilder();
                var finalInvalidAction = TransformFunction.EInvalidAction.Pass;

                //copy row data.
                var passRow = new object[_columnCount];
                for (var i = 0; i < _primaryFieldCount; i++)
                {
                    passRow[_mapFieldOrdinals[i]] = PrimaryTransform[i];
                }

                if (passRow[_operationOrdinal] == null)
                {
                    passRow[_operationOrdinal] = 'C';
                }

                object[] rejectRow = null;

                bool passed;

                //run the validation functions
                try
                {
                    passed = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
                }
                catch (FunctionIgnoreRowException)
                {
                    passed = false;
                }
                catch (Exception ex)
                {
                    throw new TransformException(
                        $"The validation transform {Name} failed.  {ex.Message}",
                        ex);
                }

                if (!passed)
                {
                    foreach (var mapping in Mappings.OfType<MapValidation>())
                    {
                        if (!mapping.Validated(out string reason))
                        {
                            rejectReason.AppendLine(reason);

                            if (mapping.Function.InvalidAction == TransformFunction.EInvalidAction.Abend)
                            {
                                var reason1 = $"The validation rule abended as the invalid action is set to abend.  " + rejectReason;
                                throw new Exception(reason1);
                            }
                            
                            //set the final invalidation action based on priority order of other rejections.
                            finalInvalidAction = finalInvalidAction < mapping.Function.InvalidAction ? mapping.Function.InvalidAction : finalInvalidAction;

                            if (mapping.Function.InvalidAction == TransformFunction.EInvalidAction.Reject || mapping.Function.InvalidAction == TransformFunction.EInvalidAction.RejectClean)
                            {
                                //if the row is rejected, copy unmodified row to a reject row.
                                if (rejectRow == null)
                                {
                                    rejectRow = new object[CacheTable.Columns.Count];
                                    passRow.CopyTo(rejectRow, 0);
                                    rejectRow[_operationOrdinal] = 'R';
                                    TransformRowsRejected++;
                                }
                            }
                        }
                    }
                }

                if (finalInvalidAction == TransformFunction.EInvalidAction.RejectClean ||
                    finalInvalidAction == TransformFunction.EInvalidAction.Clean)
                {
                    // update the pass row with any outputs from clean functions.
                    var cleanRow = new object[_columnCount];
                    Mappings.MapOutputRow(cleanRow);
                    
                    //copy row data.
                    for (var i = 0; i < _primaryFieldCount; i++)
                    {
                        passRow[_mapFieldOrdinals[i]] = cleanRow[_mapFieldOrdinals[i]-1];
                    }

                    if (passRow[_operationOrdinal] == null)
                    {
                        passRow[_operationOrdinal] = 'C';
                    }
                }
   
                if (ValidateDataTypes)
                {
                    for (var i = 1; i < _columnCount; i++)
                    {
                        // value if the position - 1 due to the "Operation" column being in pos[0]
                        var value = passRow[i];
                        var col = CacheTable.Columns[i];

                        if (col.DeltaType == TableColumn.EDeltaType.TrackingField || col.DeltaType == TableColumn.EDeltaType.NonTrackingField)
                        {

                            if (value == null || value is DBNull)
                            {
                                if (col.AllowDbNull == false)
                                {
                                    if (rejectRow == null)
                                    {
                                        rejectRow = new object[_columnCount];
                                        passRow.CopyTo(rejectRow, 0);
                                        rejectRow[_operationOrdinal] = 'R';
                                        TransformRowsRejected++;
                                    }
                                    rejectReason.AppendLine("Column:" + col.Name + ": Tried to insert null into non-null column.");
                                    finalInvalidAction = TransformFunction.EInvalidAction.Reject;
                                }
                                passRow[i] = DBNull.Value;
                            }
                            else
                            {
                                try
                                {
                                    passRow[i] =  Operations.Parse(col.DataType, value);

                                    if(col.DataType == ETypeCode.String && col.MaxLength != null)
                                    {
                                        if(((string)passRow[i]).Length > col.MaxLength)
                                        {
                                            throw new DataTypeParseException($"The column ${col.Name} value exceeded the maximum string length of {col.MaxLength}.");
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    // if the parse fails on the column, then write out a reject record.
                                    if (rejectRow == null)
                                    {
                                        rejectRow = new object[_columnCount];
                                        passRow.CopyTo(rejectRow, 0);
                                        rejectRow[_operationOrdinal] = 'R';
                                        TransformRowsRejected++;
                                    }
                                    rejectReason.AppendLine(ex.Message);
                                    finalInvalidAction = TransformFunction.EInvalidAction.Reject;
                                }
                            }
                        }
                    }
                }

                switch(finalInvalidAction)
                {
                    case TransformFunction.EInvalidAction.Pass:
                        passRow[_validationStatusOrdinal] = "passed";
                        return passRow;
                    case TransformFunction.EInvalidAction.Clean:
                        passRow[_validationStatusOrdinal] = "cleaned";
                        return passRow;
                    case TransformFunction.EInvalidAction.RejectClean:
                        passRow[_validationStatusOrdinal] = "rejected-cleaned";
                        rejectRow[_validationStatusOrdinal] = "rejected-cleaned";
                        rejectRow[_rejectReasonOrdinal] = rejectReason.ToString();
                        _savedRejectRow = rejectRow;
                        return passRow;
                    case TransformFunction.EInvalidAction.Reject:
                        passRow[_validationStatusOrdinal] = "rejected";
                        rejectRow[_validationStatusOrdinal] = "rejected";
                        rejectRow[_rejectReasonOrdinal] = rejectReason.ToString();
                        return rejectRow;
                }

                //should never get here.
                throw new TransformException("Validation failed due to an unknown error.");
            }

            return null;

        }

    }
}
