using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Text;
using System.Threading;

namespace dexih.transforms
{
    public class TransformValidation : Transform
    {
        public TransformValidation() { }

        public TransformValidation(Transform inReader, List<Function> validations, bool validateDataTypes)
        {
            SetInTransform(inReader);
            Validations = validations;
            ValidateDataTypes = validateDataTypes;
        }

        public bool ValidateDataTypes { get; set; }

        private object[] _savedRejectRow; //used as a temporary store for the pass row when a pass and reject occur.

        private bool _lastRecord = false;

        private string _rejectReasonColumnName;
        private int _rejectReasonOrdinal;
        private int _operationOrdinal;
        private int _validationStatusOrdinal;

        private List<int> _mapFieldOrdinals;
        private int _primaryFieldCount;
        private int _columnCount;

        public List<Function> Validations
        {
            get { return Functions;  }
            set { Functions = value;  }
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = PrimaryTransform.CacheTable.Copy();

            //add the operation type, which indicates whether record is rejected 'R' or 'C/U/D' create/update/delete
            if (CacheTable.Columns.SingleOrDefault(c => c.DeltaType == TableColumn.EDeltaType.DatabaseOperation) == null)
            {
                CacheTable.Columns.Insert(0, new TableColumn("Operation", DataType.ETypeCode.Byte)
                {
                    DeltaType = TableColumn.EDeltaType.DatabaseOperation
                });
            }

            //add the rejection reason, which details the reason for a rejection.
            if (CacheTable.Columns.SingleOrDefault(c => c.DeltaType == TableColumn.EDeltaType.RejectedReason) == null)
            {
                CacheTable.Columns.Add(new TableColumn("RejectReason", DataType.ETypeCode.String)
                {
                    DeltaType = TableColumn.EDeltaType.RejectedReason
                });
            }

            //add the rejection reason, which details the reason for a rejection.
            if (CacheTable.Columns.SingleOrDefault(c => c.DeltaType == TableColumn.EDeltaType.ValidationStatus) == null)
            {
                CacheTable.Columns.Add(new TableColumn("ValidationStatus", DataType.ETypeCode.String)
                {
                    DeltaType = TableColumn.EDeltaType.ValidationStatus
                });
            }

            //store reject column details to improve performance.
            _rejectReasonOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.RejectedReason);
            if (_rejectReasonOrdinal >= 0)
                _rejectReasonColumnName = CacheTable.Columns[_rejectReasonOrdinal].ColumnName;

            _operationOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);
            _validationStatusOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ValidationStatus);

            _primaryFieldCount = PrimaryTransform.FieldCount;
            _columnCount = CacheTable.Columns.Count;
            _mapFieldOrdinals = new List<int>();
            for (int i = 0; i < _primaryFieldCount; i++)
            {
                _mapFieldOrdinals.Add(GetOrdinal(PrimaryTransform.GetName(i)));
            }



            return true;
        }

        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;


        public override ReturnValue ResetTransform()
        {
            _lastRecord = false;
            return new ReturnValue(true);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            //the saved reject row is when a validation outputs two rows (pass & fail).
            if (_savedRejectRow != null)
            {
                var row = _savedRejectRow;
                _savedRejectRow = null;
                return new ReturnValue<object[]>(true, row);
            }

            if (_lastRecord)
                return new ReturnValue<object[]>(false, null);

            while (await PrimaryTransform.ReadAsync(cancellationToken))
            {
                StringBuilder rejectReason = new StringBuilder();
                Function.EInvalidAction finalInvalidAction = Function.EInvalidAction.Pass;

                //copy row data.
                object[] passRow = new object[_columnCount];
                for (int i = 0; i < _primaryFieldCount; i++)
                {
                    passRow[_mapFieldOrdinals[i]] = PrimaryTransform[i];
                }

                if (passRow[_operationOrdinal] == null)
                    passRow[_operationOrdinal] = 'C';

                object[] rejectRow = null;

                //run the validation functions
                if (Validations != null)
                {
                    foreach (Function validation in Validations)
                    {
                        //set inputs for the validation function
                        foreach (Parameter input in validation.Inputs.Where(c => c.IsColumn))
                        {
                            var result = input.SetValue(PrimaryTransform[input.Column.SchemaColumnName()]);
                            if (result.Success == false)
                                throw new Exception("Error setting validation values: " + result.Message);
                        }
                        var invokeresult = validation.Invoke();

                        //if the validation function had an error, then throw exception.
                        if (invokeresult.Success == false)
                            throw new Exception("Error invoking validation function: " + invokeresult.Message);

                        //if the validation is negative.  apply any output columns, and set a reject status
                        if ((bool)invokeresult.Value == false)
                        {
                            rejectReason.AppendLine("function: " + validation.FunctionName + ", parameters: " + string.Join(",", validation.Inputs.Select(c => c.Name + "=" + (c.IsColumn ? c.Column.SchemaColumnName() : c.Value.ToString())).ToArray()) + ".");

                            // fail job immediately.
                            if (validation.InvalidAction == Function.EInvalidAction.Abend)
                                throw new Exception(rejectReason.ToString());

                            //if the record is to be discarded, continue the loop and get the next source record.
                            if (validation.InvalidAction == Function.EInvalidAction.Discard)
                                continue;

                            //set the final invalidation action based on priority order of other rejections.
                            finalInvalidAction = finalInvalidAction < validation.InvalidAction ? validation.InvalidAction : finalInvalidAction;

                            if (validation.InvalidAction == Function.EInvalidAction.Reject || validation.InvalidAction == Function.EInvalidAction.RejectClean)
                            {
                                //if the row is rejected, copy unmodified row to a reject row.
                                if (rejectRow == null)
                                {
                                    rejectRow = new object[CacheTable.Columns.Count];
                                    passRow.CopyTo(rejectRow, 0);
                                    rejectRow[_operationOrdinal] = 'R';
                                    TransformRowsRejected++;
                                }

                                //add a reject reason if it exists
                                if (_rejectReasonOrdinal >= 0)
                                {
                                    if (validation.Outputs != null)
                                    {
                                        Parameter param = validation.Outputs.SingleOrDefault(c => c.Column.SchemaColumnName() == _rejectReasonColumnName);
                                        if (param != null)
                                        {
                                            rejectReason.Append("  Reason: " + (string)param.Value);
                                        }
                                    }
                                }
                            }

                            if (validation.InvalidAction == Function.EInvalidAction.Clean || validation.InvalidAction == Function.EInvalidAction.RejectClean)
                            {
                                validation.ReturnValue();
                                if (validation.Outputs != null)
                                {
                                    foreach (Parameter output in validation.Outputs)
                                    {
                                        if (output.Column.SchemaColumnName() != "")
                                        {
                                            int ordinal = CacheTable.GetOrdinal(output.Column.SchemaColumnName());
                                            TableColumn col = CacheTable[output.Column.SchemaColumnName()];
                                            if (ordinal >= 0)
                                            {
                                                var parseresult = DataType.TryParse(col.Datatype, output.Value, col.MaxLength);
                                                if (!parseresult.Success)
                                                    throw new Exception("Error parsing the cleaned value: " + output.Value.ToString() + " as a datatype: " + col.Datatype.ToString());

                                                passRow[ordinal] = parseresult.Value;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (ValidateDataTypes && (finalInvalidAction == Function.EInvalidAction.Pass || finalInvalidAction == Function.EInvalidAction.Clean))
                {
                    for (int i = 0; i < _columnCount; i++)
                    {
                        object value = passRow[i];
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
                                    rejectReason.AppendLine("Column:" + col.ColumnName + ": Tried to insert null into non-null column.");
                                    finalInvalidAction = Function.EInvalidAction.Reject;
                                }
                                passRow[i] = DBNull.Value;
                            }
                            else
                            {
                                var parseresult = DataType.TryParse(col.Datatype, value, col.MaxLength);
                                if (parseresult.Success == false)
                                {
                                    if (rejectRow == null)
                                    {
                                        rejectRow = new object[_columnCount];
                                        passRow.CopyTo(rejectRow, 0);
                                        rejectRow[_operationOrdinal] = 'R';
                                        TransformRowsRejected++;
                                    }
                                    rejectReason.AppendLine(parseresult.Message);
                                    finalInvalidAction = Function.EInvalidAction.Reject;
                                }
                                else
                                {
                                    passRow[i] = parseresult.Value;
                                }
                            }
                        }
                    }
                }

                switch(finalInvalidAction)
                {
                    case Function.EInvalidAction.Pass:
                        passRow[_validationStatusOrdinal] = "passed";
                        return new ReturnValue<object[]>(true, passRow);
                    case Function.EInvalidAction.Clean:
                        passRow[_validationStatusOrdinal] = "cleaned";
                        return new ReturnValue<object[]>(true, passRow);
                    case Function.EInvalidAction.RejectClean:
                        passRow[_validationStatusOrdinal] = "rejected-cleaned";
                        rejectRow[_validationStatusOrdinal] = "rejected-cleaned";
                        rejectRow[_rejectReasonOrdinal] = rejectReason.ToString();
                        _savedRejectRow = rejectRow;
                        return new ReturnValue<object[]>(true, passRow);
                    case Function.EInvalidAction.Reject:
                        passRow[_validationStatusOrdinal] = "rejected";
                        rejectRow[_validationStatusOrdinal] = "rejected";
                        rejectRow[_rejectReasonOrdinal] = rejectReason.ToString();
                        return new ReturnValue<object[]>(true, rejectRow);
                }

                //should never get here.
                throw new Exception("Validation reached an unknown possibility.  This should not be possible");
            }

            return new ReturnValue<object[]>(false, null);

        }

        public override string Details()
        {
            return "Validation";
        }
    }
}
