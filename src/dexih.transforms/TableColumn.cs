using System;
using System.Linq;
using System.Text;
using dexih.functions;
using static dexih.functions.DataType;

namespace dexih.transforms
{
    
    public class TableColumn
    {
        public TableColumn()
        {
            
        }

        public TableColumn(string columName)
        {
            ColumnName = columName;
            DataType = ETypeCode.String;
        }

        public TableColumn(string columName, ETypeCode dataType)
        {
            ColumnName = columName;
            DataType = DataType;
        }

        public enum EDeltaType
        {
            SurrogateKey,
            SourceSurrogateKey,
            ValidFromDate,
            ValidToDate,
            CreateDate,
            UpdateDate,
            CreateAuditKey,
            UpdateAuditKey,
            IsCurrentField,
            NaturalKey,
            TrackingField,
            NonTrackingField,
            IgnoreField,
            ValidationStatus,
            RejectedReason,
            FileName,
            AutoGenerate,
            AzureRowKey, //special column type for Azure Storage Tables.  
            AzurePartitionKey,//special column type for Azure Storage Tables.  
        }

        public enum ESecurityFlag
        {
            None,
            Encrypt,
            OneWayHash
        }

        public Table ParentTable { get; set; }

        public int ColumnKey { get; set; }
        public int Position { get; set; }
        public string ColumnName { get; set; }
        public string LogicalName { get; set; }
        public string Description { get; set; }
        public ETypeCode DataType { get; set; }
        public int? ColumnValidationKey { get; set; }
        public int? MaxLength { get; set; }
        public int? Precision { get; set; }
        public int? Scale { get; set; }
        public bool AllowDbNull { get; set; }
        public EDeltaType DeltaType { get; set; }
        public bool IsUnique { get; set; }
        public bool IsMandatory { get; set; } = false;
        public ESecurityFlag SecurityFlag { get; set; } = ESecurityFlag.None;
        public bool IsInput { get; set; }
        public bool IsIncrementalUpdate { get; set; }
        public string InputValue { get; set; }
        public int? InputColumnKey { get; set; }

        public string DataTypeString => DataType.ToString();
        public string DeltaTypeString => DeltaType.ToString();

        public Type ColumnGetType
        {
            get
            {
                return Type.GetType("System." + DataType);
            }
            set 
            {
                DataType = GetTypeCode(value);
            }
        }

        //checks if this column is valid in the table.
        public ReturnValue ValidateColumn(Table compareTable)
        {
            bool isValid = true;
            
            StringBuilder messages = new StringBuilder();

            //check for duplicate names
            if(ParentTable.Columns.Any(c => c.ColumnName == ColumnName && c.ColumnKey != ColumnKey && c.Position <= Position))
            {
                messages.AppendLine("Error: The column " + ColumnName + " exists multiple times.  Remove the duplicates.");
                isValid = false;
            }

            //check for duplicate deltatype
            if (ParentTable.Columns.Any(c => IsGeneratedColumn() && c.DeltaType == DeltaType && c.ColumnKey != ColumnKey && c.Position <= Position))
            {
                messages.AppendLine("Error: More than one column with the delta type: " + DeltaType.ToString() + " exists.  Remove the duplicates.");
                isValid = false;
            }

            //compare with underlying physical table.
            if(compareTable != null)
            {
                //check column position
                int columnPos = 0;
                int comparePos = 0;
                TableColumn compareColumn = null;
                foreach (var column in ParentTable.Columns.OrderBy(c => c.Position))
                {
                    if (column.ColumnName == ColumnName)
                        break;
                    columnPos++;
                }
                foreach (var column in compareTable.Columns.OrderBy(c => c.Position))
                {
                    if (column.ColumnName == ColumnName)
                    {
                        compareColumn = column;
                        break;
                    }
                    comparePos++;
                }
                if(compareColumn == null)
                {
                    messages.AppendLine("Error: The column: " + ColumnName + " does not exist in the underlying table.  Remove this column or resync with the underlying table.exists.");
                    isValid = false;
                } else 
                {
                    if(comparePos != columnPos)
                    {
                        messages.AppendLine("Warning: The source column is at position " + (columnPos +1).ToString() + " which does not match the position in the underlying table which is at " + (comparePos + 1 ).ToString() + ".  This will may cause an issue with the bulk load function.");
                        isValid = false;
                    }
                    if (compareColumn.DataType != DataType)
                    {
                        messages.AppendLine("Warning: The column: " + ColumnName + " has datatype " + compareColumn.DataTypeString + " which does not match the current data type of " + DataTypeString + ".");
                        isValid = false;
                    }
                }
            }

            if (isValid && compareTable != null)
                return new ReturnValue(true, "Column matches physical table.", null);
            if (isValid)
                return new ReturnValue(false, "Run compare action to compare with underlying table.", null);
            else
                return new ReturnValue(false, messages.ToString(), null);

        }

        /// <summary>
        /// Is the column one form the source (vs. a value added column).
        /// </summary>
        /// <returns></returns>
        public bool IsSourceColumn()
        {
            switch (DeltaType)
            {
                case EDeltaType.NaturalKey:
                case EDeltaType.TrackingField:
                case EDeltaType.NonTrackingField:
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Columns which require no mapping and are generated automatically for auditing.
        /// </summary>
        /// <returns></returns>
        public bool IsGeneratedColumn()
        {
            switch (DeltaType)
            {
                case EDeltaType.CreateAuditKey:
                case EDeltaType.UpdateAuditKey:
                case EDeltaType.CreateDate:
                case EDeltaType.UpdateDate:
                case EDeltaType.SurrogateKey:
                case EDeltaType.AutoGenerate:
                case EDeltaType.ValidationStatus:
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Columns which indicate if the record is current.  These are the createdate, updatedate, iscurrentfield
        /// </summary>
        /// <returns></returns>
        public bool IsCurrentColumn()
        {
            switch (DeltaType)
            {
                case EDeltaType.ValidFromDate:
                case EDeltaType.ValidToDate:
                case EDeltaType.IsCurrentField:
                    return true;
            }
            return false;
        }

 
        /// <summary>
        /// Creates a copy of the column which can be used when generating other tables.
        /// </summary>
        /// <returns></returns>
        public TableColumn Copy()
        {
            return new TableColumn()
            {
                ColumnName = ColumnName,
                Position = Position,
                LogicalName = LogicalName,
                Description = Description,
                DataType = DataType,
                MaxLength = MaxLength,
                Precision = Precision,
                Scale = Scale,
                AllowDbNull = AllowDbNull,
                ColumnValidationKey = ColumnValidationKey,
                DeltaType = DeltaType,
                IsUnique = IsUnique,
                SecurityFlag = SecurityFlag,
                IsInput = IsInput,
                IsMandatory = IsMandatory,
                IsIncrementalUpdate = IsIncrementalUpdate,
                InputValue = InputValue,
                InputColumnKey = InputColumnKey,
            };
        }
    }
}
