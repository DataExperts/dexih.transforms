using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.functions
{
    [MessagePackObject]
    public class TableColumn : IEquatable<TableColumn>

    {
        // [JsonConverter(typeof(StringEnumConverter))]
     

        public TableColumn()
        {
//            ExtendedProperties = new Dictionary<string, string>();
        }

        public TableColumn(string columnName, ETypeCode dataType = ETypeCode.String,
            EDeltaType deltaType = EDeltaType.TrackingField, int rank = 0, string parentTable = null)
        {
            Name = columnName;
            LogicalName = columnName;
            DataType = dataType;
            DeltaType = deltaType;
            ReferenceTable = parentTable;
            Rank = rank;
        }

        public TableColumn(string columnName, EDeltaType deltaType, string parentTable = null)
        {
            Name = columnName;
            LogicalName = columnName;
            DataType = GetDeltaDataType(deltaType);
            DeltaType = deltaType;
            ReferenceTable = parentTable;
        }
        
        //this is the underlying datatype of a non encrypted data type.  
        private ETypeCode _baseDataType;

        //this is the max length of the non-encrypted data type.
        private int? _baseMaxLength;


        [Key(0)]
        public string ReferenceTable { get; set; }

        [Key(1)]
        public string Name { get; set; }

        [Key(2)]
        public string LogicalName { get; set; }

        [Key(3)]
        public string Description { get; set; }

        [Key(4)]
        public ETypeCode DataType
        {
            get
            {
                if (SecurityFlag == ESecurityFlag.None || SecurityFlag == ESecurityFlag.FastDecrypt ||
                    SecurityFlag == ESecurityFlag.StrongDecrypt)
                    return _baseDataType;
                return ETypeCode.String;
            }
            set => _baseDataType = value;
        }

        [Key(5)]
        public int? MaxLength
        {
            get
            {
                if (SecurityFlag == ESecurityFlag.None || SecurityFlag == ESecurityFlag.FastDecrypt ||
                    SecurityFlag == ESecurityFlag.StrongDecrypt)
                    return _baseMaxLength;
                return 250;
            }
            set => _baseMaxLength = value;
        }

        /// <summary>
        /// A string that can be used to group columns.  This is also used to structure json/xml data.  Uses syntax group1.subgroup2.subsubgroup3 etc.
        /// </summary>
        [Key(6)]
        public string ColumnGroup { get; set; }

        [Key(7)]
        public int Rank { get; set; }



        [Key(8)]
        public int? Precision { get; set; }

        [Key(9)]
        public int? Scale { get; set; }

        [Key(10)]
        public bool AllowDbNull { get; set; }

        [Key(11)]
        public EDeltaType DeltaType { get; set; }

        [Key(12)]
        public bool? IsUnicode { get; set; }

        [Key(13)]
        public object DefaultValue { get; set; }

        [Key(14)]
        public bool IsUnique { get; set; }

        [Key(15)]
        public bool IsMandatory { get; set; }

        [Key(16)]
        public ESecurityFlag SecurityFlag { get; set; } = ESecurityFlag.None;

        [Key(17)]
        public bool IsInput { get; set; }

        [Key(18)]
        public bool IsIncrementalUpdate { get; set; }

        // used by the passthrough to indicate if the column is a part of the parent node, or part of current node.
        [Key(19)]
        public bool IsParent { get; set; }

        [Key(20)]
        public TableColumns ChildColumns { get; set; }

        public bool IsArray() => Rank > 0;

        public bool IsAutoIncrement() => DeltaType == EDeltaType.DbAutoIncrement || DeltaType == EDeltaType.AutoIncrement;
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public Type ColumnGetType
        {
            get => Dexih.Utils.DataType.DataType.GetType(DataType);
            set => DataType = GetTypeCode(value, out _);
        }

        /// <summary>
        /// Returns a string with the schema.columngroup.columnname
        /// </summary>
        /// <returns></returns>
        public string TableColumnName()
        {
            if (!string.IsNullOrEmpty(ReferenceTable))
            {
                return ReferenceTable + "." + Name;
            }

            if (!string.IsNullOrEmpty(ColumnGroup))
            {
                return ColumnGroup + "." + Name;
            }
            return Name;
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
                case EDeltaType.AutoIncrement:
                case EDeltaType.DbAutoIncrement:
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
        public TableColumn Copy(bool includeChildColumns = true)
        {
            var newColumn = new TableColumn
            {
                ReferenceTable = ReferenceTable,
                Name = Name,
                LogicalName = LogicalName,
                Description = Description,
                ColumnGroup = ColumnGroup,
                DataType = DataType,
                MaxLength = MaxLength,
                Precision = Precision,
                Scale = Scale,
                AllowDbNull = AllowDbNull,
                DefaultValue = DefaultValue,
                DeltaType = DeltaType,
                IsUnique = IsUnique,
                IsInput = IsInput,
                IsMandatory = IsMandatory,
                IsParent = IsParent,
                IsIncrementalUpdate = IsIncrementalUpdate,
                Rank = Rank,
            };

            if (includeChildColumns && ChildColumns != null && ChildColumns.Count > 0)
            {
                newColumn.ChildColumns = new TableColumns();

                foreach (var col in ChildColumns)
                {
                    newColumn.ChildColumns.Add(col.Copy());
                }
            }

            switch (SecurityFlag)
            {
                case ESecurityFlag.FastEncrypt:
                    newColumn.SecurityFlag = ESecurityFlag.FastEncrypted;
                    break;
                case ESecurityFlag.StrongEncrypt:
                    newColumn.SecurityFlag = ESecurityFlag.StrongEncrypted;
                    break;
                case ESecurityFlag.OneWayHash:
                    newColumn.SecurityFlag = ESecurityFlag.OneWayHashed;
                    break;
                default:
                    newColumn.SecurityFlag = SecurityFlag;
                    break;
            }

            return newColumn;
        }

        /// <summary>
        /// Gets the default datatype for specified delta column
        /// </summary>
        /// <returns>The delta data type.</returns>
        /// <param name="deltaType">Delta type.</param>
        public static ETypeCode GetDeltaDataType(EDeltaType deltaType)
        {
            switch (deltaType)
            {
                case EDeltaType.AutoIncrement:
                case EDeltaType.SourceSurrogateKey:
                case EDeltaType.CreateAuditKey:
                case EDeltaType.UpdateAuditKey:
                case EDeltaType.FileRowNumber:
                    return ETypeCode.UInt64;
                case EDeltaType.ValidFromDate:
                case EDeltaType.ValidToDate:
                case EDeltaType.CreateDate:
                case EDeltaType.UpdateDate:
                case EDeltaType.TimeStamp:
                    return ETypeCode.DateTime;
                case EDeltaType.IsCurrentField:
                    return ETypeCode.Boolean;
                case EDeltaType.NaturalKey:
                case EDeltaType.TrackingField:
                case EDeltaType.NonTrackingField:
                case EDeltaType.IgnoreField:
                case EDeltaType.ValidationStatus:
                case EDeltaType.RejectedReason:
                case EDeltaType.FileName:
                case EDeltaType.RowKey:
                case EDeltaType.PartitionKey:
                case EDeltaType.DatabaseOperation:
                case EDeltaType.ResponseSuccess:
                case EDeltaType.ResponseData:
                case EDeltaType.ResponseStatus:
                case EDeltaType.ResponseSegment:
                case EDeltaType.Error:
                case EDeltaType.Url:
                    return ETypeCode.String;
                case EDeltaType.DbAutoIncrement:
                    break;
                case EDeltaType.Version:
                    return ETypeCode.Int32;
                default:
                    throw new ArgumentOutOfRangeException(nameof(deltaType), deltaType, null);
            }

            return ETypeCode.String;
        }

        /// <summary>
        /// Compare the column 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Compare(TableColumn other)
        {
            if (other == null)
            {
                return false;
            }

            return TableColumnName() == other.TableColumnName() ||
                   Name == other.Name;
        }

        public string DBML()
        {
            var desc = new StringBuilder();
            desc.Append($"{Name} {DataType}");

            if (MaxLength != null && (DataType == ETypeCode.Byte || DataType == ETypeCode.Char || DataType == ETypeCode.String))
            {
                desc.Append($"({MaxLength})");
            }

            if (DataType == ETypeCode.Decimal || DataType == ETypeCode.Single)
            {
                desc.Append($"({Precision}, {Scale})");
            }
            
            for (var i = 0; i < Rank; i++)
            {
                desc.Append("()");
            }
            
            var columnSettings = new List<string>();
                
            if (IsAutoIncrement())
            {
                columnSettings.Add("pk");
                columnSettings.Add("increment");
            }

            if (IsUnique)
            {
                columnSettings.Add("unique");
            }

            if (AllowDbNull)
            {
                columnSettings.Add("null");
            }
            else
            {
                columnSettings.Add("not null");
            }

            if (!string.IsNullOrEmpty(LogicalName) && LogicalName != Name)
            {
                columnSettings.Add($"note: 'Local Name: {LogicalName}'");
            }

            
            if (!string.IsNullOrEmpty(Description))
            {
                columnSettings.Add($"comment: '{Description}'");
            }

            if (DefaultValue != null)
            {
                if (Dexih.Utils.DataType.DataType.IsNumber(DataType))
                {
                    columnSettings.Add($"default: {DefaultValue}");
                }
                else
                {
                    columnSettings.Add($"default: '{DefaultValue}'");
                }
            }

            desc.Append($" [{string.Join(", ", columnSettings)}]");

            return desc.ToString();
        }

        public bool Equals(TableColumn other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return
                string.Equals(ReferenceTable, other.ReferenceTable) &&
                string.Equals(Name, other.Name) &&
                string.Equals(LogicalName, other.LogicalName) &&
                string.Equals(Description, other.Description) &&
                Rank == other.Rank &&
                _baseDataType == other._baseDataType &&
                _baseMaxLength == other._baseMaxLength &&
                Precision == other.Precision &&
                Scale == other.Scale &&
                AllowDbNull == other.AllowDbNull &&
                DeltaType == other.DeltaType &&
                IsUnicode == other.IsUnicode &&
                Equals(DefaultValue, other.DefaultValue) &&
                IsUnique == other.IsUnique &&
                IsMandatory == other.IsMandatory &&
                SecurityFlag == other.SecurityFlag &&
                IsInput == other.IsInput &&
                IsIncrementalUpdate == other.IsIncrementalUpdate;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TableColumn) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (ReferenceTable != null ? ReferenceTable.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (LogicalName != null ? LogicalName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Description != null ? Description.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Rank;
                hashCode = (hashCode * 397) ^ (int) _baseDataType;
                hashCode = (hashCode * 397) ^ _baseMaxLength.GetHashCode();
                hashCode = (hashCode * 397) ^ Precision.GetHashCode();
                hashCode = (hashCode * 397) ^ Scale.GetHashCode();
                hashCode = (hashCode * 397) ^ AllowDbNull.GetHashCode();
                hashCode = (hashCode * 397) ^ (int) DeltaType;
                hashCode = (hashCode * 397) ^ IsUnicode.GetHashCode();
                hashCode = (hashCode * 397) ^ (DefaultValue != null ? DefaultValue.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ IsUnique.GetHashCode();
                hashCode = (hashCode * 397) ^ IsMandatory.GetHashCode();
                hashCode = (hashCode * 397) ^ (int) SecurityFlag;
                hashCode = (hashCode * 397) ^ IsInput.GetHashCode();
                hashCode = (hashCode * 397) ^ IsIncrementalUpdate.GetHashCode();
                return hashCode;
            }
        }
    }
}