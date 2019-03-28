using System;

namespace dexih.transforms
{
    public class DeltaValues
    {
        public char Operation { get; }
        public long AutoIncrementValue { get; set; }
        public bool IsCurrent { get; }
        public DateTime CreateDate { get; }
        public DateTime UpdateDate { get; }
        public DateTime ValidFrom { get; }
        public DateTime ValidTo { get; }

        public long CreateAuditKey { get; }
        public long UpdateAuditKey { get; }
        public int Version { get; }
        
        public DeltaValues(char operation)
        {
            Operation = operation;
            AutoIncrementValue = 0;
            IsCurrent = true;
            CreateDate = default;
            UpdateDate = default;
            ValidFrom = default;
            ValidTo = default;
            CreateAuditKey = default;
            UpdateAuditKey = default;
            Version = 1;
        }

        public DeltaValues(char operation, long autoIncrementValue, bool isCurrent, DateTime createDate, DateTime updateDate, DateTime validFrom, DateTime validTo, long createAuditKey, long updateAuditKey,
            int version)
        {
            Operation = operation;
            AutoIncrementValue = autoIncrementValue;
            IsCurrent = isCurrent;
            CreateDate = createDate;
            UpdateDate = updateDate;
            ValidFrom = validFrom;
            ValidTo = validTo;
            CreateAuditKey = createAuditKey;
            UpdateAuditKey = updateAuditKey;
            Version = version;
        }
    }
}