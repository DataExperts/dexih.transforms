using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;

namespace dexih.transforms
{

    /// <summary>
    /// The join table is loaded into memory and then joined to the primary table.
    /// </summary>
    public class TransformConcatinate : Transform
    {
        Task<bool> primaryReadTask;
        Task<bool> referenceReadTask;

        private List<int> primaryMappings = new List<int>();
        private List<int> referenceMappings = new List<int>();

        public TransformConcatinate() { }

        public TransformConcatinate(Transform primaryTransform, Transform concatinateTransform)
        {
            SetInTransform(primaryTransform, concatinateTransform);
        }

        private bool _firstRead;

        private int _primaryFieldCount;
        private int _referenceFieldCount;

        public override bool InitializeOutputFields()
        {
            if (ReferenceTransform == null)
                throw new Exception("There must a concatinate transform specified.");

            CacheTable = new Table("Concatinated");

            var pos = 0;
            foreach (var column in PrimaryTransform.CacheTable.Columns)
            {
                CacheTable.Columns.Add(column.Copy());
                primaryMappings.Add(pos);
                pos++;
            }

            foreach (var column in ReferenceTransform.CacheTable.Columns)
            {
                var ordinal = CacheTable.GetOrdinal(column.Name);
                if (ordinal < 0)
                {
                    CacheTable.Columns.Add(column.Copy());
                    ordinal = pos;
                    pos++;
                }
                referenceMappings.Add(ordinal);
            }

            _firstRead = true;

            _primaryFieldCount = PrimaryTransform.FieldCount;
            _referenceFieldCount = ReferenceTransform.FieldCount;

            return true;
        }

        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
        {
            AuditKey = auditKey;
            if (query == null)
                query = new SelectQuery();

            var returnValue = await PrimaryTransform.Open(auditKey, query, cancelToken);
            if (!returnValue.Success)
                return returnValue;

            returnValue = await ReferenceTransform.Open(auditKey, null, cancelToken);
            if (!returnValue.Success)
                return returnValue;

            return returnValue;
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            if(_firstRead )
            {
                primaryReadTask = PrimaryTransform.ReadAsync(cancellationToken);
                referenceReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                _firstRead = false;
            }

            if(primaryReadTask != null && referenceReadTask != null)
            {
                await Task.WhenAny(primaryReadTask, referenceReadTask);

                if(primaryReadTask.IsCanceled || referenceReadTask.IsCanceled)
                {
                    return new ReturnValue<object[]>(false, "The read record task was cancelled.", null);
                }

                if(primaryReadTask.IsFaulted)
                {
                    return new ReturnValue<object[]>(false, "The primary reader failed due to " + primaryReadTask.Exception?.Message ?? "Unknown error.", primaryReadTask.Exception);
                }

                if (referenceReadTask.IsFaulted)
                {
                    return new ReturnValue<object[]>(false, "The reference reader failed due to " + referenceReadTask.Exception?.Message ?? "Unknown error.", referenceReadTask.Exception);
                }

                if (primaryReadTask.IsCompleted)
                {
                    var result = primaryReadTask.Result;
                    if(result == true)
                    {
                        var returnValue = CreateRecord(PrimaryTransform, primaryMappings);
                        primaryReadTask = PrimaryTransform.ReadAsync(cancellationToken);
                        return returnValue;
                    }
                    primaryReadTask = null;
                }

                if (referenceReadTask.IsCompleted)
                {
                    var result = referenceReadTask.Result;
                    if (result == true)
                    {
                        var returnValue = CreateRecord(ReferenceTransform, referenceMappings);
                        referenceReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                        return returnValue;
                    }
                    primaryReadTask = null;
                }
            }

            if(primaryReadTask != null)
            {
                var result = await primaryReadTask;
                if (result == true)
                {
                    var returnValue = CreateRecord(PrimaryTransform, primaryMappings);
                    primaryReadTask = PrimaryTransform.ReadAsync(cancellationToken);
                    return returnValue;
                }
                primaryReadTask = null;
            }

            if (referenceReadTask != null)
            {
                var result = await referenceReadTask;
                if (result == true)
                {
                    var returnValue = CreateRecord(ReferenceTransform, referenceMappings);
                    referenceReadTask = ReferenceTransform.ReadAsync(cancellationToken);
                    return returnValue;
                }
                referenceReadTask = null;
            }

            return new ReturnValue<object[]>(false, null);
        }

        private ReturnValue<object[]> CreateRecord(Transform transform, List<int> mappings)
        {
            var newRow = new object[CacheTable.Columns.Count];

            for(var i = 0; i< mappings.Count; i++)
            {
                newRow[mappings[i]] = transform[i];
            }

            return new ReturnValue<object[]>(true, newRow);
        }

        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true);
        }

        public override string Details()
        {
            return "Concatinate";
        }

    }

}
