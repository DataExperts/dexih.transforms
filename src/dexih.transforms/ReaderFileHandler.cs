using System;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.File;

namespace dexih.transforms
{
    public sealed class ReaderFileHandler : Transform
    {
        private readonly FileHandlerBase _fileHandler;

        private SelectQuery _selectQuery;

		public FlatFile CacheFlatFile => (FlatFile)CacheTable;

        public ReaderFileHandler(FileHandlerBase fileHandler, Table table)
        {
            CacheTable = table;
            _fileHandler = fileHandler;
        }
        
        public override void Close()
        {
            _fileHandler?.Dispose();
            IsOpen = false;
        }

        public override Task<bool> Open(long auditKey, SelectQuery query = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            _selectQuery = query;

            if (IsOpen)
            {
                throw new ConnectionException("The file reader connection is already open.");
            }

            IsOpen = true;
            
            return Task.FromResult(true);

        }

        public override string TransformName => $"File Reader: {_fileHandler?.FileType}";
        public override string TransformDetails => CacheTable?.Name ?? "Unknown";


        public override bool ResetTransform()
        {
            return IsOpen;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            while (true)
            {
                object[] row;
                try
                {
                    row = await _fileHandler.GetRow();
                }
                catch (Exception ex)
                {
                    throw new ConnectionException("The flat file reader failed with the following message: " + ex.Message, ex);
                }

                if (row != null) return row;
                
                _fileHandler.Dispose();
                IsOpen = false;

                return null;

            }

        }
        
        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            Reset();
            return Open(auditKey, query, cancellationToken);
        }

        public override bool FinalizeLookup()
        {
            Close();
            return true;
        }


    }
}
