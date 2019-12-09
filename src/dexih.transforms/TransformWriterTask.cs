using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{
    public abstract class TransformWriterTask: IDisposable
    {
        protected Table TargetTable;
        protected Connection TargetConnection;
        protected Table RejectTable;
        protected Connection RejectConnection;
        protected bool TruncateComplete = false;
        
        public TimeSpan WriteDataTicks;

        protected int DbAutoIncrementOrdinal;
        protected int AutoIncrementOrdinal;
        
        public virtual void Initialize(Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection)
        {
            TargetTable = targetTable;
            TargetConnection = targetConnection;
            RejectTable = rejectTable;
            RejectConnection = rejectConnection;
            TruncateComplete = false;

            AutoIncrementOrdinal = targetTable?.GetOrdinal(EDeltaType.AutoIncrement)??-1;
            DbAutoIncrementOrdinal = targetTable?.GetOrdinal(EDeltaType.DbAutoIncrement)??-1;
        }

        public abstract Task<int> StartTransaction(int transactionReference = -1);
        public abstract void CommitTransaction();
        
        public abstract void RollbackTransaction();
        
        
        
        public abstract Task<long> AddRecord(char operation, object[] row, CancellationToken cancellationToken = default);

        public abstract Task FinalizeWrites(CancellationToken cancellationToken = default);

        public abstract void Dispose();
    }
}