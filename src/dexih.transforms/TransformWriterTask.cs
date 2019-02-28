using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{
    public abstract class TransformWriterTask
    {
        protected Table TargetTable;
        protected Connection TargetConnection;
        protected Table RejectTable;
        protected Connection RejectConnection;
        
        public TimeSpan WriteDataTicks;
        
        public void Initialize(Table targetTable, Connection targetConnection, Table rejectTable, Connection rejectConnection)
        {
            TargetTable = targetTable;
            TargetConnection = targetConnection;
            RejectTable = rejectTable;
            RejectConnection = rejectConnection;
        }
        
        public abstract Task<long> AddRecord(char operation, object[] row, CancellationToken cancellationToken);

        public abstract Task FinalizeRecords(CancellationToken cancellationToken);
    }
}