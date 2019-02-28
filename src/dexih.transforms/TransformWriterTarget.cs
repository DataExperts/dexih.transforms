using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.transforms
{
    public class TransformWriterTarget
    {

        [JsonConverter(typeof(StringEnumConverter))]
        public enum ETransformWriterMethod
        {
            Bulk, 
            Transaction,
            None
        }

        public TransformWriterTarget(ETransformWriterMethod transformWriterMethod, TransformWriterResult writerResult,
            Connection targetConnection, Table targetTable, Connection rejectConnection, Table rejectTable):
            this(transformWriterMethod, writerResult, targetConnection, targetTable, rejectConnection, rejectTable, 1000)
        {}
        
        public TransformWriterTarget(ETransformWriterMethod transformWriterMethod, TransformWriterResult writerResult, Connection targetConnection, Table targetTable, Connection rejectConnection, Table rejectTable, int commitSize)
        {
            TransformWriterMethod = transformWriterMethod;
            
            switch (transformWriterMethod)
            {
                case ETransformWriterMethod.Bulk:
                    _transformWriterTask = new TransformWriterTaskBulk(commitSize);
                    break;
                case ETransformWriterMethod.Transaction:
                    _transformWriterTask = new TransformWriterTaskTransaction();
                    break;
            }

            TargetConnection = targetConnection;
            TargetTable = targetTable;
            RejectTable = rejectTable;
            RejectConnection = rejectConnection;
            WriterResult = writerResult;
            
            _transformWriterTask.Initialize(targetTable, targetConnection, rejectTable, rejectConnection);
        }
        
        public ETransformWriterMethod TransformWriterMethod { get; set; }
        
        public TransformWriterResult WriterResult { get; set; }
        
        public string[] ColumnPath { get; set; }
        public Connection TargetConnection { get; set; }
        public Table TargetTable { get; set; }

        public Connection RejectConnection { get; set; }
        public Table RejectTable { get; set; }
        
        public Connection ProfileConnection { get; set; }
        public Table ProfileTable { get; set; }
        
        public string KeyName { get; set; }
        public long KeyValue { get; set; }
        
        private int[] _fieldOrdinals;
        private int[] _rejectFieldOrdinals;
        private int _operationOrdinal;
        private bool _ordinalsInitialized = false;
        
        public long CurrentAutoIncrementKey { get; protected set; }

        private readonly TransformWriterTask _transformWriterTask;

        /// <summary>
        /// Initializes all target tables
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task WriterInitialize(CancellationToken cancellationToken)
        {
            //if the table doesn't exist, create it.  
            var tableExistsResult = await TargetConnection.TableExists(TargetTable, cancellationToken);
            if (!tableExistsResult)
            {
                await TargetConnection.CreateTable(TargetTable, false, cancellationToken);
            }

            await TargetConnection.DataWriterStart(TargetTable);

            //if the truncate table flag is set, then truncate the target table.
            if (WriterResult.TruncateTarget)
            {
                await TargetConnection.TruncateTable(TargetTable, cancellationToken);
            }
            
            // get the last surrogate key it there is one on the table.
            var surrogateKey = TargetTable.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
            if (surrogateKey != null)
            {
                CurrentAutoIncrementKey = await TargetConnection.GetNextKey(TargetTable, surrogateKey, cancellationToken);
            }
            else
            {
                CurrentAutoIncrementKey = -1;
            }

            _ordinalsInitialized = false;
        }

     /// <summary>
        /// Caches a records from the inTransform.
        /// 
        /// </summary>
        /// <param name="writerTargets"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The operation, and the tableCache if the rows have been exceeded.</returns>
        public async Task WriteRecord(TransformWriterTargets writerTargets, CancellationToken cancellationToken)
        {
            // initialize the ordinal lookups if this is the first write.
            if (!_ordinalsInitialized)
            {
                var count = TargetTable.Columns.Count;
                _fieldOrdinals = new int[count];
                for (var i = 0; i < count; i++)
                {
                    _fieldOrdinals[i] = writerTargets.Transform.GetOrdinal(TargetTable.Columns[i].Name);
                }

                if (RejectTable != null)
                {
                    count = RejectTable.Columns.Count;
                    _rejectFieldOrdinals = new int[count];
                    for (var i = 0; i < count; i++)
                    {
                        _rejectFieldOrdinals[i] = writerTargets.Transform.GetOrdinal(RejectTable.Columns[i].Name);
                    }
                }

                _operationOrdinal = writerTargets.Transform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);
                KeyName = TargetTable.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement)?.Name;
                if (string.IsNullOrEmpty(KeyName))
                {
                    KeyName = TargetTable.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement)?.Name;
                }

                _ordinalsInitialized = true;
            }
            
            Table table;
            var ordinals = _fieldOrdinals;
            
            var operation = _operationOrdinal >= 0 ? (char) writerTargets.Transform[_operationOrdinal] : 'C';

            if (operation == 'R')
            {
                table = RejectTable;
                ordinals = _rejectFieldOrdinals;
                if (RejectTable == null)
                {
                    var rejectColumn = writerTargets.Transform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.RejectedReason);
                    var rejectReason = "";
                    rejectReason = rejectColumn > 0 ? writerTargets.Transform[rejectColumn].ToString() : "No reject reason found.";
                    throw new TransformWriterException($"Transform write failed as a record was rejected, however there is no reject table set.  The reject reason was: {rejectReason}.");
                }
            } else if (operation == 'T')
            {
                if (!TargetConnection.DynamicTableCreation)
                {
                    await TargetConnection.TruncateTable(TargetTable, cancellationToken);
                }
                return;
            }
            else
            {
                table = TargetTable;
            }

            var columnCount = table.Columns.Count;

            var row = new object[columnCount];

            for (var i = 0; i < columnCount; i++)
            {
                var ordinal = ordinals[i];
                if (ordinal >= 0)
                {
                    row[i] = TargetConnection.ConvertForWrite(table.Columns[i], writerTargets.Transform[ordinal]);
                }
                else
                {
                    var key = writerTargets.GetSurrogateKey(table.Columns[i].Name);
                    if (key != null)
                    {
                        row[i] = key;
                    }
                }
            }

            switch (operation)
            {
                case 'C': 
                    WriterResult.IncrementRowsCreated();
                    break;
                case 'U':
                    WriterResult.IncrementRowsUpdated();
                    break;
                case 'D':
                    WriterResult.IncrementRowsDeleted();
                    break;
            }
           
            KeyValue = await _transformWriterTask.AddRecord(operation, row, cancellationToken);
        }

        public async Task WriterFinalize(Transform inTransform, CancellationToken cancellationToken)
        {
            await _transformWriterTask.FinalizeRecords(cancellationToken);

            if (ProfileTable != null)
            {
                var profileResults = inTransform.GetProfileResults();
                if (profileResults != null)
                {
                    var profileExists = await ProfileConnection.TableExists(ProfileTable, cancellationToken);
                    if (!profileExists)
                    {
                        await ProfileConnection.CreateTable(ProfileTable, false, cancellationToken);
                    }

                    WriterResult.ProfileTableName = ProfileTable.Name;

                    try
                    {
                        await ProfileConnection.ExecuteInsertBulk(ProfileTable, profileResults, cancellationToken);
                    }
                    catch(Exception ex)
                    {
                        var message = $"Failed to save profile results.  {ex.Message}";
                        var newException = new TransformWriterException(message, ex);
                        await WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, CancellationToken.None);
                        return;
                    }
                }
            }
            
            //update the statistics.
            WriterResult.RowsFiltered = inTransform.TotalRowsFiltered;
            WriterResult.RowsSorted = inTransform.TotalRowsSorted;
            WriterResult.RowsRejected = inTransform.TotalRowsRejected;
            WriterResult.RowsPreserved = inTransform.TotalRowsPreserved;
            WriterResult.RowsIgnored = inTransform.TotalRowsIgnored;
            WriterResult.RowsReadPrimary = inTransform.TotalRowsReadPrimary;
            WriterResult.RowsReadReference = inTransform.TotalRowsReadReference;

            //calculate the throughput figures
            var rowsWritten = WriterResult.RowsTotal - WriterResult.RowsIgnored;

            var performance = new StringBuilder();
            performance.AppendLine(inTransform.PerformanceSummary());

            var writeDataTicks = _transformWriterTask.WriteDataTicks;
            performance.AppendLine($"Target {TargetConnection.Name} - Time: {writeDataTicks:c}, Rows: {rowsWritten}, Performance: {(rowsWritten/writeDataTicks.TotalSeconds):F} rows/second");

            WriterResult.PerformanceSummary = performance.ToString();


            WriterResult.WriteTicks = writeDataTicks.Ticks;
            WriterResult.ReadTicks = inTransform.ReaderTimerTicks().Ticks;
            WriterResult.ProcessingTicks = inTransform.ProcessingTimerTicks().Ticks;

            WriterResult.EndTime = DateTime.Now;

            if (WriterResult.RowsTotal == 0)
                WriterResult.MaxIncrementalValue = WriterResult.LastMaxIncrementalValue;
            else
                WriterResult.MaxIncrementalValue = inTransform.GetMaxIncrementalValue();

            if (CurrentAutoIncrementKey != -1)
            {
                var surrogateKey = TargetTable.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
                await TargetConnection.UpdateIncrementalKey(TargetTable, surrogateKey.Name, inTransform.SurrogateKey, cancellationToken);
            }
            
            inTransform.Dispose();

            try
            {
                await TargetConnection.DataWriterFinish(TargetTable);
            }
            catch(Exception ex)
            {
                throw new TransformWriterException($"The transform writer failed to finish when attempting a finish on the target table {TargetTable.Name} in {TargetConnection.Name}.  {ex.Message}.", ex);
            }
            
        }

    }
}