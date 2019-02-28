//using dexih.functions;
//using dexih.functions.Query;
//using dexih.transforms.Exceptions;
//using System;
//using System.Collections;
//using System.Collections.Generic;
//using System.Linq;
//using System.Net.Sockets;
//using System.Text;
//using System.Threading;
//using System.Threading.Tasks;
//using System.Xml.Xsl;
//
//namespace dexih.transforms
//{
//    /// <summary>
//    /// The transform write transaction class places each top level row in a transaction, and inserts child records within
//    /// this same transaction.  This has slower performance than the TransformWriteBulk, however guarantees record integrity.
//    /// </summary>
//    public class TransformWriterTransaction: Writer
//    {
//
//
//        /// <summary>
//        /// Indicates the rows buffer per commit.  
//        /// </summary>
//        public int CommitSize { get; set; } = 1000;
//
//        private bool _writeOpen;
//        private int _operationColumnIndex; //the index of the operation in the source data.
//
//        private CancellationToken _cancellationToken;
//
//        public TimeSpan WriteDataTicks;
//
//        /// <summary>
//        /// Writes all record from the inTransform to the target table and reject table.
//        /// </summary>
//        /// <param name="inTransform">Transform to read data from</param>
//        /// <param name="writerTargets"></param>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        public override async Task<bool> WriteRecordsAsync(Transform inTransform, TransformWriterTargets writerTargets, CancellationToken cancellationToken)
//        {
//            _cancellationToken = cancellationToken;
//
//            var updateResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null, cancellationToken);
//            if (!updateResult)
//            {
//                return false;
//            }
//
//            try
//            {
//                await WriteStart(inTransform, writerTargets, cancellationToken);
//            }
//            catch (Exception ex)
//            {
//                var message = $"The transform writer failed to start.  {ex.Message}";
//				var newException = new TransformWriterException(message, ex);
//				await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Abended, message, newException, cancellationToken);
//                return false;
//            }
//
//            var firstRead = true;
//
//            writerTargets.Transform = inTransform;
//
//            while (await inTransform.ReadAsync(cancellationToken))
//            {
//                if (firstRead)
//                {
//                    var runStatusResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Running, null, null, CancellationToken.None);
//                    if (!runStatusResult)
//                    {
//                        return false;
//                    }
//                    firstRead = false;
//                }
//
//                await WriteRecord(writerTargets);
//
//                if (cancellationToken.IsCancellationRequested)
//                {
//                    var runStatusResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Cancelled, null, null, CancellationToken.None);
//                    return runStatusResult;
//                }
//            }
//
//            var setRunStatusResult = await writerTargets.SetRunStatus(TransformWriterResult.ERunStatus.Finished, null, null, CancellationToken.None);
//            return setRunStatusResult;
//        }
//
//        public async Task WriteStart(Transform inTransform, TransformWriterTargets writerTargets, CancellationToken cancellationToken)
//        {
//
//            if (_writeOpen)
//            {
//                throw new TransformWriterException("Transform write failed to start, as a previous operation is still running.");
//            }
//
//            _operationColumnIndex = inTransform.CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);
//
//            // create any missing tables.
//            foreach (var writerTarget in writerTargets.GetAll())
//            {
//                await writerTarget.WriterInitialize(inTransform, cancellationToken);
//                
////                var tableExistsResult = await writerTarget.TargetConnection.TableExists(writerTarget.TargetTable, cancellationToken);
////                if (!tableExistsResult)
////                {
////                    await  writerTarget.TargetConnection.CreateTable(writerTarget.TargetTable, false, cancellationToken);
////                }
////
////                await  writerTarget.TargetConnection.DataWriterStart(writerTarget.TargetTable);
////
////                //if the truncate table flag is set, then truncate the target table.
////                if (writerTarget.WriterResult.TruncateTarget)
////                {
////                    await writerTarget.TargetConnection.TruncateTable(writerTarget.TargetTable, cancellationToken);
////                }
//            }
//
//            _writeOpen = true;
//
//        }
//
//        public async Task WriteRecord(TransformWriterTargets writerTargets)
//        {
//            if (_writeOpen == false)
//            {
//                throw new TransformWriterException($"Transform write failed to write record as the WriteStart has not been called.");
//            }
//
//
//            foreach (var writerTarget in writerTargets.Items)
//            {
//                var record = writerTarget.ConvertRecord(writerTargets.Transform);
//                var table = writerTarget.TargetTable;
//                var connection = writerTarget.TargetConnection;
//
//                switch (record.operation)
//                {
//                    case 'C':
//                    case 'R':
//                        var queryColumns = new List<QueryColumn>();
//                        for(var i = 0; i < table.Columns.Count; i++)
//                        {
//                            var col = table.Columns[i];
//                            if (!col.IsGeneratedColumn())
//                            {
//                                queryColumns.Add(new QueryColumn(col, record.row[i]));
//                            }
//                        }
//
//                        var insertQuery = new InsertQuery(table.Name, queryColumns);
//                        var surrogateKey = await connection.ExecuteInsert(table, new List<InsertQuery>() {insertQuery}, _cancellationToken);
//                        writerTarget.KeyValue = surrogateKey;
//                        writerTarget.KeyName = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement)?.Name;
//
//                        break;
//                    case 'U':
//                        var updateQuery = new UpdateQuery(
//                            table.Name,
//                            table.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.SurrogateKey)
//                                .Select((c, index) => new QueryColumn(c, record.row[index])).ToList(),
//                            table.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey)
//                                .Select((c, index) => new Filter(c, Filter.ECompare.IsEqual, record.row[index])).ToList()
//                        );
//                        await connection.ExecuteUpdate(table, new List<UpdateQuery>() {updateQuery},
//                            _cancellationToken);
//                        break;
//
//                    case 'D':
//                        var deleteQuery = new DeleteQuery(
//                            table.Name,
//                            table.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey)
//                                .Select((c, index) => new Filter(c, Filter.ECompare.IsEqual, record.row[index])).ToList()
//                        );
//                        await connection.ExecuteDelete(table, new List<DeleteQuery>() {deleteQuery},
//                            _cancellationToken);
//                        break;
//                    case 'T':
//                        if (!writerTarget.TargetConnection.DynamicTableCreation)
//                        {
//                            await writerTarget.TargetConnection.TruncateTable(
//                                writerTarget.TargetTable, _cancellationToken);
//                        }
//
//                        break;
//                }
//            }
//
//            if (writerTargets.ChildNodes == null || writerTargets.ChildNodes.Count == 0)
//            {
//                return;
//            }
//
//            // loop through any child nodes, and recurse to write more records.
//            foreach (var writerChild in writerTargets.ChildNodes)
//            {
//                var transform = (Transform) writerTargets.Transform[writerChild.NodeName];
//                await transform.Open(_cancellationToken);
//                writerChild.Transform = transform;
//
//                while (await transform.ReadAsync(_cancellationToken))
//                {
//                    await WriteRecord(writerChild);
//                }
//            }
//
//
//            return;
//        }
//    }
//}
