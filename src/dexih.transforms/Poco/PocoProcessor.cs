using System;
using System.Collections.Concurrent;
using System.Threading;
using dexih.functions.Query;

namespace dexih.transforms.Poco
{

    public enum EPocoOperation
    {
        Insert = 1, Update, Delete
    }

    public class PocoProcessorEntry<T>
    {
        public EPocoOperation Operation { get; set; }
        public InsertQuery InsertQuery { get; set; }
        public UpdateQuery UpdateQuery { get; set; }
        public DeleteQuery DeleteQuery { get; set; }
        
        public T Item { get; set; }
        public Connection Connection { get; set; }
    }
    
    /// <summary>
    /// Background processor for updating poco objects.
    /// </summary>
    public class PocoProcessor<T>
        {
        private const int _maxQueuedMessages = 1024;

        private readonly BlockingCollection<PocoProcessorEntry<T>> _messageQueue = new BlockingCollection<PocoProcessorEntry<T>>(_maxQueuedMessages);
        private readonly Thread _outputThread;

        private readonly PocoTable<T> _pocoTable;
        
        public PocoProcessor()
        {
            _pocoTable = new PocoTable<T>();
            
            // Start Console message queue processor
            _outputThread = new Thread(ProcessLogQueue)
            {
                IsBackground = true,
                Name = "Poco database updator background thread."
            };
            _outputThread.Start();
        }

        public virtual void EnqueueMessage(EPocoOperation operation, Connection connection, T item)
        {
            var message = new PocoProcessorEntry<T>()
            {
                Item = item,
                Operation = operation,
                Connection = connection
            };

            switch (operation)
            {
                case EPocoOperation.Insert:
                    message.InsertQuery = _pocoTable.PrepareInsert(item);
                    break;
                case EPocoOperation.Update:
                    message.UpdateQuery = _pocoTable.PrepareUpdate(item);
                    break;
                case EPocoOperation.Delete:
                    message.DeleteQuery = _pocoTable.PrepareDelete(item);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
            }
            
            if (!_messageQueue.IsAddingCompleted)
            {
                try
                {
                    _messageQueue.Add(message);
                    return;
                }
                catch (InvalidOperationException) { }
            }

            // Adding is completed so just log the message
            WriteMessage(message);
        }

        // for testing
        internal virtual void WriteMessage(PocoProcessorEntry<T> message)
        {
            switch (message.Operation)
            {
                case EPocoOperation.Insert:
                    _pocoTable.ExecuteInsert(message.Connection, message.InsertQuery, message.Item, CancellationToken.None).Wait();
                    break;
                case EPocoOperation.Update:
                    _pocoTable.ExecuteUpdate(message.Connection, message.UpdateQuery, CancellationToken.None).Wait();
                    break;
                case EPocoOperation.Delete:
                    _pocoTable.ExecuteDelete(message.Connection, message.DeleteQuery, CancellationToken.None).Wait();
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private void ProcessLogQueue()
        {
            try
            {
                foreach (var message in _messageQueue.GetConsumingEnumerable())
                {
                    WriteMessage(message);
                }
            }
            catch
            {
                try
                {
                    _messageQueue.CompleteAdding();
                }
                catch { }
            }
        }

        public void Dispose()
        {
            _messageQueue.CompleteAdding();

            try
            {
                _outputThread.Join(1500);
            }
            catch (ThreadStateException) { }
        }
    }
}