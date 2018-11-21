using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using System.Threading;
using dexih.transforms.Exceptions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.connections.sql
{
    public sealed class ReaderSql : Transform
    {
        private bool _isOpen = false;
        private DbDataReader _sqlReader;
        private DbConnection _sqlConnection;

        private List<int> _fieldOrdinals;
        private int _fieldCount;

        private List<Sort> _sortFields;


        public ReaderSql(ConnectionSql connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table;
        }

        public override void Close()
        {
            _sqlReader?.Close();
            _sqlConnection?.Close();
            _isOpen = false;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            try
            {
                AuditKey = auditKey;

                if (_isOpen)
                {
                    throw new ConnectionException("The reader is already open.");
                }

                _sqlConnection = await ((ConnectionSql)ReferenceConnection).NewConnection();
                _sqlReader = await ReferenceConnection.GetDatabaseReader(CacheTable, _sqlConnection, query, cancellationToken);

                _fieldCount = _sqlReader.FieldCount;
                _fieldOrdinals = new List<int>();
                for (var i = 0; i < _sqlReader.FieldCount; i++)
                {
                    var fieldName = _sqlReader.GetName(i);
                    var ordinal = CacheTable.GetOrdinal(fieldName);
                    if (ordinal < 0)
                    {
                        throw new ConnectionException($"The reader could not be opened as column {fieldName} could not be found in the table {CacheTable.Name}.");
                    }
                    _fieldOrdinals.Add(ordinal);
                }

                _sortFields = query?.Sorts;

				_isOpen = true;
                return true;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Open reader failed. {ex.Message}", ex);
            }
        }

        public override string Details()
        {
            
            return ReferenceConnection == null ? "SqlReader" : $"SqlReader - {ReferenceConnection.Name}({ReferenceConnection.GetType().Name})";
        }

        public override List<Sort> SortFields => _sortFields;
        public override bool ResetTransform() => _isOpen;

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if (_sqlReader == null)
            {
                throw new ConnectionException("The read record failed as the connection has not been opened.");
            }
            try
            {
                if (await _sqlReader.ReadAsync(cancellationToken))
                {
                    var row = new object[CacheTable.Columns.Count];

                    for (var i = 0; i < _fieldCount; i++)
                    {
                        try
                        {
                            var column = CacheTable.Columns[_fieldOrdinals[i]];
//                            if (column.IsArray())
//                            {
//                                var value = _sqlReader[i];
//                                if (value is string valueString)
//                                {
//                                    row[_fieldOrdinals[i]] = new DataValue(column.DataType, column.Rank, valueString);
//                                }
//                                else
//                                {
//                                    row[_fieldOrdinals[i]] = new DataValue(column.DataType, column.Rank, _sqlReader[i]);
//                                    
//                                }
//                                
//                            }
//                            else
//                            {
                                row[_fieldOrdinals[i]] = Operations.Parse(column.DataType, column.Rank, _sqlReader[i]);    
//                            }
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException(
                                $"The value on column {CacheTable.Columns[_fieldOrdinals[i]].Name} could not be converted to {CacheTable.Columns[_fieldOrdinals[i]].DataType}.  {ex.Message}",
                                ex, _sqlReader[i]);
                        }
                    }

                    return row;
                }
                else
                {
                    Close();
                    return null;
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Read record failed. {ex.Message}", ex);
            }
        }
        
        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            Reset();
            Dispose();
            return Open(auditKey, query, cancellationToken);
        }

    }
}
