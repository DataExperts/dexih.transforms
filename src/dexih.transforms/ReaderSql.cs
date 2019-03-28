using dexih.transforms;
using System;
using System.Collections.Generic;
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
        private DbDataReader _sqlReader;
        private DbConnection _sqlConnection;

        private List<int> _fieldOrdinals;
        private int _fieldCount;

        private List<Sort> _sortFields;


        public ReaderSql(ConnectionSql connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table;
            Name = table.Name;
        }
        
        public override string TransformName => $"Database Reader - {ReferenceConnection?.Name}";
        public override string TransformDetails => CacheTable?.Name ?? "Unknown";


        protected override void CloseConnections()
        {
            _sqlReader?.Close();
            _sqlConnection?.Close();
        }

        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            if (IsOpen)
            {
                throw new ConnectionException("The file reader connection is already open.");
            }

            try
            {
                AuditKey = auditKey;
                IsOpen = true;

                _sqlConnection = await ((ConnectionSql)ReferenceConnection).NewConnection();
                _sqlReader = await ReferenceConnection.GetDatabaseReader(CacheTable, _sqlConnection, selectQuery, cancellationToken);
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

                _sortFields = selectQuery?.Sorts;
                return true;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Open reader failed. {ex.Message}", ex);
            }
        }

        public override List<Sort> SortFields => _sortFields;
        public override bool ResetTransform() => IsOpen;

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
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
        
        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken = default)
        {
            Reset();
            Dispose();
            return Open(auditKey, query, cancellationToken);
        }

    }
}
