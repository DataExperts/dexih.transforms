using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using System.Threading;
using dexih.transforms.Exceptions;
using dexih.functions.Query;

namespace dexih.connections.sql
{
    public sealed class ReaderSql : Transform
    {
        private DbDataReader _sqlReader;
        private DbConnection _sqlConnection;

        private List<int> _fieldOrdinals;
        private int _fieldCount;

        private bool _firstRead = true;
        
        public ReaderSql(ConnectionSql connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table.Copy(removeIgnoreColumns: true);
            Name = table.Name;
        }
        
        public override string TransformName => $"Database Reader - {ReferenceConnection?.Name}";
        
        public override Dictionary<string, object> TransformProperties()
        {
            if (ReferenceConnection != null && CacheTable != null && SelectQuery != null)
            {
                return new Dictionary<string, object>()
                {
                    {"SqlCommand", ReferenceConnection.GetDatabaseQuery(CacheTable, SelectQuery)}
                };
            }

            return null;
        }

        protected override async Task CloseConnections()
        {
            if (_sqlReader != null)
            {
                await _sqlReader?.CloseAsync();
                _sqlReader?.Dispose();
            }

            if (_sqlConnection != null)
            {
                await _sqlConnection?.CloseAsync();
                _sqlConnection?.Dispose();
            }
        }
        
        public override Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            if (IsOpen)
            {
                throw new ConnectionException("The file reader connection is already open.");
            }

            try
            {
                AuditKey = auditKey;
                IsOpen = true;
                _firstRead = true;

                if (requestQuery != null)
                {
                    requestQuery.Alias = TableAlias;
                }
                
                // disables push down query logic
                if (IgnoreQuery)
                {
                    requestQuery = null;
                }

                SelectQuery = requestQuery;
                GeneratedQuery = GetGeneratedQuery(requestQuery);
                
                if (GeneratedQuery?.Columns?.Count > 0)
                {
                    CacheTable.Columns.Clear();
                    foreach (var column in GeneratedQuery.Columns)
                    {
                        CacheTable.Columns.Add(column.OutputColumn?? column.Column);
                    }

                    // foreach (var join in GeneratedQuery.Joins)
                    // {
                    //     foreach (var column in join.JoinTable.Columns)
                    //     {
                    //         CacheTable.Columns.Add(column);
                    //     }
                    // }
                }
                
                return Task.FromResult(true);
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Open reader failed. {ex.Message}", ex);
            }
        }

        public override bool ResetTransform() => IsOpen;

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (_firstRead)
            {
                _firstRead = false;
                _sqlConnection = await ((ConnectionSql) ReferenceConnection).NewConnection(cancellationToken);
                _sqlReader =
                    await ReferenceConnection.GetDatabaseReader(CacheTable, _sqlConnection, SelectQuery, cancellationToken);
                _fieldCount = _sqlReader.FieldCount;
                _fieldOrdinals = new List<int>();

                for (var i = 0; i < _sqlReader.FieldCount; i++)
                {
                    var fieldName = _sqlReader.GetName(i);
                    var field = fieldName.Split("--", 2);
                    int ordinal;
                    if (field.Length == 1)
                    {
                        ordinal = CacheTable.GetOrdinal(field[0]);
                    }
                    else
                    {
                        ordinal = CacheTable.GetOrdinal(field[0], field[1]);
                        if (ordinal < 0)
                        {
                            ordinal = CacheTable.GetOrdinal(fieldName);
                            if (ordinal < 0 && (field[0] == CacheTable.Name || field[0] == TableAlias))
                            {
                                ordinal = CacheTable.GetOrdinal(field[1]);
                            }
                        }
                    }

                    if (ordinal < 0)
                    {
                        throw new ConnectionException(
                            $"The reader could not be opened as column {fieldName} could not be found in the table {CacheTable.Name}.");
                    }

                    _fieldOrdinals.Add(ordinal);
                }
            }

            if (_sqlReader == null)
            {
                throw new ConnectionException("The read record failed as the connection has not been opened.");
            }
            try
            {
                if (await _sqlReader.ReadAsync(cancellationToken))
                {
                    var row = new object[_fieldCount];

                    for (var i = 0; i < _fieldCount; i++)
                    {
                        try
                        {
                            var column = CacheTable.Columns[_fieldOrdinals[i]];
                            row[_fieldOrdinals[i]] = ReferenceConnection.ConvertForRead(_sqlReader, i, column);
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
