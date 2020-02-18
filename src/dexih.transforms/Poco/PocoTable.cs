using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.Poco
{
    public class PocoTable<T>
    {
        public Table Table { get; set; }
        public List<PocoTableMapping> TableMappings { get; set; }
        public PropertyInfo AutoIncrementProperty {get;set;}

        public PocoTable()
        {
            var table = new Table(typeof(T).Name);

            var tableAttr = typeof(T).GetTypeInfo().GetCustomAttribute<PocoTableAttribute>(false);
            if (tableAttr != null)
            {
                table.OutputSortFields = tableAttr.SortFields;
                if (!string.IsNullOrEmpty(tableAttr.Name))
                {
                    table.Name = tableAttr.Name;
                }
            }

            var mappings = new List<PocoTableMapping>();

            var position = 0;
            foreach (var propertyInfo in typeof(T).GetProperties())
            {
                var field = propertyInfo.GetCustomAttribute<PocoColumnAttribute>(false) ?? new PocoColumnAttribute(propertyInfo.Name);

                if (field.DeltaType != EDeltaType.IgnoreField && !field.Skip)
                {
                    var column = new TableColumn()
                    {
                        Name = string.IsNullOrEmpty(field.Name) ? propertyInfo.Name : field.Name,
                        DeltaType = field.DeltaType,
                        DataType = field.DataType == ETypeCode.Unknown ? GetTypeCode(propertyInfo.PropertyType, out _) : field.DataType,
                        AllowDbNull = propertyInfo.PropertyType == typeof(string) || Nullable.GetUnderlyingType(propertyInfo.PropertyType) != null || field.AllowDbNull,
                        MaxLength = field.MaxLength >= 0 ? (int?)field.MaxLength : null,
                        Precision = field.Precision >= 0 ? (int?)field.Precision : null,
                        Scale = field.Scale >= 0 ? (int?)field.Scale : null,
                    };

                    table.Columns.Add(column);
                    mappings.Add(new PocoTableMapping(propertyInfo, position,field.IsKey));

                    if(field.DeltaType == EDeltaType.DbAutoIncrement)
                    {
                        AutoIncrementProperty = propertyInfo;
                    }

                    position++;
                }
            }

            Table = table;
            TableMappings = mappings;
        }

        public PocoTable(Table table)
        {
            var mappings = new List<PocoTableMapping>();

            foreach (var propertyInfo in typeof(T).GetProperties())
            {
                var field = propertyInfo.GetCustomAttribute<PocoColumnAttribute>(false) ?? new PocoColumnAttribute(propertyInfo.Name);
                var fieldName = string.IsNullOrEmpty(field.Name) ? propertyInfo.Name : field.Name;
                var isKey = field.IsKey;

                var position = table.Columns.GetOrdinal(fieldName);
                if(position >= 0)
                {
                    mappings.Add((new PocoTableMapping(propertyInfo, position, isKey)));
                }
            }
            Table = table;
            TableMappings = mappings;
        }

        /// <summary>
        /// Creates a table in the underlying connection
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="dropTable"></param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public Task CreateTable(Connection connection, bool dropTable, CancellationToken cancellationToken = default)
        {
            return connection.CreateTable(Table, dropTable, cancellationToken);
        }

        /// <summary>
        /// Checks if the table exists in the target database
        /// </summary>
        /// <returns>True = table exists.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public Task<bool> TableExists(Connection connection, CancellationToken cancellationToken = default)
        {
            return connection.TableExists(Table, cancellationToken);
        }

        public InsertQuery PrepareInsert(T item)
        {
            var columns = new List<QueryColumn>();

            foreach (var mapping in TableMappings)
            {
                var column = Table.Columns[mapping.Position];
                if (column.DeltaType != EDeltaType.DbAutoIncrement)
                {
                    var value = Operations.Parse(column.DataType, mapping.PropertyInfo.GetValue(item));
                    var queryColumn = new QueryColumn(column, value);
                    columns.Add(queryColumn);
                }
            }


            var insertQuery = new InsertQuery(columns);

            return insertQuery;
        }

        /// <summary>
        /// Inserts the item into the connection.
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="item">Item.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public Task ExecuteInsert(Connection connection, T item, CancellationToken cancellationToken = default)
        {

            var insertQuery = PrepareInsert(item);
            return ExecuteInsert(connection, insertQuery, item, cancellationToken);
        }

        public async Task ExecuteInsert(Connection connection, InsertQuery insertQuery, T item, CancellationToken cancellationToken = default)
        {
            var insertResult = await connection.ExecuteInsert(Table, new List<InsertQuery>() { insertQuery }, cancellationToken);

            if(AutoIncrementProperty != null)
            {
                AutoIncrementProperty.SetValue(item, insertResult);
            }
        }


        public DeleteQuery PrepareDelete(T item)
        {
            var filters = new Filters();

            foreach (var mapping in TableMappings)
            {
                var column = Table.Columns[mapping.Position];
                if (mapping.IsKey)
                {
                    var filter = new Filter(column, ECompare.IsEqual, mapping.PropertyInfo.GetValue(item));
                    filters.Add(filter);
                }
            }

            var deleteQuery = new DeleteQuery(Table.Name, filters);

            return deleteQuery;
        }

        /// <summary>
        /// Deletes the item, based on the item values that contain a deltaType = NaturalKey
        /// </summary>
        /// <returns>The delete.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="item">Item to delete</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public Task ExecuteDelete(Connection connection, T item, CancellationToken cancellationToken = default)
        {
            var deleteQuery = PrepareDelete(item);
            return ExecuteDelete(connection, deleteQuery, cancellationToken);
        }

        public Task ExecuteDelete(Connection connection, DeleteQuery deleteQuery, CancellationToken cancellationToken = default)
        {
            return connection.ExecuteDelete(Table, new List<DeleteQuery>() { deleteQuery }, cancellationToken);

        }

        public UpdateQuery PrepareUpdate(T item)
        {
            var filters = new Filters();
            var updateColumns = new List<QueryColumn>();

            foreach (var mapping in TableMappings)
            {
                var column = Table.Columns[mapping.Position];
                var value = Operations.Parse(column.DataType, mapping.PropertyInfo.GetValue(item));

                if (mapping.IsKey)
                {
                    var filter = new Filter(column, ECompare.IsEqual, value);
                    filters.Add(filter);
                }
                else
                {
                    var updateColumn = new QueryColumn(column, value);
                    updateColumns.Add(updateColumn);
                }
            }

            var updateQuery = new UpdateQuery(updateColumns, filters);

            return updateQuery;
        }

        /// <summary>
        /// Deletes the item, based on the item values that contain a deltaType = NaturalKey
        /// </summary>
        /// <returns>The delete.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="item">Item to delete</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public Task ExecuteUpdate(Connection connection, T item, CancellationToken cancellationToken = default)
        {
            var updateQuery = PrepareUpdate(item);
            return ExecuteUpdate(connection, updateQuery, cancellationToken);
        }

        public Task ExecuteUpdate(Connection connection, UpdateQuery updateQuery, CancellationToken cancellationToken = default)
        {
            return connection.ExecuteUpdate(Table, new List<UpdateQuery>() { updateQuery }, cancellationToken);
        }

        /// <summary>
        /// Inserts the items into the connection.
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="items">Item.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public Task ExecuteInsertBulk(Connection connection, IEnumerable<T> items, CancellationToken cancellationToken = default)
        {
            var pocoTable = new PocoTable<T>(Table.Copy());

            var pocoReader = new PocoReader<T>(pocoTable, items);
            pocoReader.Open(cancellationToken);
            return connection.ExecuteInsertBulk(Table, pocoReader, cancellationToken);
        }
    }


}
