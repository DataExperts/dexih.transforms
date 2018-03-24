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

                // no attributes, then just create a columns with the same name as the property.
                if(field == null)
                {
                    var column = new TableColumn(propertyInfo.Name)
                    {
                        DeltaType = TableColumn.EDeltaType.TrackingField,
                        DataType = Dexih.Utils.DataType.DataType.GetTypeCode(propertyInfo.PropertyType)
                    };

                    table.Columns.Add(column);
                    mappings.Add(new PocoTableMapping(propertyInfo, position, false));
                    position++; 
                }
                else if (field.DeltaType != TableColumn.EDeltaType.IgnoreField && !field.Skip)
                {
                    var column = new TableColumn()
                    {
                        Name = string.IsNullOrEmpty(field.Name) ? propertyInfo.Name : field.Name,
                        DeltaType = field.DeltaType,
                        DataType = field.DataType == ETypeCode.Unknown ? Dexih.Utils.DataType.DataType.GetTypeCode(propertyInfo.PropertyType) : field.DataType,
                        AllowDbNull = Nullable.GetUnderlyingType(propertyInfo.PropertyType) != null || field.AllowDbNull,
                        MaxLength = field.MaxLength >= 0 ? (int?)field.MaxLength : null,
                        Precision = field.Precision >= 0 ? (int?)field.Precision : null,
                        Scale = field.Scale >= 0 ? (int?)field.Scale : null,
                    };

                    table.Columns.Add(column);
                    mappings.Add(new PocoTableMapping(propertyInfo, position,field.IsKey));

                    if(field.DeltaType == TableColumn.EDeltaType.AutoIncrement)
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
                var fieldName = string.IsNullOrEmpty(field?.Name) ? propertyInfo.Name : field.Name;
                var isKey = field != null && field.IsKey;

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
        /// Inserts the item into the connection.
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="item">Item.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task CreateTable(Connection connection, bool dropTable, CancellationToken cancellationToken)
        {
            await connection.CreateTable(Table, dropTable, cancellationToken);
        }

        /// <summary>
        /// Checks if the table exists in the target database
        /// </summary>
        /// <returns>True = table exists.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task<bool> TableExists(Connection connection, CancellationToken cancellationToken)
        {
            return await connection.TableExists(Table, cancellationToken);
        }

        /// <summary>
        /// Inserts the item into the connection.
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="item">Item.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task ExecuteInsert(Connection connection, T item, CancellationToken cancellationToken)
        {
            var columns = new List<QueryColumn>();

            foreach (var mapping in TableMappings)
            {
                var column = Table.Columns[mapping.Position];
                if (column.DeltaType != TableColumn.EDeltaType.AutoIncrement)
                {
                    var value = DataType.TryParse(column.DataType, mapping.PropertyInfo.GetValue(item));
                    var queryColumn = new QueryColumn(column, value);
                    columns.Add(queryColumn);
                }
            }


            var insertQuery = new InsertQuery(Table.Name, columns);

            var insertResult = await connection.ExecuteInsert(Table, new List<InsertQuery>() { insertQuery }, cancellationToken);

            if(AutoIncrementProperty != null)
            {
                AutoIncrementProperty.SetValue(item, insertResult);
            }

        }

        /// <summary>
        /// Deletes the item, based on the item values that contain a deltaType = NaturalKey
        /// </summary>
        /// <returns>The delete.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="item">Item to delete</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task ExecuteDelete(Connection connection, T item, CancellationToken cancellationToken)
        {
            var filters = new List<Filter>();

            foreach (var mapping in TableMappings)
            {
                var column = Table.Columns[mapping.Position];
                if (mapping.IsKey)
                {
                    var filter = new Filter(column, Filter.ECompare.IsEqual, mapping.PropertyInfo.GetValue(item));
                    filters.Add(filter);
                }
            }

            var deleteQuery = new DeleteQuery(Table.Name, filters);

            await connection.ExecuteDelete(Table, new List<DeleteQuery>() { deleteQuery }, cancellationToken);
        }

        /// <summary>
        /// Deletes the item, based on the item values that contain a deltaType = NaturalKey
        /// </summary>
        /// <returns>The delete.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="item">Item to delete</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task ExecuteUpdate(Connection connection, T item, CancellationToken cancellationToken)
        {
            var filters = new List<Filter>();
            var updateColumns = new List<QueryColumn>();

            foreach (var mapping in TableMappings)
            {
                var column = Table.Columns[mapping.Position];
                var value = DataType.TryParse(column.DataType, mapping.PropertyInfo.GetValue(item));

                if (mapping.IsKey)
                {
                    var filter = new Filter(column, Filter.ECompare.IsEqual, value);
                    filters.Add(filter);
                }
                else
                {
                    var updateColumn = new QueryColumn(column, value);
                    updateColumns.Add(updateColumn);
                }
            }

            var updateQuery = new UpdateQuery(Table.Name, updateColumns, filters);

            await connection.ExecuteUpdate(Table, new List<UpdateQuery>() { updateQuery }, cancellationToken);
        }

        /// <summary>
        /// Inserts the items into the connection.
        /// </summary>
        /// <returns>The insert.</returns>
        /// <param name="connection">Connection.</param>
        /// <param name="items">Item.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task ExecuteInsertBulk(Connection connection, IEnumerable<T> items, CancellationToken cancellationToken)
        {
            var pocoTable = new PocoTable<T>(Table.Copy());

            var pocoReader = new PocoReader<T>(pocoTable, items);
            await connection.ExecuteInsertBulk(Table, pocoReader, cancellationToken);
        }
    }


}
