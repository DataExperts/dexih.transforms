using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{
    /// <summary>
    /// TransformSource is a starting point in a chain of transforms and accepts any standard DbDataReader as an input.
    /// </summary>
    public class ReaderDbDataReader : Transform
    {
        public ReaderDbDataReader() { }

        /// <summary>
        /// Initialises a transform source.  
        /// </summary>
        /// <param name="inReader">An initialized DbDataReader.</param>
        /// <param name="sortFields">A list of already sorted fields in the inReader.  If the fields are not sorted in the source data and sortfields are set, transforms such as group, row, join will fail or return incorrect results.</param>
        public ReaderDbDataReader(DbDataReader inReader, Sorts sortFields = (Sorts) null)
        {
            InReader = inReader;
            var fieldCount = inReader.FieldCount;

            CacheTable = new Table("InReader") {OutputSortFields = sortFields};
            
            GeneratedQuery = new SelectQuery()
            {
                Sorts = sortFields
            };

#if NET462
            try
            {

                DataTable schema = inReader.GetSchemaTable();
                CacheTable.TableName = schema.Rows[0][SchemaTableColumn.BaseTableName].ToString();

                foreach (DataRow row in schema.Rows)
                {
                    var column = new TableColumn();
                    column.ColumnName = row[SchemaTableColumn.ColumnName].ToString();
                    column.DataType = DataType.GetTypeCode((Type)row[SchemaTableColumn.DataType]);
                    column.MaxLength = Convert.ToInt32(row[SchemaTableColumn.ColumnSize]);
                    column.Scale = Convert.ToInt32(row[SchemaTableColumn.NumericScale]);
                    column.Precision = Convert.ToInt32(row[SchemaTableColumn.NumericPrecision]);
                    CacheTable.Columns.Add(column);
                }
            }
            catch (Exception)
            {
                for (int i = 0; i < fieldCount; i++)
                {
                    CacheTable.Columns.Add(new TableColumn(inReader.GetName(i)));
                }
            }
#else
            //if we can't get a column schema we will have to settle for column names only
            if (!inReader.CanGetColumnSchema())
            {
                for (var i = 0; i < fieldCount; i++)
                {
                    CacheTable.Columns.Add(new TableColumn(inReader.GetName(i)));
                }
            }
            else
            {
                var columnSchema = inReader.GetColumnSchema();
                CacheTable.Name = columnSchema[0].BaseTableName;

                foreach(var columnDetail in columnSchema)
                {
                    var column = new TableColumn
                    {
                        Name = columnDetail.ColumnName,
                        DataType = Dexih.Utils.DataType.DataType.GetTypeCode(columnDetail.DataType, out var rank),
                        Rank = rank,
                        MaxLength = columnDetail.ColumnSize,
                        Scale = columnDetail.NumericScale,
                        Precision = columnDetail.NumericPrecision
                    };
                    CacheTable.Columns.Add(column);
                }
            }
#endif
        }

        /// <summary>
        /// Initializes a transform source reader using the table to describe the fields.
        /// </summary>
        /// <param name="inReader"></param>
        /// <param name="table"></param>
        public ReaderDbDataReader(DbDataReader inReader, Table table)
        {
            InReader = inReader;

            CacheTable = table;

            if (table.OutputSortFields != null)
            {
                GeneratedQuery = new SelectQuery()
                {
                    Sorts = table.OutputSortFields
                };
            }
        }

        public DbDataReader InReader { get; set; }

        protected override Task CloseConnections()
        {
            return InReader?.CloseAsync();
        }

        public override string TransformName { get; } = "Generic Database Reader";
        
        public override Dictionary<string, object> TransformProperties()
        {
            if (InReader != null)
            {
                return new Dictionary<string, object>()
                {
                    {"ReaderType", InReader.GetType().Name},
                };
            }

            return null;
        }

        public override bool ResetTransform()
        {
            throw new TransformException("The source reader cannot be reset as the DbReader is a forward only reader");
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            var success = await InReader.ReadAsync(cancellationToken);
            object[] newRow;
            if (success)
            {
                newRow = new object[FieldCount];
                InReader.GetValues(newRow);
            }
            else
                newRow = null;

            return newRow;
        }
    }
}
