using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using System.Data;
using System.Threading;

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
        public ReaderDbDataReader(DbDataReader inReader, List<Sort> sortFields = null)
        {
            InReader = inReader;

            CacheTable = new Table("InReader");

#if NET46
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
                for (int i = 0; i < inReader.FieldCount; i++)
                {
                    CacheTable.Columns.Add(new TableColumn(inReader.GetName(i)));
                }
            }
#else
            //if we can't get a column schema we will have to settle for column names only
            if (!inReader.CanGetColumnSchema())
            {
                for (int i = 0; i < inReader.FieldCount; i++)
                {
                    CacheTable.Columns.Add(new TableColumn(inReader.GetName(i)));
                }
            }
            else
            {
                var columnSchema = inReader.GetColumnSchema();
                CacheTable.TableName = columnSchema[0].BaseTableName;

                foreach(var columnDetail in columnSchema)
                {
                    var column = new TableColumn();
                    column.ColumnName = columnDetail.ColumnName;
                    column.DataType = DataType.GetTypeCode(columnDetail.DataType);
                    column.MaxLength = columnDetail.ColumnSize;
                    column.Scale = columnDetail.NumericScale;
                    column.Precision = columnDetail.NumericPrecision;
                }
            }
#endif

            SortFields = sortFields;
        }

        /// <summary>
        /// Initializes a transform source reader using the table to describe the fields.
        /// </summary>
        /// <param name="inReader"></param>
        /// <param name="table"></param>
        /// <param name="sortFields"></param>
        public ReaderDbDataReader(DbDataReader inReader, Table table, List<Sort> sortFields = null)
        {
            InReader = inReader;

            CacheTable = table;
            SortFields = sortFields;
        }

        public DbDataReader InReader { get; set; }


        public override string Details()
        {
            return "DataSource";
        }

        public override bool InitializeOutputFields()
        {
            return true;
        }

        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(false, "The source reader cannot be reset as the DbReader is a forward only reader", null);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            bool success = await InReader.ReadAsync();
            object[] newRow;
            if (success)
            {
                newRow = new object[FieldCount];
                InReader.GetValues(newRow);
            }
            else
                newRow = null;

            return new ReturnValue<object[]>(success, newRow);
        }
    }
}
