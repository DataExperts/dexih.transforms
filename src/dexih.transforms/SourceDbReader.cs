using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using System.Data;

namespace dexih.transforms
{
    /// <summary>
    /// TransformSource is a starting point in a chain of transforms and accepts any standard DbDataReader as an input.
    /// </summary>
    public class SourceDbReader : Transform
    {
        public SourceDbReader() { }

        /// <summary>
        /// Initialises a transform source.  
        /// </summary>
        /// <param name="inReader">An initialized DbDataReader.</param>
        /// <param name="sortFields">A list of already sorted fields in the inReader.  If the fields are not sorted in the source data and sortfields are set, transforms such as group, row, join will fail or return incorrect results.</param>
        public SourceDbReader(DbDataReader inReader, List<Sort> sortFields = null)
        {
            InReader = inReader;

            CachedTable = new Table("InReader");

#if NET451
            DataTable schema = inReader.GetSchemaTable();
            CachedTable.TableName = schema.Rows[0][SchemaTableColumn.BaseTableName].ToString();

            foreach(DataRow row in schema.Rows)
            {
                var column = new TableColumn();
                column.ColumnName = row[SchemaTableColumn.ColumnName].ToString();
                column.DataType = DataType.GetTypeCode((Type)row[SchemaTableColumn.DataType]);
                column.MaxLength = Convert.ToInt32(row[SchemaTableColumn.ColumnSize]);
                column.Scale = Convert.ToInt32(row[SchemaTableColumn.NumericScale]);
                column.Precision = Convert.ToInt32(row[SchemaTableColumn.NumericPrecision]);
            }
            for (int i = 0; i< inReader.FieldCount; i++)
            {
                CachedTable.Columns.Add(new TableColumn(inReader.GetName(i)));
            }
#else
            //if we can't get a column schema we will have to settle for column names only
            if (!inReader.CanGetColumnSchema())
            {
                for (int i = 0; i < inReader.FieldCount; i++)
                {
                    CachedTable.Columns.Add(new TableColumn(inReader.GetName(i)));
                }
            }
            else
            {
                var columnSchema = inReader.GetColumnSchema();
                CachedTable.TableName = columnSchema[0].BaseTableName;

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

        public DbDataReader InReader { get; set; }

        public override bool CanRunQueries
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override string Details()
        {
            return "DataSource";
        }

        public override bool Initialize()
        {
            return true;
        }

        public override bool ResetValues()
        {
            return true;
        }

        protected override bool ReadRecord()
        {
            bool success = InReader.Read();
            if (success)
            {
                CurrentRow = new object[FieldCount];
                InReader.GetValues(CurrentRow);
            }
            else
                CurrentRow = null;

            return success;
        }
    }
}
