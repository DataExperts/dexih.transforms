using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{
    public class TransformRowCreator : Transform
    {
        private int _currentRow;

        public int StartAt { get; set; }
        public int EndAt { get; set; }
        public int Increment { get; set; }

        public void InitializeRowCreator(int startAt, int endAt, int increment)
        {
            StartAt = startAt;
            EndAt = endAt;
            Increment = increment;

            Initialize();
        }

        public override bool Initialize()
        {
            _currentRow = StartAt-1;
            return true;
        }

        public override int FieldCount => 1;

        public override bool CanRunQueries => false;

        public override bool PrefersSort => false;
        public override bool RequiresSort => false;


        public override string GetName(int i)
        {
            if (i == 0)
                return "RowNumber";
            throw new Exception("There is only one column available in the rowcreator transform");
        }

        public override int GetOrdinal(string columnName)
        {
            if (columnName == "RowNumber")
                return 0;
            throw new Exception("There is only one column with the name RowNumber in rowcreator transform");
        }

     //   public override DataTable GetSchemaTable()
     //   {
     //       DataTable schema = new DataTable("SchemaTable")
     //       {
     //           Locale = CultureInfo.InvariantCulture,
     //           MinimumCapacity = FieldCount
     //       };

     //       schema.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.BaseColumnName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.BaseSchemaName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.BaseTableName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ColumnName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.DataType, typeof(object)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsAliased, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsExpression, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsKey, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsLong, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(short)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.NumericScale, typeof(short)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ProviderType, typeof(int)).ReadOnly = true;

     //       schema.Columns.Add(SchemaTableOptionalColumn.BaseCatalogName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.BaseServerName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsHidden, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsRowVersion, typeof(bool)).ReadOnly = true;

     //       // null marks columns that will change for each row
     //       object[] schemaRow = {
     //               false,					// 00- AllowDBNull
					//"RowNumber",			// 01- BaseColumnName
     //               string.Empty,			// 02- BaseSchemaName
					//string.Empty,			// 03- BaseTableName
					//"RowNumber",		    // 04- ColumnName
					//0,					// 05- ColumnOrdinal
					//int.MaxValue,			// 06- ColumnSize
					//typeof(int),			// 07- DataType
					//false,					// 08- IsAliased
					//false,					// 09- IsExpression
					//true,					// 10- IsKey
					//false,					// 11- IsLong
					//true,					// 12- IsUnique
					//DBNull.Value,			// 13- NumericPrecision
					//DBNull.Value,			// 14- NumericScale
					//(int) DbType.String,	// 15- ProviderType

					//string.Empty,			// 16- BaseCatalogName
					//string.Empty,			// 17- BaseServerName
					//true,					// 18- IsAutoIncrement
					//false,					// 19- IsHidden
					//true,					// 20- IsReadOnly
					//false					// 21- IsRowVersion
			  //};

     //       schema.Rows.Add(schemaRow);
     //       return schema;

     //   }

        protected override bool ReadRecord()
        {
            _currentRow = _currentRow + Increment;
            if (_currentRow > EndAt)
                return false;
            CurrentRow = new object[] { _currentRow };
            return true;
        }

        public override bool ResetValues()
        {
            _currentRow = StartAt-1;
            return true; // not applicable for filter.
        }

        public override string Details()
        {
            return "RowCreator: Starts at: " + StartAt + ", Ends At: " + EndAt;
        }

        public override List<Sort> RequiredSortFields()
        {
            return null;
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            return null;
        }

        public override List<Sort> OutputSortFields()
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue> LookupRow(List<Filter> filters)
        {
            throw new NotImplementedException();
        }
    }
}
