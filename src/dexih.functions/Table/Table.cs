using dexih.functions.Query;
using Dexih.Utils.DataType;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;

namespace dexih.functions
{
    
    [Serializable]
    public class Table
    {

        #region Initializers
        public Table()
        {
            Data = new TableCache(0);
            Columns = new TableColumns();
        }

        public Table(string tableName, TableColumns columns, TableCache data = null) 
        {
            Name = tableName;
            LogicalName = DefaultLogicalName();
            BaseTableName = CleanString(tableName);
            Columns = columns??new TableColumns();
            Data = data?? new TableCache();
        }

        public Table(string tableName, int maxRows, params TableColumn[] columns) 
        {
            Name = tableName;
            LogicalName = DefaultLogicalName();
            BaseTableName = CleanString(tableName);
            Columns = new TableColumns();
            foreach (var column in columns)
                Columns.Add(column);

            Data = new TableCache(maxRows);
        }

        public Table(string tableName, int maxRows, TableColumns columns)
        {
            Name = tableName;
            LogicalName = DefaultLogicalName();
            BaseTableName = CleanString(tableName);
            Columns = columns;
            Data = new TableCache(maxRows);
        }

		public Table(string tableName, int maxRows = 0)
		{
		    Name = tableName;
            LogicalName = DefaultLogicalName();
            BaseTableName = CleanString(tableName);
			Columns = new TableColumns();
			Data = new TableCache(maxRows);
		}

        public Table(string tableName, string schema, int maxRows = 0) 
        {
            Name = tableName;
			Schema = schema;
            LogicalName = DefaultLogicalName();
            BaseTableName = CleanString(tableName);
            Columns = new TableColumns();
            Data = new TableCache(maxRows);
        }

        protected string DefaultLogicalName()
        {
            var name = Name?.Replace("\"", "")??"";

            if (string.IsNullOrEmpty(Schema))
            {
                return name;
            }
            else
            {
                return Schema + "." + name;
            }
        }

        /// <summary>
        /// Removes all non alphanumeric characters from the string
        /// </summary>
        /// <returns></returns>
        private string CleanString(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            var arr = value.Where(c => (char.IsLetterOrDigit(c))).ToArray();
            var newValue = new string(arr);
            return newValue;
        }

        #endregion

        [JsonConverter(typeof(StringEnumConverter))]
        public enum ETableType
        {
            Table,
            View,
            Query
            
        }

        #region Properties

        /// <summary>
        /// Reference to the phsycal table name.
        /// </summary>
        public string Name { get; set; }

		/// <summary>
		/// The table schema or owner.
		/// </summary>
		/// <value>The table schema.</value>
		public string Schema { get; set; }

        /// <summary>
        /// The name of the source connection when pointing to an another hub
        /// </summary>
        /// <value>The table connection.</value>
        public string SourceConnectionName { get; set; }

        /// <summary>
        /// A logical name for the table.
        /// </summary>
        public string LogicalName { get; set; }

        /// <summary>
        /// Table description.
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Is the original base table name.
        /// </summary>
        public string BaseTableName { get; set; }

        /// <summary>
        /// Indicates the type of table (i.e. table, view etc.)
        /// </summary>
        public ETableType TableType { get; set; }

        /// <summary>
        /// Indicates if the table contains versions (history) of data change, such as sql temporal tables.
        /// </summary>
        public bool IsVersioned { get; set; }

        /// <summary>
        /// Indicates if this is a sql (or other) type query.
        /// </summary>
        public bool UseQuery { get; set; }

        /// <summary>
        /// Sql Query string (or query string for other db types)
        /// </summary>
        public string QueryString { get; set; }
     
        /// <summary>
        /// Indicates the output sort fields for the table.
        /// </summary>
        /// <returns></returns>
        public virtual List<Sort> OutputSortFields { get; set; }

//        /// <summary>
//        /// Indicates the key that should be used when running update/delete operations against the target.
//        /// </summary>
//        public List<string> KeyFields { get; set; }

        public TableCache Data { get; set; }

        public TableColumns Columns { get; set; }

		// public Dictionary<string, string> ExtendedProperties { get; set; }

        public TableColumn this[string columnName] => Columns[columnName];
        
        public TableColumn this[int ordinal] => Columns[ordinal];

        /// <summary>
        /// Maximum levels to recurse through structured data when importing columns.
        /// </summary>
        public int MaxImportLevels { get; set; } = 10;


        #endregion

        #region Lookup
        
        /// <summary>
        /// stores indexes
        /// first int[] contains array of column ordinals, and returns dictionary
        /// second dictionary contains object[] with are values to lookup and returns rows numbers
        /// containing index results.
        /// </summary>
        private List<(int[] columnOrdinals, Dictionary<object[], List<int>> index)> _indexes;

        /// <summary>
        /// Performs a row scan lookup on the data contained in the table.
        /// </summary>
        /// <param name="filters">Filter for the lookup.  For an index to be used, the filters must be in the same column order as the index.</param>
        /// <param name="startRow"></param>
        /// <returns></returns>
        public object[] LookupSingleRow(List<Filter> filters, int startRow = 0)
        {
            try
            {
                // use the index to reduce the scan rows
                var data = IndexLookup(filters) ?? Data;
                
                //scan the data for a matching row.  
                for (var i = startRow; i < Data.Count(); i++)
                {
                    if (RowMatch(filters, Data[i]))
                        return Data[i];
                }

                return null;
            }
            catch (Exception ex)
            {
                throw new TableException("The lookup row failed.  " + ex.Message, ex);
            }
        }

        public List<object[]> LookupMultipleRows(List<Filter> filters, int startRow = 0)
        {
            try
            {
                List<object[]> rows = null;

                // use the index to reduce the scan rows
                var data = IndexLookup(filters) ?? Data;

                //scan the data for a matching row.  
                for (var i = startRow; i < Data.Count(); i++)
                {
                    if (RowMatch(filters, Data[i]))
                    {
                        if (rows == null)
                            rows = new List<object[]>();
                        rows.Add(Data[i]);
                    }
                }

                return rows;
            }
            catch (Exception ex)
            {
                throw new TableException("The lookup multiple rows failed.  " + ex.Message, ex);
            }
        }

        public bool RowMatch(IEnumerable<Filter> filters, object[] row)
        {
            var isMatch = true;

            foreach (var filter in filters)
            {
                object value1;
                object value2;

                if (filter.Column1 == null)
                {
                    value1 = filter.Value1;
                }
                else
                {
                    value1 = row[GetOrdinal(filter.Column1.Name)];
                }

                if (filter.Column2 == null)
                    value2 = filter.Value2;
                else
                {
                    value2 = row[GetOrdinal(filter.Column2.Name)];
                }

                isMatch = Operations.Evaluate(filter.Operator, filter.CompareDataType, value1, value2);
                
                if (!isMatch)
                    break;
            }

            return isMatch;
        }

        public void AddIndex(string column)
        {
            AddIndex(new [] {column});
        }
        
        public void AddIndex(IEnumerable<string> columns)
        {
            var sortedColumns = columns.OrderBy(c => c).ToArray();
            var columnOrdinals = sortedColumns.Select(GetOrdinal).ToArray();
            var index = new Dictionary<object[], List<int>>();

            var rowNumber = 0;
            foreach (var row in Data)
            {
                var values = columnOrdinals.Select(c => row[c]).ToArray();
                if (index.TryGetValue(values, out var rowOrdinals))
                {
                    rowOrdinals.Add(rowNumber);
                }
                else
                {
                    index.Add(values, new List<int>() {rowNumber});
                }
                rowNumber++;
            }

            if (_indexes == null)
            {
                _indexes = new List<(int[] columnOrdinals, Dictionary<object[], List<int>> index)> { (columnOrdinals, index)};
            }
            else
            {
                _indexes.Add((columnOrdinals, index));    
            }
            
            
        }

        private IEnumerable<object[]> IndexLookup(IEnumerable<Filter> filters)
        {
            var indexFilter = filters
                .Where(c => c.Operator == ECompare.IsEqual && c.Column1 != null && c.Column2 is null)
                .Select(c => ((int ordinal, object value)) (GetOrdinal(c.Column1.ColumnGroup), c.Value2))
                .OrderBy(c => c.ordinal);

            return IndexLookup(indexFilter);
        }

        /// <summary>
        /// Returns a set of rows based on the index.
        /// Note, this is a performance routine to reduce rows searched.  More rows may
        /// be returned if the index only contains some columns from the index.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        private IEnumerable<object[]> IndexLookup(IEnumerable<(int columnOrdinal, object value)> filters)
        {
            if (_indexes == null) return null;
            
            var sortedFilter = filters.OrderBy(c => c.columnOrdinal).ToArray();

            foreach (var (columnOrdinals, index) in _indexes)
            {
                if (sortedFilter.Length <= columnOrdinals.Length)
                {
                    var filter = sortedFilter.Take(columnOrdinals.Length).ToArray();
                    if (columnOrdinals.SequenceEqual(filter.Select(c => c.columnOrdinal)))
                    {
                        if(index.TryGetValue(filter.Select(c=>c.value).ToArray(), out var rowIndexes))
                        {
                            var rows = new object[rowIndexes.Count][];
                            for (var i = 0; i < rowIndexes.Count; i++)
                            {
                                rows[i] = Data[rowIndexes[i]];
                            }
                            return rows;
                        }
                        
                        return new List<object[]>();
                    }
                }
            }

            return null;
        }
        
        #endregion


        /// <summary>
        /// Adds the standard set of audit columns to the table.  
        /// </summary>
        public void AddAuditColumns(string surrogateKeyName = "SurrogateKey")
        {

            //add the audit columns if they don't exist
            //get some of the key fields to save looking up for each row.
            var colValidFrom = GetColumn(TableColumn.EDeltaType.ValidFromDate);
            var colValidTo = GetColumn(TableColumn.EDeltaType.ValidToDate);
            var colCreateDate = GetColumn(TableColumn.EDeltaType.CreateDate);
            var colUpdateDate = GetColumn(TableColumn.EDeltaType.UpdateDate);
            var colSurrogateKey = GetColumn(TableColumn.EDeltaType.AutoIncrement);
            var colIsCurrentField = GetColumn(TableColumn.EDeltaType.IsCurrentField);
            var colVersionField = GetColumn(TableColumn.EDeltaType.Version);
            var colCreateAuditKey = GetColumn(TableColumn.EDeltaType.CreateAuditKey);
            var colUpdateAuditKey = GetColumn(TableColumn.EDeltaType.UpdateAuditKey);
//            var colSourceSurrogateKey = GetDeltaColumn(TableColumn.EDeltaType.SourceSurrogateKey);
//            var colRejectedReason = GetDeltaColumn(TableColumn.EDeltaType.RejectedReason);

            if (colValidFrom == null)
            {
                colValidFrom = new TableColumn("ValidFromDate", ETypeCode.DateTime) { DeltaType = TableColumn.EDeltaType.ValidFromDate };
                Columns.Add(colValidFrom);
            }
            
            if (colValidTo == null)
            {
                colValidTo = new TableColumn("ValidToDate", ETypeCode.DateTime) { DeltaType = TableColumn.EDeltaType.ValidToDate };
                Columns.Add(colValidTo);
            }
            
            if (colCreateDate == null)
            {
                colCreateDate = new TableColumn("CreateDate", ETypeCode.DateTime) { DeltaType = TableColumn.EDeltaType.CreateDate };
                Columns.Add(colCreateDate);
            }
            
            if (colUpdateDate == null)
            {
                colUpdateDate = new TableColumn("UpdateDate", ETypeCode.DateTime) { DeltaType = TableColumn.EDeltaType.UpdateDate };
                Columns.Add(colUpdateDate);
            }
            
            if (colSurrogateKey == null)
            {
                colSurrogateKey = new TableColumn(surrogateKeyName, ETypeCode.Int64) { DeltaType = TableColumn.EDeltaType.AutoIncrement };
                Columns.Add(colSurrogateKey);
            }

            if (colIsCurrentField == null)
            {
                colIsCurrentField = new TableColumn("IsCurrent", ETypeCode.Boolean) { DeltaType = TableColumn.EDeltaType.IsCurrentField };
                Columns.Add(colIsCurrentField);
            }

            if (colVersionField == null)
            {
                colVersionField = new TableColumn("Version", ETypeCode.Int32) { DeltaType = TableColumn.EDeltaType.Version };
                Columns.Add(colVersionField);
            }
            
            if (colCreateAuditKey == null)
            {
                colCreateAuditKey = new TableColumn("CreateAuditKey", ETypeCode.Int64) { DeltaType = TableColumn.EDeltaType.CreateAuditKey };
                Columns.Add(colCreateAuditKey);
            }

            if (colUpdateAuditKey == null)
            {
                colUpdateAuditKey = new TableColumn("UpdateAuditKey", ETypeCode.Int64) { DeltaType = TableColumn.EDeltaType.UpdateAuditKey };
                Columns.Add(colUpdateAuditKey);
            }
        }

        /// <summary>
        /// Create as a rejected table based on the current table.
        /// </summary>
        /// <returns></returns>
        public Table GetRejectedTable(string rejectedTableName)
        {
            if (string.IsNullOrEmpty(rejectedTableName)) return null;

            var table = Copy();

            // reset delta types on reject table to ensure the reject table contains no keys.
            foreach(var column in table.Columns)
            {
                column.DeltaType = TableColumn.EDeltaType.TrackingField;
            }

            table.Name = rejectedTableName;
            table.Description = "Rejected table for: " + Description;

            if(GetColumn(TableColumn.EDeltaType.RejectedReason) == null)
                table.Columns.Add(new TableColumn("RejectedReason", ETypeCode.String, TableColumn.EDeltaType.RejectedReason));

            return table;
        }

        /// <summary>
        /// Gets the secured version of the table, with columns tagged as secured set to appropriate string type
        /// </summary>
        /// <returns></returns>
        public Table GetSecureTable()
        {
            var table = Copy();

            foreach(var column in table.Columns)
            {
                column.SecurityFlag = Columns[column.Name].SecurityFlag;
                if(column.SecurityFlag != TableColumn.ESecurityFlag.None)
                {
                    column.DataType = ETypeCode.String;
                    column.MaxLength = 250;
                }
            }

            return table;
        }

        /// <summary>
        /// Creates a copy of the table, excluding cached data, and sort columns
        /// </summary>
        /// <returns></returns>
        public Table Copy(bool removeSchema = false, bool removeIgnoreColumns = false)
        {
            var table = new Table(Name, Schema)
            {
                Description = Description,
                LogicalName = LogicalName
            };


            foreach (var column in Columns)
            {
                if (!removeIgnoreColumns || column.DeltaType != TableColumn.EDeltaType.IgnoreField)
                {
                    var newCol = column.Copy();
                    if (removeSchema) newCol.ReferenceTable = null;

                    table.Columns.Add(newCol);
                }
            }

            return table;
        }

        public void AddColumn(string columnName, ETypeCode dataType = ETypeCode.String, TableColumn.EDeltaType deltaType = TableColumn.EDeltaType.TrackingField, byte arrayDimensions = 0)
        {
            if (Columns == null)
                Columns = new TableColumns();

            Columns.Add(new TableColumn(columnName, dataType, deltaType, arrayDimensions, Name));
        }

        public void AddRow(params object[] values)
        {
            if (values.Length != Columns.Count())
                throw new Exception("The number of parameters must match the number of columns (" + Columns.Count + ") precisely.");

            var row = new object[Columns.Count];
            values.CopyTo(row, 0);

            Data.Add(values);

            if (_indexes == null) return;
            
            //update indexes
            foreach (var (ordinals, value) in _indexes)
            {
                var indexValues = ordinals.Select(c => values[c]).ToArray();
                if (value.TryGetValue(indexValues, out var rows))
                {
                    rows.Add(Data.Count+1);
                }
                else
                {
                    value.Add(indexValues, new List<int>(Data.Count));
                }
            }
        }

        public int GetOrdinal(string schemaColumnName) => Columns.GetOrdinal(schemaColumnName);
        public int GetOrdinal(TableColumn column, bool groupMustMatch = false) => Columns.GetOrdinal(column, groupMustMatch);
//         public int GetOrdinal(string tableName, string columnGroup) => Columns.GetOrdinal(tableName, columnGroup);

        public bool TryGetColumn(string columnName, out TableColumn column) => Columns.TryGetColumn(columnName, out column);

        public bool TryGetColumn(TableColumn inColumn, out TableColumn column) => Columns.TryGetColumn(inColumn, out column);
        public bool TryGetColumn(TableColumn.EDeltaType deltaType, out TableColumn column) => Columns.TryGetColumn(deltaType, out column);
        
        public TableColumn GetColumn(TableColumn.EDeltaType deltaType) => Columns.GetColumn(deltaType);

        public int GetOrdinal(TableColumn.EDeltaType deltaType) => Columns.GetOrdinal(deltaType);

        public IEnumerable<int> GetOrdinals(TableColumn.EDeltaType deltaType) => Columns.GetOrdinals(deltaType);

        public TableColumn GetAutoIncrementColumn() => Columns.GetAutoIncrementColumn();

        public int GetAutoIncrementOrdinal() => Columns.GetAutoIncrementOrdinal();

        public TableColumn[] GetColumns(TableColumn.EDeltaType deltaType) => Columns.GetColumns(deltaType);
        

        //creates a simple select query with all fields and no sorts, filters
        public SelectQuery DefaultSelectQuery(int rows = -1)
        {
            return new SelectQuery
            {
                Columns = Columns.Where(c=>c.DeltaType != TableColumn.EDeltaType.IgnoreField && c.DataType != ETypeCode.Unknown).Select(c => new SelectColumn(c)).ToList(),
                Table = Name,
                Rows = rows
            };
        }

        public string GetCsv()
        {
            var csvData = new StringBuilder();

            var columns = Columns.Select(c => c.Name).ToArray();
            //add column headers
            var columnCount = Columns.Count;
            var s = new string[columnCount];
            for (var j = 0; j < columnCount; j++)
            {
                s[j] = columns[j];
                if (s[j].Contains("\"")) //replace " with ""
                    s[j] = s[j].Replace("\"", "\"\"");
                if (s[j].Contains("\"") || s[j].Contains(" ")) //add "'s around any string with space or "
                    s[j] = "\"" + s[j] + "\"";
            }
            csvData.AppendLine(string.Join(",", s));

            //add rows
            foreach (var row in Data)
            {
                for (var j = 0; j < columnCount; j++)
                {
                    s[j] = row[j] == null ? "" : row[j].ToString();
                    if (s[j].Contains("\"")) //replace " with ""
                        s[j] = s[j].Replace("\"", "\"\"");
                    if (s[j].Contains("\"") || s[j].Contains(" ")) //add "'s around any string with space or "
                        s[j] = "\"" + s[j] + "\"";
                }
                csvData.AppendLine(string.Join(",", s));
            }

            return csvData.ToString();
        }

    }
}
