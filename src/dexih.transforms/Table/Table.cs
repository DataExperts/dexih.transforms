using dexih.functions.Query;
using Dexih.Utils.DataType;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;


namespace dexih.functions
{
    
    [DataContract]
    // [Union(0, typeof(FlatFile))]
    // [Union(1, typeof(WebService))]
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

            var arr = value.Where(c => char.IsLetterOrDigit(c)).ToArray();
            var newValue = new string(arr);
            return newValue;
        }

        #endregion

        // [JsonConverter(typeof(StringEnumConverter))]
        public enum ETableType
        {
            Table = 1,
            View,
            Query
            
        }

        #region Properties

        /// <summary>
        /// Reference to the physical table name.
        /// </summary>
        [DataMember(Order = 0)]
        public string Name { get; set; }

        /// <summary>
        /// The table schema or owner.
        /// </summary>
        /// <value>The table schema.</value>
        [DataMember(Order = 1)]
        public string Schema { get; set; }

        /// <summary>
        /// The name of the source connection when pointing to an another hub
        /// </summary>
        /// <value>The table connection.</value>
        [DataMember(Order = 2)]
        public string SourceConnectionName { get; set; }

        /// <summary>
        /// A logical name for the table.
        /// </summary>
        [DataMember(Order = 3)]
        public string LogicalName { get; set; }

        /// <summary>
        /// Table description.
        /// </summary>
        [DataMember(Order = 4)]
        public string Description { get; set; }

        /// <summary>
        /// Is the original base table name.
        /// </summary>
        [DataMember(Order = 5)]
        public string BaseTableName { get; set; }

        /// <summary>
        /// Indicates the type of table (i.e. table, view etc.)
        /// </summary>
        [DataMember(Order = 6)]
        public ETableType TableType { get; set; } = ETableType.Table;

        /// <summary>
        /// Indicates if the table contains versions (history) of data change, such as sql temporal tables.
        /// </summary>
        [DataMember(Order = 7)]
        public bool IsVersioned { get; set; }

        // /// <summary>
        // /// Indicates if this is a sql (or other) type query.
        // /// </summary>
        // [DataMember(Order = 8)]
        // public bool UseQuery { get; set; }

        /// <summary>
        /// Sql Query string (or query string for other db types)
        /// </summary>
        [DataMember(Order = 8)]
        public string QueryString { get; set; }

        /// <summary>
        /// Indicates the output sort fields for the table.
        /// </summary>
        /// <returns></returns>
        [DataMember(Order = 9)]
        public Sorts OutputSortFields { get; set; }
        
        [IgnoreDataMember]
        public TableCache Data { get; set; }

        [DataMember(Order = 10)]
        public TableColumns Columns { get; set; }

        // public Dictionary<string, string> ExtendedProperties { get; set; }

        public TableColumn this[string columnName] => Columns[columnName];

        public TableColumn this[int ordinal] => Columns[ordinal];

        /// <summary>
        /// Maximum levels to recurse through structured data when importing columns.
        /// </summary>
        [DataMember(Order = 11)]
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
        public object[] LookupSingleRow(IEnumerable<Filter> filters, int startRow = 0)
        {
            try
            {
                // use the index to reduce the scan rows
                var data = IndexLookup(filters) ?? Data;
                
                //scan the data for a matching row. 
                var i = 0;
                foreach(var item in data)
                {
                    if(i++ < startRow) continue;
                    
                    if (RowMatch(filters, item))
                        return item;
                }

                return null;
            }
            catch (Exception ex)
            {
                throw new TableException("The lookup row failed.  " + ex.Message, ex);
            }
        }

        public List<object[]> LookupMultipleRows(IEnumerable<Filter> filters, int startRow = 0)
        {
            try
            {
                List<object[]> rows = null;

                // use the index to reduce the scan rows
                var data = IndexLookup(filters) ?? Data;
                
                //scan the data for a matching row. 
                var i = 0;
                foreach(var item in data)
                {
                    if(i++ < startRow) continue;
                    
                    if (RowMatch(filters, item))
                    {
                        if (rows == null)
                            rows = new List<object[]>();
                        rows.Add(item);
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
            if (_indexes == null) yield break;
            
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
                            foreach (var t in rowIndexes)
                            {
                                yield return Data[t];
                            }
                        }
                    }
                }
            }
        }
        
        #endregion


        /// <summary>
        /// Adds the standard set of audit columns to the table.  
        /// </summary>
        public void AddAuditColumns(string surrogateKeyName = "SurrogateKey")
        {

            //add the audit columns if they don't exist
            //get some of the key fields to save looking up for each row.
            var colValidFrom = GetColumn(EDeltaType.ValidFromDate);
            var colValidTo = GetColumn(EDeltaType.ValidToDate);
            var colCreateDate = GetColumn(EDeltaType.CreateDate);
            var colUpdateDate = GetColumn(EDeltaType.UpdateDate);
            var colSurrogateKey = GetColumn(EDeltaType.AutoIncrement);
            var colIsCurrentField = GetColumn(EDeltaType.IsCurrentField);
            var colVersionField = GetColumn(EDeltaType.Version);
            var colCreateAuditKey = GetColumn(EDeltaType.CreateAuditKey);
            var colUpdateAuditKey = GetColumn(EDeltaType.UpdateAuditKey);
//            var colSourceSurrogateKey = GetDeltaColumn(EDeltaType.SourceSurrogateKey);
//            var colRejectedReason = GetDeltaColumn(EDeltaType.RejectedReason);

            if (colValidFrom == null)
            {
                colValidFrom = new TableColumn("ValidFromDate", ETypeCode.DateTime) { DeltaType = EDeltaType.ValidFromDate };
                Columns.Add(colValidFrom);
            }
            
            if (colValidTo == null)
            {
                colValidTo = new TableColumn("ValidToDate", ETypeCode.DateTime) { DeltaType = EDeltaType.ValidToDate };
                Columns.Add(colValidTo);
            }
            
            if (colCreateDate == null)
            {
                colCreateDate = new TableColumn("CreateDate", ETypeCode.DateTime) { DeltaType = EDeltaType.CreateDate };
                Columns.Add(colCreateDate);
            }
            
            if (colUpdateDate == null)
            {
                colUpdateDate = new TableColumn("UpdateDate", ETypeCode.DateTime) { DeltaType = EDeltaType.UpdateDate };
                Columns.Add(colUpdateDate);
            }
            
            if (colSurrogateKey == null)
            {
                colSurrogateKey = new TableColumn(surrogateKeyName, ETypeCode.Int64) { DeltaType = EDeltaType.AutoIncrement };
                Columns.Add(colSurrogateKey);
            }

            if (colIsCurrentField == null)
            {
                colIsCurrentField = new TableColumn("IsCurrent", ETypeCode.Boolean) { DeltaType = EDeltaType.IsCurrentField };
                Columns.Add(colIsCurrentField);
            }

            if (colVersionField == null)
            {
                colVersionField = new TableColumn("Version", ETypeCode.Int32) { DeltaType = EDeltaType.Version };
                Columns.Add(colVersionField);
            }
            
            if (colCreateAuditKey == null)
            {
                colCreateAuditKey = new TableColumn("CreateAuditKey", ETypeCode.Int64) { DeltaType = EDeltaType.CreateAuditKey };
                Columns.Add(colCreateAuditKey);
            }

            if (colUpdateAuditKey == null)
            {
                colUpdateAuditKey = new TableColumn("UpdateAuditKey", ETypeCode.Int64) { DeltaType = EDeltaType.UpdateAuditKey };
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

            var table = new Table(rejectedTableName)
            {
                Description = "Rejected table for: " + Description
            };

            // reset delta types on reject table to ensure the reject table contains no keys.
            foreach(var column in Columns)
            {
                var newColumn = column.Copy();
                newColumn.DeltaType = EDeltaType.TrackingField;
                table.Columns.Add(newColumn);
            }
            
            if (GetColumn(EDeltaType.RejectedReason) == null)
            {
                table.Columns.Add(new TableColumn("RejectedReason", ETypeCode.String, EDeltaType.RejectedReason));
            }

            if (GetColumn(EDeltaType.CreateAuditKey) == null)
            {
                table.Columns.Add(new TableColumn("AuditKey", ETypeCode.Int64, EDeltaType.CreateAuditKey));
            }


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
                if(column.SecurityFlag != ESecurityFlag.None)
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
                LogicalName = LogicalName,
                TableType = TableType,
                QueryString = QueryString,
                IsVersioned = IsVersioned,
                OutputSortFields = OutputSortFields,
                BaseTableName = BaseTableName,
            };


            foreach (var column in Columns)
            {
                if (!removeIgnoreColumns || column.DeltaType != EDeltaType.IgnoreField)
                {
                    var newCol = column.Copy();
                    if (removeSchema) newCol.ReferenceTable = null;

                    table.Columns.Add(newCol);
                }
            }

            return table;
        }

        public void AddColumn(string columnName, ETypeCode dataType = ETypeCode.String, EDeltaType deltaType = EDeltaType.TrackingField, byte arrayDimensions = 0)
        {
            Columns ??= new TableColumns();
            Columns.Add(new TableColumn(columnName, dataType, deltaType, arrayDimensions, Name));
        }

        public void AddRow(params object[] values)
        {
            if (values.Length != Columns.Count)
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
        public int GetOrdinal(string schemaColumnName, string referenceTable) => Columns.GetOrdinal(schemaColumnName, referenceTable);
        public int GetOrdinal(TableColumn column, bool groupMustMatch = false) => Columns.GetOrdinal(column, groupMustMatch);
//         public int GetOrdinal(string tableName, string columnGroup) => Columns.GetOrdinal(tableName, columnGroup);

        public bool TryGetColumn(string columnName, out TableColumn column) => Columns.TryGetColumn(columnName, out column);

        public bool TryGetColumn(TableColumn inColumn, out TableColumn column) => Columns.TryGetColumn(inColumn, out column);
        public bool TryGetColumn(EDeltaType deltaType, out TableColumn column) => Columns.TryGetColumn(deltaType, out column);
        
        public TableColumn GetColumn(EDeltaType deltaType) => Columns.GetColumn(deltaType);

        public int GetOrdinal(EDeltaType deltaType) => Columns.GetOrdinal(deltaType);

        public IEnumerable<int> GetOrdinals(EDeltaType deltaType) => Columns.GetOrdinals(deltaType);

        public TableColumn GetAutoIncrementColumn() => Columns.GetAutoIncrementColumn();

        public int GetAutoIncrementOrdinal() => Columns.GetAutoIncrementOrdinal();

        public TableColumn[] GetColumns(EDeltaType deltaType) => Columns.GetColumns(deltaType);
        

        //creates a simple select query with all fields and no sorts, filters
        public SelectQuery DefaultSelectQuery(int rows = -1)
        {
            return new SelectQuery
            {
                Columns = new SelectColumns(Columns.Where(c=>c.DeltaType != EDeltaType.IgnoreField && c.DataType != ETypeCode.Unknown).Select(c => new SelectColumn(c))),
                TableName = Name,
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

        /// <summary>
        /// Gets a dbml string for the table
        /// </summary>
        /// <returns></returns>
        public string DBML()
        {
            var dbml = new StringBuilder();
            
            dbml.AppendLine($"Table {Name} {{");

            foreach (var column in Columns)
            {
                dbml.AppendLine("  " + column.DBML());
            }

            var indexes = new List<string>();
            var naturalKey = Columns.Where(c => c.DeltaType == EDeltaType.NaturalKey).Select(c=> c.Name).ToArray();
            if (naturalKey.Length > 0)
            {
                indexes.Add($"({string.Join(", ", naturalKey)}) [name: 'natural_key']");
            }

            var incremental = Columns.Where(c => c.IsAutoIncrement()).Select(c=> c.Name).ToArray();
            if(incremental.Length > 0)
            {
                indexes.Add($"({string.Join(", ", incremental)}) [name: 'pk']");
            }

            if (indexes.Count > 0)
            {
                dbml.AppendLine();
                dbml.AppendLine("  Indexes {");
                foreach (var index in indexes)
                {
                    dbml.AppendLine("    " + index);
                }

                dbml.AppendLine("  }");
            }
            dbml.AppendLine("}");
            return dbml.ToString();
        }
    }
}
