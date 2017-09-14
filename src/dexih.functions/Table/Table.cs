using dexih.functions.Query;
using Dexih.Utils.DataType;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static dexih.functions.TableColumn;
using static Dexih.Utils.DataType.DataType;

namespace dexih.functions
{
    
    public class Table 
    {

        #region Initializers
        public Table()
        {
            Data = new TableCache(0);
            Columns = new TableColumns();
        }

        public Table(string tableName, TableColumns columns, TableCache data) 
        {
            Name = tableName;
            BaseTableName = DataType.CleanString(tableName);
            Columns = columns;
            Data = data;
        }

        public Table(string tableName, int maxRows, params TableColumn[] columns) 
        {
            Name = tableName;
            BaseTableName = DataType.CleanString(tableName);
            Columns = new TableColumns();
            foreach (var column in columns)
                Columns.Add(column);

            Data = new TableCache(maxRows);
        }

        public Table(string tableName, int maxRows, TableColumns columns)
        {
            Name = tableName;
            BaseTableName = DataType.CleanString(tableName);
            Columns = columns;
            Data = new TableCache(maxRows);
        }

		public Table(string tableName, int maxRows = 0)
		{
		    Name = tableName;
			BaseTableName = DataType.CleanString(tableName);
			Columns = new TableColumns();
			Data = new TableCache(maxRows);
		}

        public Table(string tableName, string schema, int maxRows = 0) 
        {
            Name = tableName;
			Schema = schema;
            BaseTableName = DataType.CleanString(tableName);
            Columns = new TableColumns();
            Data = new TableCache(maxRows);
        }

        #endregion

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

        /// <summary>
        /// Indicates the key that should be used when running update/delete operations against the target.
        /// </summary>
        public List<string> KeyFields { get; set; }

        public TableCache Data { get; set; }

        public TableColumns Columns { get; protected set; }

		public string ContinuationToken { get; set; }

		// public Dictionary<string, string> ExtendedProperties { get; set; }

        public TableColumn this[string columnName]
        {
            get
            {
                return Columns[columnName];
            }
         }

        //public string GetExtendedProperty(string name)
        //{
        //    if (ExtendedProperties == null)
        //        return null;
        //    else if (ExtendedProperties.ContainsKey(name))
        //        return ExtendedProperties[name];
        //    else
        //        return null;
        //}

        //public void SetExtendedProperty(string name, string value)
        //{
        //    if (ExtendedProperties == null)
        //        ExtendedProperties = new Dictionary<string, string>();

        //    if (ExtendedProperties.ContainsKey(name))
        //        ExtendedProperties[name] = value;
        //    else
        //        ExtendedProperties.Add(name, value);
        //}


        #endregion

        #region Lookup

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
                //scan the data for a matching row.  
                //TODO add indexing to lookup process.
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
                var rows = new List<object[]>();

                //scan the data for a matching row.  
                //TODO add indexing to lookup process.
                for (var i = startRow; i < Data.Count(); i++)
                {
                    if (RowMatch(filters, Data[i]))
                        rows.Add(Data[i]);
                }

                return rows;
            }
            catch (Exception ex)
            {
                throw new TableException("The lookup multiple rows failed.  " + ex.Message, ex);
            }
        }



        public bool RowMatch(List<Filter> filters, object[] row)
        {
            var isMatch = true;

            foreach (var filter in filters)
            {
                object value1;
                object value2;

                if (filter.Value1 != null)
                    value1 = filter.Value1;
                else
                {
                    value1 = row[GetOrdinal(filter.Column1.Name)];
                }

                if (filter.Value2 != null)
                    value2 = filter.Value2;
                else
                {
                    value2 = row[GetOrdinal(filter.Column2.Name)];
                }

                var compareResult = DataType.Compare(filter.CompareDataType, value1, value2);

                switch (filter.Operator)
                {
                    case Filter.ECompare.IsEqual:
                        isMatch = compareResult == DataType.ECompareResult.Equal;
                        break;
                    case Filter.ECompare.NotEqual:
                        isMatch = compareResult != DataType.ECompareResult.Equal;
                        break;
                    case Filter.ECompare.LessThan:
                        isMatch = compareResult == DataType.ECompareResult.Less;
                        break;
                    case Filter.ECompare.LessThanEqual:
                        isMatch = compareResult == DataType.ECompareResult.Less || compareResult == DataType.ECompareResult.Equal;
                        break;
                    case Filter.ECompare.GreaterThan:
                        isMatch = compareResult == DataType.ECompareResult.Greater;
                        break;
                    case Filter.ECompare.GreaterThanEqual:
                        isMatch = compareResult == DataType.ECompareResult.Greater || compareResult == DataType.ECompareResult.Greater;
                        break;
                }

                if (!isMatch)
                    break;
            }

            return isMatch;
        }
        #endregion


        /// <summary>
        /// Adds the standard set of audit columns to the table.  
        /// </summary>
        public void AddAuditColumns()
        {

            //add the audit columns if they don't exist
            //get some of the key fields to save looking up for each row.
            var colValidFrom = GetDeltaColumn(TableColumn.EDeltaType.ValidFromDate);
            var colValidTo = GetDeltaColumn(TableColumn.EDeltaType.ValidToDate);
            var colCreateDate = GetDeltaColumn(TableColumn.EDeltaType.CreateDate);
            var colUpdateDate = GetDeltaColumn(TableColumn.EDeltaType.UpdateDate);
            var colSurrogateKey = GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);
            var colIsCurrentField = GetDeltaColumn(TableColumn.EDeltaType.IsCurrentField);
            var colSourceSurrogateKey = GetDeltaColumn(TableColumn.EDeltaType.SourceSurrogateKey);
            var colCreateAuditKey = GetDeltaColumn(TableColumn.EDeltaType.CreateAuditKey);
            var colUpdateAuditKey = GetDeltaColumn(TableColumn.EDeltaType.UpdateAuditKey);
            var colRejectedReason = GetDeltaColumn(TableColumn.EDeltaType.RejectedReason);

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
                colSurrogateKey = new TableColumn("SurrogateKey", ETypeCode.Int64) { DeltaType = TableColumn.EDeltaType.SurrogateKey };
                Columns.Add(colSurrogateKey);
            }
            if (colIsCurrentField == null)
            {
                colIsCurrentField = new TableColumn("IsCurrent", ETypeCode.Boolean) { DeltaType = TableColumn.EDeltaType.IsCurrentField };
                Columns.Add(colIsCurrentField);
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

            table.Name = rejectedTableName;
            table.Description = "Rejected table for: " + Description;

            if(GetDeltaColumn(TableColumn.EDeltaType.RejectedReason) == null)
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
                    column.Datatype = ETypeCode.String;
                    column.MaxLength = 250;
                }
            }

            return table;
        }

        /// <summary>
        /// Creates a copy of the table, excluding cached data, and sort columns
        /// </summary>
        /// <returns></returns>
        public Table Copy(bool removeSchema = false)
        {
            var table = new Table(Name, Schema)
            {
                Description = Description
            };

            //if (ExtendedProperties != null)
            //{
            //    foreach (var key in ExtendedProperties.Keys)
            //        table.SetExtendedProperty(key, ExtendedProperties[key]);
            //}

            table.LogicalName = LogicalName;

            foreach (var column in Columns)
            {
                var newCol = column.Copy();
                if (removeSchema) newCol.Schema = null;

                table.Columns.Add(newCol);
            }

            return table;
        }

        public void AddColumn(string columnName, ETypeCode dataType = ETypeCode.String)
        {
            if (Columns == null)
                Columns = new TableColumns();

            Columns.Add(new TableColumn(columnName, dataType, Name));
        }

        public void AddColumn(string columnName, ETypeCode dataType = ETypeCode.String, TableColumn.EDeltaType deltaType = TableColumn.EDeltaType.TrackingField)
        {
            if (Columns == null)
                Columns = new TableColumns();

            Columns.Add(new TableColumn(columnName, dataType, deltaType, Name));
        }

        public void AddRow(params object[] values)
        {
            if (values.Length != Columns.Count())
                throw new Exception("The number of parameters must match the number of columns (" + Columns.Count + ") precicely.");

            var row = new object[Columns.Count];
            values.CopyTo(row, 0);

            Data.Add(values);
        }

        public int GetOrdinal(string schemaColumnName)
        {
            return Columns.GetOrdinal(schemaColumnName);
        }

        public TableColumn GetDeltaColumn(TableColumn.EDeltaType deltaType)
        {
            return Columns.SingleOrDefault(c => c.DeltaType == deltaType);
        }

        public int GetDeltaColumnOrdinal(TableColumn.EDeltaType deltaType)
        {
            for (var i = 0; i < Columns.Count; i++)
                if (Columns[i].DeltaType == deltaType)
                    return i;

            return -1;
        }

        public TableColumn[] GetColumnsByDeltaType(TableColumn.EDeltaType deltaType)
        {
            var columns = (from s in Columns where s.DeltaType == deltaType select s).ToArray();
            return columns;
        }
        
        public TableColumn GetIncrementalUpdateColumn()
        {
            return Columns.SingleOrDefault(c => c.IsIncrementalUpdate);
        }

        //creates a simple select query with all fields and no sorts, filters
        public SelectQuery DefaultSelectQuery(int rows = -1)
        {
            return new SelectQuery
            {
                Columns = Columns.Where(c=>c.DeltaType != TableColumn.EDeltaType.IgnoreField && c.Datatype != ETypeCode.Unknown).Select(c => new SelectColumn(c, SelectColumn.EAggregate.None)).ToList(),
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
                    s[j].Replace("\"", "\"\"");
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
                        s[j].Replace("\"", "\"\"");
                    if (s[j].Contains("\"") || s[j].Contains(" ")) //add "'s around any string with space or "
                        s[j] = "\"" + s[j] + "\"";
                }
                csvData.AppendLine(string.Join(",", s));
            }

            return csvData.ToString();
        }


    }
}
