using System.Collections.Generic;
using System.Linq;
using dexih.transforms;
using System;
using dexih.functions;

namespace dexih.transforms
{
    
    public class Table 
    {

        #region Initializers
        public Table()
        {
            Data = new TableCache(0);
            Columns = new List<TableColumn>();
            ExtendedProperties = new Dictionary<string, object>();
        }

        public Table(string tableName, List<TableColumn> columns, TableCache data) 
        {
            TableName = tableName;
            Columns = columns;
            Data = data;
            ExtendedProperties = new Dictionary<string, object>();
        }

        public Table(string tableName, List<TableColumn> columns, int maxRows = 0) 
        {
            TableName = tableName;
            Columns = columns;
            Data = new TableCache(maxRows);
            ExtendedProperties = new Dictionary<string, object>();
        }

        public Table(string tableName, int maxRows = 0) 
        {
            TableName = tableName;
            Columns = new List<TableColumn>();
            Data = new TableCache(maxRows);
            ExtendedProperties = new Dictionary<string, object>();
        }

        #endregion


        #region Properties
        /// <summary>
        /// Reference to the phsycal table name.
        /// </summary>
        public string TableName { get; set; }
        /// <summary>
        /// A logical name for the table.
        /// </summary>
        public string LogicalName { get; set; }

        /// <summary>
        /// Table description.
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Indicates the output sort fields for the table.
        /// </summary>
        /// <returns></returns>
        public List<Sort> OutputSortFields { get; set; }

        public TableCache Data { get; set; }

        public List<TableColumn> Columns { get; set; }

        public Dictionary<string, object> ExtendedProperties { get; set; }

        public TableColumn this[string columnName]
        {
            get
            {
                return Columns.SingleOrDefault(c => c.ColumnName == columnName);
            }
         }
        #endregion

        /// <summary>
        /// Creates a copy of the table, excluding cached data, and sort columns
        /// </summary>
        /// <returns></returns>
        public Table Copy()
        {
            Table table = new Table(TableName);
            table.Description = Description;

            foreach(var key in ExtendedProperties.Keys)
                table.ExtendedProperties.Add(key, ExtendedProperties[key]);

            table.LogicalName = LogicalName;

            foreach (var column in Columns)
                table.Columns.Add(column.Copy());

            return table;
        }

        public string GetSqlCompare(Filter.ECompare compare)
        {
            switch (compare)
            {
                case Filter.ECompare.EqualTo: return "=";
                case Filter.ECompare.GreaterThan: return ">";
                case Filter.ECompare.GreaterThanEqual: return ">=";
                case Filter.ECompare.LessThan: return "<";
                case Filter.ECompare.LessThanEqual: return "<=";
                case Filter.ECompare.NotEqual: return "!=";
                default:
                    return "";
            }
        }

        /// <summary>
        /// Performs a row scan lookup on the data contained in the table.
        /// </summary>
        /// <param name="filters">Filter for the lookup.  For an index to be used, the filters must be in the same column order as the index.</param>
        /// <returns></returns>
        public ReturnValue<object[]> LookupRow(List<Filter> filters, int startRow = 0)
        {
            try
            {
                //scan the data for a matching row.  
                //TODO add indexing to lookup process.
                for (int i = startRow; i < Data.Count(); i++)
                {
                    if (RowMatch(filters, Data[i]))
                        return new ReturnValue<object[]>(true, Data[i]);
                }

                return new ReturnValue<object[]>(false, "Record not found.", null);
            }
            catch (Exception ex)
            {
                return new ReturnValue<object[]>(false, "Error in lookup: " + ex.Message, ex);
            }
        }

        public bool RowMatch(List<Filter> filters, object[] row)
        {
            bool isMatch = true;

            foreach (var filter in filters)
            {
                object value1;
                object value2;

                if (filter.Value1 != null)
                    value1 = filter.Value1;
                else
                {
                    value1 = row[GetOrdinal(filter.Column1)];
                }

                if (filter.Value2 != null)
                    value2 = filter.Value2;
                else
                {
                    value2 = row[GetOrdinal(filter.Column2)];
                }

                bool isEqual = object.Equals(value1, value2);

                switch (filter.Operator)
                {
                    case Filter.ECompare.EqualTo:
                        isMatch = isEqual;
                        break;
                    case Filter.ECompare.NotEqual:
                        isMatch = !isEqual;
                        break;
                    default:
                        if ((filter.Operator == Filter.ECompare.GreaterThanEqual || filter.Operator == Filter.ECompare.LessThanEqual) && isMatch == true)
                            break;

                        bool greater = false;

                        if (value1 is byte)
                            greater = (byte)value1 > (byte)value2;
                        if (value1 is SByte)
                            greater = (SByte)value1 > (SByte)value2;
                        if (value1 is UInt16)
                            greater = (UInt16)value1 > (UInt16)value2;
                        if (value1 is UInt32)
                            greater = (UInt32)value1 > (UInt32)value2;
                        if (value1 is UInt64)
                            greater = (UInt64)value1 > (UInt64)value2;
                        if (value1 is Int16)
                            greater = (Int16)value1 > (Int16)value2;
                        if (value1 is Int32)
                            greater = (Int32)value1 > (Int32)value2;
                        if (value1 is Int64)
                            greater = (Int64)value1 > (Int64)value2;
                        if (value1 is Decimal)
                            greater = (Decimal)value1 > (Decimal)value2;
                        if (value1 is Double)
                            greater = (Double)value1 > (Double)value2;
                        if (value1 is String)
                            greater = String.Compare((String)value1, (String)value2) > 0;
                        if (value1 is Boolean)
                            greater = (Boolean)value1 == false && (Boolean)value2 == true;
                        if (value1 is DateTime)
                            greater = (DateTime)value1 > (DateTime)value2;

                        if ((filter.Operator == Filter.ECompare.GreaterThan || filter.Operator == Filter.ECompare.GreaterThanEqual) && greater)
                            break;

                        if (filter.Operator == Filter.ECompare.LessThan && !isEqual && !greater)
                            break;

                        if (filter.Operator == Filter.ECompare.LessThanEqual && !greater)
                            break;

                        isMatch = false;
                        break;
                }

                if (!isMatch)
                    break;
            }

            return isMatch;
        }

        public int GetOrdinal(string ColumnName)
        {
            for (int i = 0; i < Columns.Count; i++)
            {
                if (Columns[i].ColumnName == ColumnName)
                    return i;
            }

            return -1;
        }

        public void AddColumn(string columnName, DataType.ETypeCode dataType = DataType.ETypeCode.String)
        {
            if (Columns == null)
                Columns = new List<TableColumn>();

            Columns.Add(new TableColumn(columnName, dataType));
        }

        public TableColumn GetDeltaColumn(TableColumn.EDeltaType deltaType)
        {
            return Columns.SingleOrDefault(c => c.DeltaType == deltaType);
        }

        public string[] GetColumnsByDeltaType(TableColumn.EDeltaType deltaType)
        {
            string[] columns = (from s in Columns where s.DeltaType == deltaType select s.ColumnName).ToArray();
            return columns;
        }
        
        public TableColumn GetIncrementalUpdateColumn()
        {
            return Columns.SingleOrDefault(c => c.IsIncrementalUpdate);
        }
    }
}
