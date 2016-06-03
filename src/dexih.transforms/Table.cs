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

        }

        public Table(string tableName, List<TableColumn> columns, List<object[]> data)
        {
            TableName = tableName;
            Columns = columns;
            Data = data;
        }

        public Table(string tableName, List<TableColumn> columns)
        {
            TableName = tableName;
            Columns = columns;
            Data = new List<object[]>();
        }

        public Table(string tableName)
        {
            TableName = tableName;
            Columns = new List<TableColumn>();
            Data = new List<object[]>();
        }

        #endregion


        #region Properties
        public string TableName { get; set; }
        public string LogicalName { get; set; }
        public string Description { get; set; }
        public bool IsSorted { get; set; }
        public List<object[]> Data { get; set; }
        public List<TableColumn> Columns { get; set; }

        public TableColumn this[string columnName]
        {
            get
            {
                return Columns.SingleOrDefault(c => c.ColumnName == columnName);
            }
         }
        #endregion

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
        /// <param name="filters"></param>
        /// <returns></returns>
        public ReturnValue<object[]> LookupRow(List<Filter> filters, int startRow = 0)
        {
            try
            {
                //scan the data for a matching row.  
                //TODO add indexing to lookup process.
                for (int i = startRow; i < Data.Count(); i++)
                {
                    object[] row = Data[i];

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

                    if (isMatch)
                        return new ReturnValue<object[]>(true, row);
                }

                return new ReturnValue<object[]>(false, "Record not found.", null);
            }
            catch (Exception ex)
            {
                return new ReturnValue<object[]>(false, "Error in lookup: " + ex.Message, ex);
            }
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
