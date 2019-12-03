using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Dexih.Utils.DataType;

using MessagePack;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class SelectQuery: IEquatable<SelectQuery>
    {
        public SelectQuery()
        {
            Columns = new List<SelectColumn>();
            Filters = new List<Filter>();
            Sorts = new Sorts();
            Groups = new List<TableColumn>();
            Rows = -1; //-1 means show all rows.
        }

        [Key(0)]
        public List<SelectColumn> Columns { get; set; }

        [Key(1)]
        public string Table { get; set; }

        [Key(2)]
        public List<Filter> Filters { get; set; }

        [Key(3)]
        public Sorts Sorts { get; set; }

        [Key(4)]
        public List<TableColumn> Groups { get; set; }

        [Key(5)] 
        public int Rows { get; set; }

        [Key(6)]
        public List<TableColumn> InputColumns { get; set; }

        /// <summary>
        /// Used for flat files to specify only a specific filename
        /// </summary>
        [Key(7)]
        public string FileName { get; set; }

        [Key(8)] 
        public EFlatFilePath Path { get; set; } = EFlatFilePath.None;
        
        /// <summary>
        /// Tests is a row should be filtered based on the filters provided.  
        /// </summary>
        /// <param name="row"></param>
        /// <param name="table"></param>
        /// <returns>true = don't filter, false = filtered</returns>
        public bool EvaluateRowFilter(IReadOnlyList<object> row, Table table)
        {
            if (Filters != null && Filters.Count > 0)
            {
                var filterResult = true;
                var isFirst = true;

                foreach (var filter in Filters)
                {
                    var column1Value = filter.Column1 == null
                        ? filter.Value1
                        : row[table.GetOrdinal(filter.Column1.Name)];
                    
                    var column2Value = filter.Column2 == null
                        ? filter.Value2
                        : row[table.GetOrdinal(filter.Column2.Name)];

                    if (isFirst)
                    {
                        filterResult = filter.Evaluate(column1Value, column2Value);
                        isFirst = false;
                    }
                    else if (filter.AndOr == Filter.EAndOr.And)
                    {
                        filterResult = filterResult && filter.Evaluate(column1Value, column2Value);
                    }
                    else
                    {
                        filterResult = filterResult || filter.Evaluate(column1Value, column2Value);
                    }
                }

                return filterResult;
            }
            else
            {
                return true;
            }
        }

        internal bool CompareSequences<T>(List<T> seq1, List<T> seq2)
        {
            if (seq1 == null && seq2 == null) return true;
            if (seq1 == null || seq2 == null) return false;
            return seq1.SequenceEqual(seq2);
        }

        public bool Equals(SelectQuery other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            
            return CompareSequences(Columns, other.Columns) && 
                   string.Equals(Table, other.Table) && 
                   CompareSequences(Filters, other.Filters) && 
                   CompareSequences(Sorts, other.Sorts) && 
                   CompareSequences(Groups, other.Groups) && 
                   Rows == other.Rows && 
                   string.Equals(FileName, other.FileName) && 
                   Path == other.Path;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((SelectQuery) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = 0;
                if (Columns != null && Columns.Count > 0)
                {
                    foreach (var column in Columns)
                    {
                        hashCode = (hashCode * 397) ^ column.GetHashCode();    
                    }
                }

                hashCode = (hashCode * 397) ^ (Table != null ? Table.GetHashCode() : 0);

                if (Filters != null && Filters.Count > 0)
                {
                    foreach (var filter in Filters)
                    {
                        hashCode = (hashCode * 397) ^ filter.GetHashCode();    
                    }
                }
                if (Sorts != null && Sorts.Count > 0)
                {
                    foreach (var sort in Sorts)
                    {
                        hashCode = (hashCode * 397) ^ sort.GetHashCode();    
                    }
                }
                if (Groups != null && Groups.Count > 0)
                {
                    foreach (var group in Groups)
                    {
                        hashCode = (hashCode * 397) ^ group.GetHashCode();    
                    }
                }

                if (InputColumns != null && InputColumns.Count > 0)
                {
                    foreach (var inputColumn in InputColumns)
                    {
                        hashCode = (hashCode * 397) ^ inputColumn.GetHashCode();    
                    }
                }
                
                hashCode = (hashCode * 397) ^ Rows;
                hashCode = (hashCode * 397) ^ (FileName != null ? FileName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (int) Path;

                return hashCode;
            }
        }

        public void LoadJsonFilters(Table table, JsonDocument jsonDocument)
        {
            if (jsonDocument == null) return;
            
            foreach (var item in jsonDocument.RootElement.EnumerateObject())
            {
                var columnName = item.Name;
                var column = table.Columns[columnName];
                if (column == null)
                {
                    throw new Exception($"The column \"{columnName}\" could not be found.");
                }

                if (item.Value.ValueKind != JsonValueKind.Object)
                {
                    var value = item.Value;
                    Filters.Add(new Filter(column, ECompare.IsEqual, value.ToString()) );
                }
                else
                {

                    foreach (var childValue in item.Value.EnumerateObject())
                    {
                        ECompare op;
                        object value = childValue.Value.GetString();

                        switch (childValue.Name)
                        {
                            case "eq":
                            case "=":
                                op = ECompare.IsEqual;
                                break;
                            case "lt":
                            case "<":
                                op = ECompare.LessThan;
                                break;
                            case "le":
                            case "<=":
                                op = ECompare.LessThanEqual;
                                break;
                            case "gt":
                            case ">":
                                op = ECompare.GreaterThan;
                                break;
                            case "ge":
                            case ">=":
                                op = ECompare.GreaterThanEqual;
                                break;
                            case "ne":
                            case "!=":
                            case "<>":
                                op = ECompare.NotEqual;
                                break;
                            case "nl":
                            case "null":
                                op = ECompare.IsNull;
                                break;
                            case "nn":
                            case "notnull":
                                op = ECompare.IsNotNull;
                                break;
                            case "in":
                                op = ECompare.IsIn;
                                if (childValue.Value.ValueKind == JsonValueKind.Array)
                                {
                                    value = childValue.Value.EnumerateArray().Select(c => c.GetString()).ToArray();
                                }

                                break;
                            default:
                                throw new Exception(
                                    $"The operator \"{childValue.Name} is not recognized.");
                        }

                        Filters.Add(new Filter(column, op, value));
                    }
                }
            }
        }

        public void LoadJsonInputColumns(JsonDocument jObject)
        {
            if (jObject == null) return;
            
            foreach (var item in jObject.RootElement.EnumerateObject())
            {

                if (item.Value.ValueKind != JsonValueKind.Array)
                {
                    var columnName = item.Name;
                    var column = new TableColumn(columnName) { IsInput = true, DefaultValue = item.Value.GetString() };
                    if(InputColumns == null) InputColumns = new List<TableColumn>();
                    InputColumns.Add(column);
                }
                else
                {
                    throw new Exception("The input parameter must only contain single values (i.e. i={\"InputColumn\": \"value\"}");
                }
            }
        }
        
        public void LoadJsonParameters(JsonDocument jObject)
        {
            if (jObject == null) return;
            
            var inputParameters = new InputParameters();
            
            foreach (var item in jObject.RootElement.EnumerateObject())
            {
                if (item.Value.ValueKind != JsonValueKind.Array)
                {
                    var name = item.Name;
                    var value = item.Value.ToString();
                    inputParameters.Add(name, value);
                }
                else
                {
                    throw new Exception("The parameter must only contain single values (i.e. p={\"name\": \"value\"}");
                }
            }
            
            AddParameters(inputParameters);
        }
        
        public void AddParameters(InputParameters inputParameters)
        {
            if (inputParameters == null)
            {
                return;
            }
            
            foreach (var filter in Filters)
            {
                filter.Value1 = inputParameters.SetParameters(filter.Value1);
                filter.Value2 = inputParameters.SetParameters(filter.Value2);
            }
        }
    }
}