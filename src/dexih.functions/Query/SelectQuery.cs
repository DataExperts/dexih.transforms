using System;
using System.Collections.Generic;
using System.Linq;
using dexih.repository;
using Dexih.Utils.DataType;
using Newtonsoft.Json.Linq;
using ProtoBuf;

namespace dexih.functions.Query
{
    [ProtoContract]
    public class SelectQuery: IEquatable<SelectQuery>
    {
        public SelectQuery()
        {
            Columns = new List<SelectColumn>();
            Filters = new List<Filter>();
            Sorts = new List<Sort>();
            Groups = new List<TableColumn>();
            Rows = -1; //-1 means show all rows.
        }

        [ProtoMember(1)]
        public List<SelectColumn> Columns { get; set; }

        [ProtoMember(2)]
        public string Table { get; set; }

        [ProtoMember(3)]
        public List<Filter> Filters { get; set; }

        [ProtoMember(4)]
        public List<Sort> Sorts { get; set; }

        [ProtoMember(5)]
        public List<TableColumn> Groups { get; set; }

        [ProtoMember(6)]
        public int Rows { get; set; }

        [ProtoMember(7)]
        public List<TableColumn> InputColumns { get; set; }

        /// <summary>
        /// Used for flat files to specify only a specific filename
        /// </summary>
        [ProtoMember(8)]
        public string FileName { get; set; }

        [ProtoMember(9)]
        public EFlatFilePath Path { get; set; }

        /// <summary>
        /// Tests is a row should be filtered based on the filters provided.  
        /// </summary>
        /// <param name="row"></param>
        /// <param name="filters"></param>
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
            
            return CompareSequences<SelectColumn>(Columns, other.Columns) && 
                   string.Equals(Table, other.Table) && 
                   CompareSequences<Filter>(Filters, other.Filters) && 
                   CompareSequences<Sort>(Sorts, other.Sorts) && 
                   CompareSequences<TableColumn>(Groups, other.Groups) && 
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

        public void LoadJsonFilters(Table table, JObject jObject)
        {
            if (jObject == null) return;
            
            foreach (var item in jObject)
            {
                var columnName = item.Key;
                var column = table.Columns[columnName];
                if (column == null)
                {
                    throw new Exception($"The column \"{columnName}\" could not be found.");
                }

                if (!item.Value.HasValues)
                {
                    var value = item.Value;
                    Filters.Add(new Filter(column, ECompare.IsEqual, value.ToString()) );
                }
                else
                {
                    var childValues = item.Value.Children();
                    if (childValues.Any())
                    {
                        foreach (var childValue in childValues)
                        {
                            if (childValue is JProperty property)
                            {
                                ECompare op;
                                object value = property.Value;

                                switch (property.Name)
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
                                        if (value is JArray jArray)
                                        {
                                            value = jArray.ToArray();
                                        }
                                        break;
                                    default:
                                        throw new Exception(
                                            $"The operator \"{childValues[0].ToString()} is not recognized.");
                                }

                                Filters.Add(new Filter(column, op, value));
                            }

                        }
                    }
                }
            }
        }

        public void LoadJsonInputColumns(JObject jObject)
        {
            if (jObject == null) return;
            
            foreach (var item in jObject)
            {

                if (!item.Value.HasValues)
                {
                    var columnName = item.Key;
                    var column = new TableColumn(columnName) { IsInput = true, DefaultValue = item.Value };
                    if(InputColumns == null) InputColumns = new List<TableColumn>();
                    InputColumns.Add(column);
                }
                else
                {
                    throw new Exception("The input parameter must only contain single values (i.e. i={\"InputColumn\": \"value\"}");
                }
            }
        }
        
        public void LoadJsonParameters(JObject jObject)
        {
            if (jObject == null) return;
            
            var inputParameters = new InputParameters();
            
            foreach (var item in jObject)
            {
                if (!item.Value.HasValues)
                {
                    var name = item.Key;
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