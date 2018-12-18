using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using Newtonsoft.Json.Linq;

namespace dexih.functions.File
{
    public class FileHandlerJson : FileHandlerBase
    {
        private readonly string _rowPath;
        private readonly Table _table;
        private readonly int _fieldCount;
        private IEnumerator<JToken> _jEnumerator;
        private readonly int _responseDataOrdinal;
        private readonly Dictionary<string, (int Ordinal, TableColumn column)> _responseSegmentOrdinals;

        public FileHandlerJson(Table table, string rowPath)
        {
            _rowPath = rowPath;
            _table = table;
            _fieldCount = table.Columns.Count;
            _responseDataOrdinal = _table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseData);
            _responseSegmentOrdinals = new Dictionary<string, (int ordinal, TableColumn column)>();
            
            foreach (var column in _table.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.ResponseSegment))
            {
                _responseSegmentOrdinals.Add(column.Name, (_table.GetOrdinal(column.Name), column));
            }
        }
        
        public override async Task<ICollection<TableColumn>> GetSourceColumns(Stream stream)
        {
            var restFunction = (WebService) _table;
            
            var reader = new StreamReader(stream);
            var jsonString = await reader.ReadToEndAsync();
            JToken content;
            try
            {
                content = JToken.Parse(jsonString);
            }
            catch (Exception ex)
            {
                throw new FileHandlerException($"Failed to parse the response json value. {ex.Message}", ex, stream);
            }

            var columns = new List<TableColumn>();

            if (content != null)
            {
                IEnumerable<JToken> tokens;
                if (string.IsNullOrEmpty(_rowPath))
                {
                    if (content.Type == JTokenType.Array)
                    {
                        tokens = content.First().Children();
                    }
                    else
                    {
                        tokens = content.Children();
                    }
                }
                else
                {
                    tokens = content.SelectTokens(_rowPath).First().Children();
                }

                if (restFunction.MaxImportLevels > 0)
                {
                    foreach (var child in tokens)
                    {
                        columns.Add(GetColumn(child, 0, restFunction.MaxImportLevels));
                    }
                    
                }
            }
            return columns;
        }

        private TableColumn GetColumn(JToken jToken, int currentLevel, int maxLevels)
        {
            if (jToken.Type == JTokenType.Property)
            {
                var value = (JProperty) jToken;

                if (value.Value.Type == JTokenType.Property || value.Value.Type == JTokenType.Object || value.Value.Type == JTokenType.Array)
                {
                    var col = new TableColumn
                    {
                        Name = value.Name,
                        IsInput = false,
                        LogicalName = value.Name,
                        DataType = DataType.ETypeCode.Json,
                        DeltaType = TableColumn.EDeltaType.ResponseSegment,
                        MaxLength = null,
                        Description = "Json value of the " + value.Path + " path",
                        AllowDbNull = true,
                        IsUnique = false
                    };

                    if (currentLevel < maxLevels)
                    {
                        var children = value.Value.Children();

                        if (children.Any())
                        {
                            if (value.Value.Type == JTokenType.Array)
                            {
                                col.DataType = DataType.ETypeCode.Array;
                                children = children.First().Children();
                            }
                            else
                            {
                                col.DataType = DataType.ETypeCode.Property;
                            }

                            var columns = new List<TableColumn>();
                            foreach (var child in children)
                            {
                                columns.Add(GetColumn(child, currentLevel + 1, maxLevels));
                            }
                            
                            col.ChildColumns = new TableColumns(columns);
                        }
                    }

                    return col;
                }
                else
                {
                    DataType.ETypeCode dataType = DataType.GetTypeCode(value.Value.Type);

                    var path = value.Path;
                    if (!string.IsNullOrEmpty(_rowPath) && path.StartsWith(_rowPath))
                    {
                        path = path.Substring(_rowPath.Length);
                    }

                    var col = new TableColumn
                    {
                        Name = value.Name,
                        IsInput = false,
                        LogicalName = value.Name,
                        DataType = dataType,
                        DeltaType = TableColumn.EDeltaType.ResponseSegment,
                        MaxLength = null,
                        Description = "Json value of the " + value.Path + " path",
                        AllowDbNull = true,
                        IsUnique = false
                    };
                    return col;
                }
            }
            else
            {
                var col = new TableColumn
                {
                    Name = jToken.Path,
                    IsInput = false,
                    LogicalName = jToken.Path,
                    DataType = DataType.ETypeCode.Json,
                    DeltaType = TableColumn.EDeltaType.ResponseSegment,
                    MaxLength = null,
                    Description = "Json from the " + jToken.Path + " path",
                    AllowDbNull = true,
                    IsUnique = false
                };
                return col;
            }

            return null;
        }

        public override async Task SetStream(Stream stream, SelectQuery selectQuery)
        {
            var reader = new StreamReader(stream);
            var jsonString = await reader.ReadToEndAsync();
            
            JToken jToken;

            try
            {
                jToken = JToken.Parse(jsonString);
                if (jToken == null)
                {
                    throw new FileHandlerException("The json data parsing returned nothing.");
                }
            }
            catch (Exception ex)
            {
                throw new FileHandlerException($"The json data could not be parsed.  {ex.Message}", ex);
            }
            
            if (string.IsNullOrEmpty(_rowPath))
            {
                if (jToken.Type == JTokenType.Array)
                {
                    _jEnumerator = jToken.Children().GetEnumerator();
                }
                else
                {
                    _jEnumerator = (new List<JToken>() {jToken}).GetEnumerator();
                }
            }
            else
            {
                _jEnumerator = jToken.SelectTokens(_rowPath).GetEnumerator();
            }
        }

        public override Task<object[]> GetRow()
        {
            if (_jEnumerator != null && _jEnumerator.MoveNext())
            {
                var row = new object[_fieldCount];

                if (_responseDataOrdinal >= 0)
                {
                    row[_responseDataOrdinal] = _jEnumerator.Current.ToString();
                }

                foreach (var column in _responseSegmentOrdinals)
                {
                    var value = _jEnumerator.Current.SelectToken(column.Key);
                        
                    try
                    {
                        var col = column.Value.column;
                        if (col.ChildColumns?.Count > 0 && col.DataType == DataType.ETypeCode.Property)
                        {
                            row[column.Value.Ordinal] = GetChildRow(value, col);
                        }
                        else
                        {
                            row[column.Value.Ordinal] = Operations.Parse(col.DataType, value);    
                        }
                        
                    }
                    catch (Exception ex)
                    {
                        throw new FileHandlerException(
                            $"Failed to convert value on column {column.Key} to datatype {column.Value.column?.DataType}. {ex.Message}",
                            ex, value);
                    }
                }

                return Task.FromResult(row);
            }
            else
            {
                return Task.FromResult((object[])null);
            }

        }

        /// <summary>
        /// Processes any child columns within a property column.
        /// </summary>
        /// <param name="jToken"></param>
        /// <param name="column"></param>
        /// <returns></returns>
        private object[] GetChildRow(JToken jToken, TableColumn column)
        {
            var childRow = new object[column.ChildColumns.Count];

            for (var i = 0; i < column.ChildColumns.Count; i++)
            {
                var childColumn = column.ChildColumns[i];
                var value = jToken.SelectToken(childColumn.Name);
                if (value != null)
                {
                    if (childColumn.ChildColumns?.Count > 0 && childColumn.DataType == DataType.ETypeCode.Property)
                    {
                        childRow = GetChildRow(value, childColumn);
                    }
                    else
                    {
                        childRow[i] = Operations.Parse(childColumn.DataType, value);    
                    }
                }
            }

            return childRow;
        }

    }
}