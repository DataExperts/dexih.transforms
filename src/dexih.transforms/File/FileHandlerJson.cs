using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using Newtonsoft.Json.Linq;


namespace dexih.transforms.File
{
    public class FileHandlerJson : FileHandlerBase
    {
        private readonly string _rowPath;
        private readonly Table _table;
        private readonly int _fieldCount;
        private IEnumerator<JToken> _jEnumerator;
        private readonly int _responseDataOrdinal;
        private readonly Dictionary<string, (int Ordinal, TableColumn column)> _responseSegmentOrdinals;

        private readonly int _fileNameOrdinal;
        private readonly int _fileDateOrdinal;
        
        public FileHandlerJson(Table table, string rowPath)
        {
            _rowPath = rowPath;
            _table = table;
            _fieldCount = table.Columns.Count;
            _responseDataOrdinal = _table.GetOrdinal(EDeltaType.ResponseData);
            _responseSegmentOrdinals = new Dictionary<string, (int ordinal, TableColumn column)>();
            
            foreach (var column in _table.Columns.Where(c => c.DeltaType == EDeltaType.ResponseSegment))
            {
                _responseSegmentOrdinals.Add(column.TableColumnName(), (_table.GetOrdinal(column), column));
            }
            
            _fileNameOrdinal = table.GetOrdinal(EDeltaType.FileName);
            _fileDateOrdinal = table.GetOrdinal(EDeltaType.FileDate);
            
            InitializeNodeTransforms(_table.Columns);

        }

        public override string FileType { get; } = "Json";

        public override async Task<ICollection<TableColumn>> GetSourceColumns(Stream stream)
        {
            var restFunction = _table;
            
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
                    tokens = content.Type == JTokenType.Array ? content.First().Children() : content.Children();
                }
                else
                {
                    tokens = content.SelectTokens(_rowPath).First().Children();
                }

                if (restFunction.MaxImportLevels > 0)
                {
                    foreach (var child in tokens)
                    {
                        columns.AddRange(GetColumns(child, 1, restFunction.MaxImportLevels, new List<string>()));
                    }
                    
                }
            }
            
            return columns;
        }

        private void InitializeNodeTransforms(ICollection<TableColumn> columns)
        {
            foreach (var column in columns.Where(c => c.DataType == ETypeCode.Node && c.ChildColumns.Count > 0))
            {
                var parentTable = new Table("parent", new TableColumns(columns));
                var childTable = new Table(column.Name, column.ChildColumns);
                var node = new TransformNode()
                {
                    Name = "Internal Node"
                };
                node.SetTable(childTable, parentTable);
                
                InitializeNodeTransforms(column.ChildColumns);
            }
        }

        private IEnumerable<TableColumn> GetColumns(JToken jToken, int currentLevel, int maxLevels, List<string> groups)
        {
            if (jToken.Type == JTokenType.Property)
            {
                var value = (JProperty) jToken;
                
                var col = new TableColumn
                {
                    Name = value.Name,
                    IsInput = false,
                    LogicalName = (groups.Any() ? $"{string.Join(".", groups)}." : "") + value.Name,
                    DataType = ETypeCode.Json,
                    DeltaType = EDeltaType.ResponseSegment,
                    ColumnGroup = string.Join(".", groups),
                    MaxLength = null,
                    Description = "Json value of the " + value.Path + " path",
                    AllowDbNull = true,
                    IsUnique = false
                };

                // if array of single values
                if (value.Value.Type == JTokenType.Array && value.Value.First() is JValue jValue)
                {
                    var dataType = GetTypeCode(jValue.Type);

                    col.DataType = dataType;
                    col.Rank = 1;

                    return new [] { col};
                }
                else if (value.Value.Type == JTokenType.Array)
                {
                    col.Description = "Json arrays values at " + value.Path + " path";
                    
                    if (currentLevel < maxLevels)
                    {
                        col.DataType = ETypeCode.Node;
                        var children = value.Value.Children();

                        if (children.Any())
                        {
                            col.DataType = ETypeCode.Node;
                            children = children.First().Children();

                            var columns = new List<TableColumn>();
                            foreach (var child in children)
                            {
                                columns.AddRange(GetColumns(child, currentLevel + 1, maxLevels, groups));
                            }
                            
                            col.ChildColumns = new TableColumns(columns);
                        }
                    }
                    else
                    {
                        col.DataType = ETypeCode.Json;
                    }

                    return new [] {col};
                }
                else if (value.Value.Type == JTokenType.Property || value.Value.Type == JTokenType.Object)
                {
                    var columns = new List<TableColumn>();

                    if (currentLevel < maxLevels)
                    {
                        var children = value.Value.Children();

                        if (children.Any())
                        {
                            var newGroups = groups.ToList();
                            newGroups.Add(value.Name);

                            foreach (var child in children)
                            {
                                columns.AddRange(GetColumns(child, currentLevel + 1, maxLevels, newGroups));
                            }
                        }
                    }

                    if (columns.Count == 0)
                    {
                        return new[] {col};
                    }

                    return columns;
                }
                else
                {
                    var dataType = GetTypeCode(value.Value.Type);
                    col.DataType = dataType;
                    return new [] {col};
                }
            }
            else
            {
                var col = new TableColumn
                {
                    Name = jToken.Path,
                    IsInput = false,
                    LogicalName = jToken.Path,
                    DataType = ETypeCode.Json,
                    DeltaType = EDeltaType.ResponseSegment,
                    MaxLength = null,
                    Description = "Json from the " + jToken.Path + " path",
                    AllowDbNull = true,
                    IsUnique = false
                };
                return new [] {col};
            }

        }

        /// <summary>
        /// Converts a <see cref="JTokenType"/> into an ETypeCode
        /// </summary>
        /// <param name="jsonType"></param>
        /// <returns></returns>
        private ETypeCode GetTypeCode(JTokenType jsonType)
        {
            switch (jsonType)
            {
                case JTokenType.Object:
                case JTokenType.Array:
                case JTokenType.Constructor:
                case JTokenType.Property:
                    return ETypeCode.Json;
                case JTokenType.None:
                case JTokenType.Comment:
                case JTokenType.Null:
                case JTokenType.Undefined:
                case JTokenType.Raw:
                case JTokenType.Uri:
                case JTokenType.String:
                    return ETypeCode.String;
                case JTokenType.Integer:
                    return ETypeCode.Int32;
                case JTokenType.Float:
                    return ETypeCode.Double;
                case JTokenType.Boolean:
                    return ETypeCode.Boolean;
                case JTokenType.Date:
                    return ETypeCode.DateTime;
                case JTokenType.Bytes:
                    return ETypeCode.Binary;
                case JTokenType.Guid:
                    return ETypeCode.Guid;
                case JTokenType.TimeSpan:
                    return ETypeCode.Time;
                default:
                    return ETypeCode.String;
            }

        }

        private object GetJTokenValue(ETypeCode typeCode, int rank, JToken jToken)
        {
            if (rank == 0)
            {
                switch (typeCode)
                {
                    case ETypeCode.Binary:
                        return jToken.Value<byte[]>();
                    case ETypeCode.Geometry:
                        return jToken.Value<string>();
                    case ETypeCode.Byte:
                        return jToken.Value<byte>();
                    case ETypeCode.Char:
                        return jToken.Value<char>();
                    case ETypeCode.SByte:
                        return jToken.Value<sbyte>();
                    case ETypeCode.UInt16:
                        return jToken.Value<ushort>();
                    case ETypeCode.UInt32:
                        return jToken.Value<uint>();
                    case ETypeCode.UInt64:
                        return jToken.Value<ulong>();
                    case ETypeCode.Int16:
                        return jToken.Value<short>();
                    case ETypeCode.Int32:
                        return jToken.Value<int>();
                    case ETypeCode.Int64:
                        return jToken.Value<long>();
                    case ETypeCode.Decimal:
                        return jToken.Value<decimal>();
                    case ETypeCode.Double:
                        return jToken.Value<double>();
                    case ETypeCode.Single:
                        return jToken.Value<float>();
                    case ETypeCode.String:
                    case ETypeCode.Text:
                        return jToken.Value<string>();
                    case ETypeCode.Boolean:
                        return jToken.Value<bool>();
                    case ETypeCode.DateTime:
                    case ETypeCode.Date:
                        return jToken.Value<DateTime>();
                    case ETypeCode.Time:
                        return jToken.Value<TimeSpan>();
                    case ETypeCode.Guid:
                        return jToken.Value<Guid>();
                    case ETypeCode.Unknown:
                        return jToken.Value<string>();
                    case ETypeCode.Json:
                        return jToken;
                    case ETypeCode.Xml:
                        return jToken.Value<string>();
                    case ETypeCode.Enum:
                        return jToken.Value<byte>();
                    case ETypeCode.CharArray:
                        return jToken.Value<string>();
                    case ETypeCode.Object:
                        return jToken.Value<string>();
                    default:
                        throw new ArgumentOutOfRangeException(nameof(typeCode), typeCode, null);
                }
            }

            if (jToken is JArray jArray)
            {
                var dataType = DataType.GetType(typeCode);
                if (rank == 1)
                {
                    var returnValue = Array.CreateInstance(dataType, jArray.Count);
                    for (var i = 0; i < jArray.Count; i++)
                    {
                        returnValue.SetValue(GetJTokenValue(typeCode, 0, jArray[i]), i);
                    }

                    return returnValue;
                }
                else if (rank == 2)
                {
                    var array2 = (JArray) jArray.First();
                    var returnValue = Array.CreateInstance(dataType, jArray.Count, array2.Count);

                    for (var i = 0; i < jArray.Count; i++)
                    {
                        array2 = (JArray) jArray[i];
                        for (var j = 0; j < array2.Count; j++)
                        {
                            returnValue.SetValue(GetJTokenValue(typeCode, 0, array2[j]), i, j);
                        }
                    }

                    return returnValue;
                }
            }
            
            throw new ArgumentOutOfRangeException(nameof(typeCode), typeCode, null);
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
                    _jEnumerator = new List<JToken>() {jToken}.GetEnumerator();
                }
            }
            else
            {
                _jEnumerator = jToken.SelectTokens(_rowPath).GetEnumerator();
            }
        }

        public override Task<object[]> GetRow(FileProperties fileProperties)
        {
            if (_jEnumerator == null || !_jEnumerator.MoveNext()) return Task.FromResult((object[]) null);
            
            var row = new object[_fieldCount];

            if (_responseDataOrdinal >= 0)
            {
                row[_responseDataOrdinal] = _jEnumerator.Current.ToString();
            }

            foreach (var column in _responseSegmentOrdinals.Values)
            {
                var path = (column.column.ColumnGroup == null ? "" : column.column.ColumnGroup + ".") +
                           column.column.Name;
                var value = _jEnumerator.Current.SelectToken(path);
                row[column.Ordinal] = GetValue(value, column.column);
            }
            
            foreach (var column in _responseSegmentOrdinals.Values)
            {
                var path = (column.column.ColumnGroup == null ? "" : column.column.ColumnGroup + ".") +
                           column.column.Name;
                var value = _jEnumerator.Current.SelectToken(path);
                row[column.Ordinal] = GetValue(value, column.column);
            }

            if (fileProperties != null)
            {
                if (_fileNameOrdinal >= 0)
                {
                    row[_fileNameOrdinal] = fileProperties.FileName;
                }

                if (_fileDateOrdinal >= 0)
                {
                    row[_fileDateOrdinal] = fileProperties.LastModified;
                }
            }

            return Task.FromResult(row);
        }

        private object GetValue(JToken jToken, TableColumn column)
        {
            try
            {
                if (column.DataType == ETypeCode.Node && column.ChildColumns?.Count > 0)
                {
                    return GetArray(jToken, column);
                }

                if (column.DeltaType != EDeltaType.FileName &&
                    column.DeltaType != EDeltaType.FileRowNumber)
                {
                    return GetJTokenValue(column.DataType, column.Rank, jToken);
                }
                else
                {
                    return null;
                }


            }
            catch (Exception ex)
            {
                throw new FileHandlerException(
                    $"Failed to convert value on column {column.Name} to datatype {column.DataType}. {ex.Message}",
                    ex, jToken);
            }
        }

        private object GetArray(JToken jToken, TableColumn column)
        {
            var data = new TableCache();

            if (jToken is JArray jArray)
            {
                foreach (var row in jArray)
                {
                    var childRow = new object[column.ChildColumns.Count];

                    for (var i = 0; i < column.ChildColumns.Count; i++)
                    {
                        var childColumn = column.ChildColumns[i];
                        var value = row.SelectToken(childColumn.Name);
                        if (value != null)
                        {
                            childRow[i] = GetValue(value, childColumn);
                        }
                    }
                    data.Add(childRow);
                }
            }
            
            var table = new Table(column.Name, column.ChildColumns, data);
            var reader = new ReaderMemory(table);
            return reader;
            
//            var nodeTransform = _nodeTransforms[column.LogicalName];
//            nodeTransform.PrimaryTransform = reader;
//            nodeTransform.Open().Wait();  // this wait is ok as the ReaderMemory/NodeTransform open methods dont' use any "await".
//
//            return nodeTransform;
        }

    }
}