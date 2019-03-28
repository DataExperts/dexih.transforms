using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.XPath;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.transforms.File
{
    public class FileHandlerXml : FileHandlerBase
    {
        private readonly string _rowPath;
        private XPathNodeIterator _xPathNodeIterator;
        private readonly Table _table;
        private readonly int _fieldCount;
        private readonly int _responseDataOrdinal;
        private readonly Dictionary<string, (int Ordinal, DataType.ETypeCode Datatype)> _responseSegementOrdinals;
        
        public FileHandlerXml(Table table, string rowPath)
        {
            _table = table;
            _fieldCount = table.Columns.Count;
            _rowPath = rowPath;
            
            _responseDataOrdinal = _table.GetOrdinal(TableColumn.EDeltaType.ResponseData);

            _responseSegementOrdinals = new Dictionary<string, (int ordinal, DataType.ETypeCode typeCode)>();
            
            foreach (var column in _table.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.ResponseSegment))
            {
                _responseSegementOrdinals.Add(column.Name, (_table.GetOrdinal(column.Name), column.DataType));
            }

        }
        
        public override string FileType { get; } = "Xml";

        public override Task<ICollection<TableColumn>> GetSourceColumns(Stream stream)
        {
            XPathNavigator xPathNavigator;

            try
            {
                var xPathDocument = new XPathDocument(stream);
                xPathNavigator = xPathDocument.CreateNavigator();
            }
            catch (Exception ex)
            {
                throw new FileHandlerException($"Failed to parse the response xml value. {ex.Message}", ex, stream);
            }

            var columns = new List<TableColumn>();

            if (xPathNavigator != null)
            {
                XPathNodeIterator nodes;
                if (string.IsNullOrEmpty(_rowPath))
                {
                    nodes = xPathNavigator.SelectChildren(XPathNodeType.All);
                    if(nodes.Count == 1)
                    {
                        nodes.MoveNext();
                        nodes = nodes.Current.SelectChildren(XPathNodeType.All);
                    }
                }
                else
                {
                    nodes = xPathNavigator.Select(_rowPath);
                    if(nodes.Count < 0)
                    {
                        throw new FileHandlerException($"Failed to find the path {_rowPath} in the xml response.");
                    }

                    nodes.MoveNext();
                    nodes = nodes.Current.SelectChildren(XPathNodeType.All);
                }

                var columnCounts = new Dictionary<string, int>();

               
                while(nodes.MoveNext())
                {
                    var node = nodes.Current;

                    string nodePath;
                    if(columnCounts.ContainsKey(node.Name))
                    {
                        var count = columnCounts[node.Name];
                        count++;
                        columnCounts[node.Name] = count;
                        nodePath = $"{node.Name}[{count}]";
                    }
                    else
                    {
                        columnCounts.Add(node.Name, 1);
                        nodePath = $"{node.Name}[1]";
                    }

                    if (node.SelectChildren(XPathNodeType.All).Count == 1)
                    {
                        var dataType = DataType.GetTypeCode(node.ValueType, out var rank);
                        var col = new TableColumn
                        {
                            Name = nodePath,
                            IsInput = false,
                            LogicalName = node.Name,
                            DataType = dataType,
                            Rank = rank,
                            DeltaType = TableColumn.EDeltaType.ResponseSegment,
                            MaxLength = null,
                            Description = "Value of the " + nodePath + " path",
                            AllowDbNull = true,
                            IsUnique = false
                        };
                        columns.Add(col);
                    }
                    else
                    {
                        var col = new TableColumn
                        {
                            Name = nodePath,
                            IsInput = false,
                            LogicalName = node.Name,
                            DataType = DataType.ETypeCode.Xml,
                            DeltaType = TableColumn.EDeltaType.ResponseSegment,
                            MaxLength = null,
                            Description = "Xml from the " + nodePath + " path",
                            AllowDbNull = true,
                            IsUnique = false
                        };
                        columns.Add(col);
                    }
                }
            }

            return Task.FromResult((ICollection<TableColumn>)columns);
        }

        public override Task SetStream(Stream stream, SelectQuery selectQuery)
        {
            var xPathDocument = new XPathDocument(stream);
            var xPathNavigator = xPathDocument.CreateNavigator();

            if (string.IsNullOrEmpty(_rowPath))
            {
                _xPathNodeIterator = xPathNavigator.SelectChildren(XPathNodeType.All);
            }
            else
            {
                _xPathNodeIterator = xPathNavigator.Select(_rowPath);
            }

            return Task.CompletedTask;
        }

        public override Task<object[]> GetRow()
        {
            var rows = new List<object[]>();
            var columnCount = _table.Columns.Count;

            if (_xPathNodeIterator != null && _xPathNodeIterator.MoveNext())
            {
                var currentRow = _xPathNodeIterator.Current;

                var row = new object[_fieldCount];

                if (_responseDataOrdinal >= 0)
                {
                    row[_responseDataOrdinal] = currentRow.OuterXml;
                }

                foreach (var column in _responseSegementOrdinals)
                {
                    var node = currentRow.SelectSingleNode(column.Key);
                    if (node == null)
                    {
                        row[column.Value.Ordinal] = DBNull.Value;
                    }
                    else
                    {
                        if (node.SelectChildren(XPathNodeType.All).Count == 1 || column.Value.Datatype == DataType.ETypeCode.Xml)
                        {
                            row[column.Value.Ordinal] = node.OuterXml;
                        }
                        else
                        {
                            try
                            {
                                row[column.Value.Ordinal] = Operations.Parse(column.Value.Datatype, node.Value);
                            }
                            catch (Exception ex)
                            {
                                throw new FileHandlerException(
                                    $"Failed to convert value on column {column.Key} to datatype {column.Value.Datatype}. {ex.Message}",
                                    ex, node.Value);
                            }
                        }
                    }
                }

                return Task.FromResult(row);
            }
            else
            {
                return Task.FromResult((object[])null);
            }
        }

    }
}