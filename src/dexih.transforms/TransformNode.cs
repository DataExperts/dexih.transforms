using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    /// <summary>
    /// Transform to flatten an array property into the parent row.
    /// </summary>
    [Transform(
        Name = "Transform Chapter",
        Description = "Allows a chapter (or nested node) to be transformed.",
        TransformType = TransformAttribute.ETransformType.Mapping
    )]
    public class TransformChapter : Transform
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="inTransform"></param>
        /// <param name="mappings"></param>
        /// <param name="arrayPath">The path of columns to find the array</param>
        public TransformChapter(Transform inTransform, Mappings mappings, TableColumn[] arrayPath)
        {
            ArrayPath = arrayPath;
            Mappings = mappings;
            SetInTransform(inTransform);
        }

        private int[] _chapterOrdinals;


        private bool _writeCache;
        private int _writeCachePosition;
        private List<object[]> _cacheRows;
        

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            var returnValue = await PrimaryTransform.Open(auditKey, query, cancellationToken);
            if (!returnValue) return false;

            // convert the array path to a sequence of ordinals, to improve performance
            _chapterOrdinals = new int[ArrayPath.Length];

            // create the column ordinals to navigate the child columns.
            var columns = PrimaryTransform.CacheTable.Columns;
            for (var i = 0; i < ArrayPath.Length; i++)
            {
                var ordinal = columns.GetOrdinal(ArrayPath[i].Name);

                if (ordinal < 0)
                {
                    throw new Exception($"The column {ArrayPath[i].Name} could not be found in the source transform.");
                }

                _chapterOrdinals[i] = ordinal;
                var column = columns[ordinal];
                columns = column.ChildColumns;
            }

            var flattened = FlattenColumns(PrimaryTransform.CacheTable.Columns);
            CacheTable = new Table("flattened", new TableColumns(flattened), null);
            return true;
        }

        /// <summary>
        /// Recursively create a new set of columns with the required columns flattened.
        /// </summary>
        /// <param name="columns">Source columns</param>
        /// <param name="chapterPathLevel">The current level in the array path.</param>
        /// <param name="group"></param>
        /// <returns></returns>
        private IEnumerable<TableColumn> FlattenColumns(TableColumns columns, int chapterPathLevel = 0, string group = null)
        {
            var newColumns = new List<TableColumn>();

            if (chapterPathLevel < _chapterOrdinals.Length)
            {
                for (var i = 0; i < columns.Count; i++)
                {
                    if (i == _chapterOrdinals[chapterPathLevel])
                    {
                        if (chapterPathLevel >= _chapterOrdinals.Length - FlattenLevels)
                        {
                            var newGroup = group == null ? columns[i].Name : group + "." + columns[i].Name;
                            newColumns.AddRange(FlattenColumns(columns[i].ChildColumns, chapterPathLevel + 1, newGroup));
                        }
                        else
                        {
                            var newColumn = columns[i].Copy();
                            var cols = FlattenColumns(columns[i].ChildColumns, chapterPathLevel + 1, group);
                            newColumn.ChildColumns = new TableColumns(cols);
                        }
                    }
                    else
                    {
                        var newColumn = columns[i].Copy();
                        newColumn.ColumnGroup = group;
                        newColumns.Add(newColumn);
                    }
                }
            }
            else
            {
                for (var i = 0; i < columns.Count; i++)
                {
                    var newColumn = columns[i].Copy();
                    newColumn.ColumnGroup = group;
                    newColumns.Add(newColumn);
                }
            }

            return newColumns;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if (_writeCache)
            {
                var row = _cacheRows[_writeCachePosition++];
                
                if (_writeCachePosition >= _cacheRows.Count)
                {
                    _writeCache = false;
                    _cacheRows.Clear();
                }
                
                return row;
            }

            if (!await PrimaryTransform.ReadAsync(cancellationToken))
            {
                return null;
            }

            var rows = await FlattenRow(PrimaryTransform.CurrentRow, null, 0, 0);

            if (rows.Count == 0)
            {
                return null;
            }

            if (rows.Count > 1)
            {
                _cacheRows = rows;
                _writeCache = true;
                _writeCachePosition = 1;
            }

            return rows[0];

        }
        
        private async  Task<List<object[]>> FlattenRow(object[] sourceRow, object[] outputRow, int pos, int arrayPathLevel)
        {
            if (arrayPathLevel < _chapterOrdinals.Length)
            {
                // create the flattened rows and add the input row.
                var rows = new List<object[]> {outputRow ?? new object[FieldCount]};

                for (var i = 0; i < sourceRow.Length; i++)
                {
                    if (i == _chapterOrdinals[arrayPathLevel])
                    {
                        if (arrayPathLevel >= _chapterOrdinals.Length - FlattenLevels)
                        {
                            // var newGroup = group == null ? columns[i].Name : group + "." + columns[i].Name;

                            var childTransform = (Transform) sourceRow[i];

                            var flattenedRows = new List<object[]>();

                            // read each row and flatten into current row.

                            if (childTransform.HasRows)
                            {
                                while (await childTransform.ReadAsync())
                                {
                                    foreach (var baseRow in rows)
                                    {
                                        flattenedRows.AddRange(await FlattenRow(childTransform.CurrentRow,
                                            baseRow.ToArray(), pos, arrayPathLevel + 1));
                                    }
                                }
                                rows = flattenedRows;
                            }

                            pos += childTransform.FieldCount;
                        }
                        else
                        {
                            foreach (var row in rows)
                            {
                                row[pos++] = sourceRow[i];
                            }
                            
                        }
                    }
                    else
                    {
                        foreach (var row in rows)
                        {
                            row[pos++] = sourceRow[i];
                        }
                    }
                    
                }

                return rows;
            }
            else
            {
                for (var i = 0; i < sourceRow.Length; i++)
                {
                    outputRow[pos++] = sourceRow[i];
                }
                return new List<object[]> {outputRow};
            }

        }

        public override string Details()
        {
            return "Flatten Array";
        }

        public override bool ResetTransform()
        {
            return true;
        }
    }
}