using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    [Transform(
        Name = "Aggregate",
        Description = "Aggregate group values whilst maintaining the original row values.",
        TransformType = ETransformType.Aggregate
    )]
    public class TransformAggregate : Transform
    {
        public TransformAggregate() {  }

        public TransformAggregate(Transform inReader, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inReader);
        }

        private bool _firstRecord;
        private bool _lastRecord;

        private object[] _groupValues;
        
        private Queue<object[]> _cachedRows;

        private MapGroupNode _groupNode;
        private int _groupNodeOrdinal;
        
        public override bool RequiresSort => Mappings.OfType<MapGroup>().Any();

        public override string TransformName { get; } = "Aggregate";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }
        
        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            requestQuery = new SelectQuery {Columns = new SelectColumns(Mappings.GetRequiredColumns())};

            // get only the required columns

            var requiredSorts = RequiredSortFields();

            if(requestQuery.Sorts != null && requestQuery.Sorts.Count > 0)
            {
                for(var i =0; i<requiredSorts.Count; i++)
                {
                    if (requestQuery.Sorts[i].Column.Equals(requiredSorts[i].Column))
                        requiredSorts[i].Direction = requestQuery.Sorts[i].Direction;
                    else
                        break;
                }
            }

            requestQuery.Sorts = requiredSorts;

            var returnValue = await PrimaryTransform.Open(auditKey, requestQuery, cancellationToken);
            if (!returnValue)
            {
                return false;
            }

            SetRequestQuery(requestQuery, true);

            var nodeMappings = Mappings.OfType<MapGroupNode>().ToArray();
            if (nodeMappings.Length == 1)
            {
                _groupNode = nodeMappings[0];
                var nodeColumn = _groupNode.NodeColumn;
                if (nodeColumn != null)
                {
                    _groupNodeOrdinal = CacheTable.GetOrdinal(nodeColumn);
                }
            }
            else
            {
                _groupNodeOrdinal = -1;
            }

            GeneratedQuery = new SelectQuery()
            {
                Sorts = PrimaryTransform.SortFields,
                Filters = PrimaryTransform.Filters
            };
            
            return true;
        }


        public override bool ResetTransform()
        {
            Mappings.Reset(EFunctionType.Aggregate);
            Mappings.Reset(EFunctionType.Series);

            _firstRecord = true;
            _lastRecord = true;

            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            object[] outputRow ;
            
            //if there are records in the row cache, then empty them out before getting new records.
            if(_firstRecord)
            {
                _cachedRows = new Queue<object[]>();
            }
            else if( _cachedRows.Count > 0)
            {
                outputRow = _cachedRows.Dequeue();
                return outputRow;
            }
            //if all rows have been iterated through, reset the cache and add the stored row for the next group 
            else if(_firstRecord == false && _lastRecord == false)
            {
                //reset the aggregate functions
                Mappings.Reset(EFunctionType.Aggregate);
                Mappings.Reset(EFunctionType.Series);

                if (_groupNodeOrdinal < 0)
                {
                    //populate the parameters with the current row.
                    var (_, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
                    if (ignore)
                    {
                        TransformRowsIgnored += 1;
                    }
                    else
                    {
                        var cacheRow = new object[FieldCount];
                        Mappings.MapOutputRow(cacheRow);
                        _cachedRows.Enqueue(cacheRow);
                    }
                }
                else
                {
                    
                    var (_, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
                    if (ignore)
                    {
                        TransformRowsIgnored += 1;
                    }
                }
            }

            outputRow = new object[FieldCount];

            // used to track if the group fields have changed
            var groupChanged = false;
            
            if (await PrimaryTransform.ReadAsync(cancellationToken) == false)
            {
                if (_lastRecord) //return false is all record have been written.
                {
                    return null;
                }
            }
            else
            {
                do
                {
                    _lastRecord = false;

                    // get group values of the new row
                    var nextGroupValues = Mappings.GetGroupValues(PrimaryTransform.CurrentRow);
                    
                    //if it's the first record then the groupvalues are being set for the first time.
                    if (_firstRecord)
                    {
                        groupChanged = false;
                        _groupValues = nextGroupValues;
                    }
                    else
                    {
                        //if not first row, then check if the group values have changed from the previous row
                        for (var i = 0; i < nextGroupValues.Length; i++)
                        {
                            if (
                                nextGroupValues[i] == null && _groupValues?[i] != null ||
                                nextGroupValues[i] != null && _groupValues == null ||
                                !Equals(nextGroupValues[i], _groupValues?[i]) )
                            {
                                groupChanged = true;
                                break;
                            }
                        }
                    }

                    if (_groupNodeOrdinal >= 0)
                    {
                        if (!groupChanged)
                        {
                            // if the group has not changed, process the input row
                            var (_, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
                            if (ignore)
                            {
                                TransformRowsIgnored += 1;
                                continue;
                            }
                        }
                        // when group has changed
                        else
                        {
                            var (_, ignore) = await Mappings.ProcessAggregateRow(new FunctionVariables(), outputRow, EFunctionType.Aggregate, cancellationToken);
                            if (ignore)
                            {
                                TransformRowsIgnored += 1;
                                continue;
                            }

                            Mappings.MapOutputRow(outputRow);

                            //store the last groupValues read to start the next grouping.
                            _groupValues = nextGroupValues;
                        }
                        
                    }
                    else
                    {
                        if (!groupChanged)
                        {
                            // if the group has not changed, process the input row
                            var (_, ignore) = await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);
                            if (ignore)
                            {
                                TransformRowsIgnored += 1;
                                continue;
                            }
                            
                            //create a cached current row.  this will be output when the group has changed.
                            var cacheRow = new object[outputRow.Length];
                            Mappings.MapOutputRow(cacheRow);
                            _cachedRows.Enqueue(cacheRow);
                        }
                        // when group has changed
                        else
                        {
                            outputRow = await ProcessGroupChange(cancellationToken);

                            //store the last groupValues read to start the next grouping.
                            _groupValues = nextGroupValues;
                        }
                    }

                    _firstRecord = false;

                    if (groupChanged)
                    {
                        break;
                    }

                } while (await PrimaryTransform.ReadAsync(cancellationToken));
            }

            if (_firstRecord)
            {
                return null;
            }

            if (groupChanged == false) //if the reader has finished with no group change, write the values and set last record
            {
                if (_groupNodeOrdinal >= 0)
                {
                    var (_, ignore) = await Mappings.ProcessAggregateRow(new FunctionVariables(), outputRow, EFunctionType.Aggregate, cancellationToken);
                    if (!ignore)
                    {
                        TransformRowsIgnored += 1;
                        Mappings.MapOutputRow(outputRow);
                    }
                }
                else
                {
                    outputRow = await ProcessGroupChange(cancellationToken);    
                }
                

                _lastRecord = true;
            }
            
            _firstRecord = false;
            return outputRow;

        }

        private async Task<object[]> ProcessGroupChange(CancellationToken cancellationToken)
        {
            // if the group has changed, update all cached rows with aggregate functions.
            if (_cachedRows != null && _cachedRows.Any())
            {
                //create a cached current row.  this will be output when the group has changed.
//                var cacheRow = new object[outputRow.Length];
//                Mappings.MapOutputRow(cacheRow);
//                _cachedRows.Enqueue(cacheRow);

                var index = 0;
                List<(int index, object[] row)> additionalRows = null;
                foreach (var row in _cachedRows)
                {
                    var (moreRows, ignore)  = await Mappings.ProcessAggregateRow(new FunctionVariables() {Index = index}, row, EFunctionType.Aggregate, cancellationToken);

                    if (ignore)
                    {
                        TransformRowsIgnored += 1;
                    }
                    
                    // if the aggregate function wants to provide more rows, store them in a separate collection.
                    while (moreRows && !ignore)
                    {
                        var rowCopy = new object[FieldCount];
                        row.CopyTo(rowCopy, 0);
                        (moreRows, ignore)  = await Mappings.ProcessAggregateRow(new FunctionVariables() {Index = index}, row, EFunctionType.Aggregate, cancellationToken);

                        if (additionalRows == null)
                        {
                            additionalRows = new List<(int index, object[] row)>();
                        }

                        if (ignore)
                        {
                            TransformRowsIgnored += 1;
                        }
                        else
                        {
                            additionalRows.Add((index, rowCopy));    
                        }
                    }
                    
                    index++;
                }
                
                // merge the new rows in with existing cache
                if (additionalRows != null)
                {
                    var newQueue = new Queue<object[]>();
                    index = 0;
                    var additionalRowsIndex = 0;
                    foreach (var row in _cachedRows)
                    {
                        while (additionalRowsIndex < additionalRows.Count && index <= additionalRows[additionalRowsIndex].index)
                        {
                            newQueue.Enqueue(additionalRows[additionalRowsIndex++].row);
                        }

                        newQueue.Enqueue(row);
                    }

                    _cachedRows = newQueue;
                }
                
                Mappings.Reset(EFunctionType.Aggregate);
                return _cachedRows.Dequeue();
            }

            return null;
        }


        public override Sorts RequiredSortFields()
        {
            var sortFields = new Sorts(Mappings.OfType<MapGroup>().Select(c => new Sort
                {Column = c.InputColumn, Direction = ESortDirection.Ascending}));
            

            var seriesMapping = (MapSeries) Mappings.SingleOrDefault(c => c is MapSeries _);
            if (seriesMapping != null)
            {
                sortFields.Add(new Sort { Column = seriesMapping.InputColumn, Direction = ESortDirection.Ascending });
            }
            
            return sortFields;

        }

        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
