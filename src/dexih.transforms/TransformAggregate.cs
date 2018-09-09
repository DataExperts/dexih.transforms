using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Mappings;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Transforms;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    [Transform(
        Name = "Aggregate",
        Description = "Aggregate group values whilst maintaining the original row values.",
        TransformType = TransformAttribute.ETransformType.Aggregate
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

        public override bool InitializeOutputFields()
        {
            CacheTable = Mappings.Initialize(PrimaryTransform.CacheTable);
            return true;
        }

        public override bool RequiresSort => Mappings.OfType<MapGroup>().Any();

        public override Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            if (query == null)
            {
                query = new SelectQuery();
            }

            var requiredSorts = RequiredSortFields();

            if(query.Sorts != null && query.Sorts.Count > 0)
            {
                for(var i =0; i<requiredSorts.Count; i++)
                {
                    if (query.Sorts[i].Column == requiredSorts[i].Column)
                        requiredSorts[i].Direction = query.Sorts[i].Direction;
                    else
                        break;
                }
            }

            query.Sorts = requiredSorts;

            var returnValue = PrimaryTransform.Open(auditKey, query, cancellationToken);

            
            return returnValue;
        }


        public override bool ResetTransform()
        {
            Mappings.Reset(EFunctionType.Aggregate);
            Mappings.Reset(EFunctionType.Series);

            _firstRecord = true;
            _lastRecord = true;

            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
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
                
                //populate the parameters with the current row.
                Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
                var cacheRow = new object[FieldCount];
                Mappings.MapOutputRow(cacheRow);
                _cachedRows.Enqueue(cacheRow);
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
                            if (nextGroupValues[i] == null && _groupValues != null ||
                                (nextGroupValues[i] != null && _groupValues == null) ||
                                !Equals(nextGroupValues[i], _groupValues[i]) )
                            {
                                groupChanged = true;
                                break;
                            }
                        }
                    }
                    
                    if (!groupChanged)
                    {
                 
                        // if the group has not changed, process the input row
                        Mappings.ProcessInputData(PrimaryTransform.CurrentRow);

                        //create a cached current row.  this will be output when the group has changed.
                        var cacheRow = new object[outputRow.Length];
                        Mappings.MapOutputRow(cacheRow);
                        _cachedRows.Enqueue(cacheRow);
                    }
                    // when group has changed
                    else
                    {
                        ProcessGroupChange(ref outputRow);
                        
                        //store the last groupValues read to start the next grouping.
                        _groupValues = nextGroupValues;
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
                ProcessGroupChange(ref outputRow);

                _lastRecord = true;
            }
            
            _firstRecord = false;
            return outputRow;

        }

        private void ProcessGroupChange(ref object[] outputRow)
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
                    var moreRows = Mappings.ProcessAggregateRow(new FunctionVariables() {Index = index}, row, EFunctionType.Aggregate);
                    
                    // if the aggregate function wants more rows, store them in a separate collection.
                    while (moreRows)
                    {
                        var rowCopy = new object[FieldCount];
                        row.CopyTo(rowCopy, 0);
                        moreRows = Mappings.ProcessAggregateRow(new FunctionVariables() {Index = index}, row, EFunctionType.Aggregate);

                        if (additionalRows == null)
                        {
                            additionalRows = new List<(int index, object[] row)>();
                        }
                        
                        additionalRows.Add((index, rowCopy));
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
                outputRow = _cachedRows.Dequeue();
            }
            else
            {
                outputRow = null;
            }
        }

        public override string Details()
        {
            return "Group: " + ( Mappings.PassThroughColumns ? "All columns passed through, " : "") + "Mapped Columns:" + (Mappings.Count());
        }

        public override List<Sort> RequiredSortFields()
        {
            var sortFields = Mappings.OfType<MapGroup>().Select(c=> new Sort { Column = c.InputColumn, Direction = Sort.EDirection.Ascending }).ToList();

            var seriesMapping = (MapSeries) Mappings.SingleOrDefault(c => c is MapSeries _);
            if (seriesMapping != null)
            {
                sortFields.Add(new Sort { Column = seriesMapping.InputColumn, Direction = Sort.EDirection.Ascending });
            }
            
            return sortFields;

        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
