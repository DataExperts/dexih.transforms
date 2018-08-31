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
        Name = "Group",
        Description = "Group columns and apply aggregation rules to other columns.",
        TransformType = TransformAttribute.ETransformType.Group
    )]
    public class TransformGroup : Transform
    {
        public TransformGroup() {  }

        public TransformGroup(Transform inReader, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inReader);
        }

        private bool _firstRecord;
        private bool _lastRecord;

        private object[] _groupValues;

        private MapSeries _seriesMapping = null;

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
            Mappings.Reset();

            _firstRecord = true;
            _lastRecord = true;

            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            object[] outputRow ;
            
            if (_firstRecord)
            {
                _seriesMapping = (MapSeries) Mappings.SingleOrDefault(c => c is MapSeries _);
            }

            //if there are records in the passthrough cache, then empty them out before getting new records.
            if (!Mappings.GroupRows || _seriesMapping != null)
            {
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
                    Mappings.Reset();
                    
                    //populate the parameters with the current row.
                    Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
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
                        if (_seriesMapping != null)
                        {
                            if (_firstRecord)
                            {
                                Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
                                var cacheRow = new object[outputRow.Length];
                                Mappings.ProcessOutputRow(cacheRow);
                                _cachedRows.Enqueue(cacheRow);
                            }
                            else
                            {
                                var fillCount = 1;
                                var nextSeriesValue = _seriesMapping.NextValue(0, PrimaryTransform.CurrentRow);

                                do
                                {
                                    var fillSeriesValue = _seriesMapping.NextValue(1);

                                    var compareResult =
                                        ((IComparable) fillSeriesValue)?.CompareTo((IComparable) nextSeriesValue) ?? 0;

                                    if (compareResult > 0)
                                    {
                                        throw new Exception(
                                            "The group transform failed, as the series column is not sorted");
                                    }

                                    if (compareResult == 0)
                                    {
                                        //create a cached current row.  this will be output when the group has changed.
                                        Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
                                        var cacheRow = new object[outputRow.Length];
                                        Mappings.ProcessOutputRow(cacheRow);
                                        _cachedRows.Enqueue(cacheRow);
                                        break;
                                    }
                                    else
                                    {
                                        // if the series is not greater then create a dummy one, and continue the loop
                                        var fillerRow = new object[PrimaryTransform.FieldCount];
                                        Mappings.CreateFillerRow(null, fillerRow, fillSeriesValue);
                                        Mappings.ProcessInputData(fillerRow);
                                        var cacheRow = new object[outputRow.Length];
                                        Mappings.ProcessOutputRow(cacheRow);
                                        // _seriesMapping.ProcessNextValueOutput(fillCount, cacheRow); 
                                        _cachedRows.Enqueue(cacheRow);
                                    }

                                    //fillCount++;

                                    if (fillCount > 10000)
                                    {
                                        throw new Exception("The series continuation could not be found.");
                                    }
                                } while (true);
                            }
                        }
                        else
                        {
                            // if the group has not changed, process the input row
                            Mappings.ProcessInputData(PrimaryTransform.CurrentRow);

                            if (!Mappings.GroupRows)
                            {
                                //create a cached current row.  this will be output when the group has changed.
                                var cacheRow = new object[outputRow.Length];
                                Mappings.ProcessOutputRow(cacheRow);
                                _cachedRows.Enqueue(cacheRow);
                            }
                        }
                    }
                    else
                    {
                        // if the group has changed, update all cached rows with aggregate functions.
                        if (_cachedRows != null)
                        {
                            var index = 0;
                            foreach (var row in _cachedRows)
                            {
                                Mappings.ProcessAggregateRow(index, row);
                                index++;
                            }
                        }

                        // if groupr ow is not on, then get the latest cached row
                        if (!Mappings.GroupRows)
                        {
                            ////set the first cached row to current
                            outputRow = _cachedRows.Dequeue();
                        }
                        else
                        {
                            Mappings.ProcessOutputRow(outputRow);
                            Mappings.ProcessAggregateRow(0, outputRow);
                            
                            Mappings.Reset();
                            Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
                        }
                        
                        //store the last groupvalues read to start the next grouping.
                        _groupValues = nextGroupValues;

                    }
                    
                    _firstRecord = false;

                    if (groupChanged)
                    {
                        break;
                    }

                } while (await PrimaryTransform.ReadAsync(cancellationToken));
            }

            if (groupChanged == false) //if the reader has finished with no group change, write the values and set last record
            {
                if (!Mappings.GroupRows || _seriesMapping != null)
                {
                    //for passthrough, write out the aggregated values to the cached passthrough set
                    var index = 0;
                    //var startColumn = i;
                    foreach (var row in _cachedRows)
                    {
                        Mappings.ProcessAggregateRow(index, row);
                        index++;
                    }
                    
                    outputRow = _cachedRows.Dequeue();
                }
                else
                {
                    Mappings.ProcessOutputRow(outputRow);
                    Mappings.ProcessAggregateRow(0, outputRow);
                }

                // _groupValues = nextGroupValues;
                _lastRecord = true;
            }
            
            _firstRecord = false;
            return outputRow;

        }

        public override string Details()
        {
            return "Group: " + ( Mappings.PassThroughColumns ? "All columns passed through, " : "") + "Mapped Columns:" + (Mappings.Count());
        }

        public override List<Sort> RequiredSortFields()
        {
            return Mappings.OfType<MapGroup>().Select(c=> new Sort { Column = c.InputColumn, Direction = Sort.EDirection.Ascending }).ToList();
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
