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
        Name = "Series",
        Description = "Group columns, fill a date/numeric series and apply analytical rules to the series.",
        TransformType = TransformAttribute.ETransformType.Series
    )]
    public class TransformSeries : Transform
    {
        public TransformSeries() {  }

        public TransformSeries(Transform inReader, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inReader);
        }

        private bool _firstRecord;
        private bool _lastRecord;

        private object[] _groupValues;
        
        private object _seriesValue;
        private object _seriesStart;
        private object _seriesFinish;

        private MapSeries _seriesMapping;

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
            
            if (_firstRecord)
            {
                var seriesMappings = Mappings.OfType<MapSeries>().ToArray();
                if (seriesMappings.Count() != 1)
                {
                    throw new TransformException("The series transform must have one (and only on) series mapping defined.");
                }

                _seriesMapping = seriesMappings[0];

                _seriesStart = _seriesMapping.GetSeriesStart();
                _seriesFinish = _seriesMapping.GetSeriesFinish();
            }

            //if there are records in the cache, then empty them out before getting new records.
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
                        // logic for series with NO fillers.
                        if (!_seriesMapping.SeriesFill)
                        {
                            var nextSeriesValue = _seriesMapping.NextValue(0, PrimaryTransform.CurrentRow);

                            if (_seriesValue == null)
                            {
                                Mappings.ProcessInputData(new FunctionVariables() { SeriesValue = nextSeriesValue}, PrimaryTransform.CurrentRow);
                                _seriesValue = nextSeriesValue;
                            }
                            else if (Equals(nextSeriesValue, _seriesValue))
                            {
                                //create a cached current row.  this will be output when the group has changed.
                                Mappings.ProcessInputData(new FunctionVariables() { SeriesValue = nextSeriesValue}, PrimaryTransform.CurrentRow);
                                var cacheRow = new object[outputRow.Length];
                                Mappings.MapOutputRow(cacheRow);
                                _seriesValue = nextSeriesValue;
                            }
                            else
                            {
                                var cacheRow = new object[outputRow.Length];
                                Mappings.MapOutputRow(cacheRow);
                                Mappings.ProcessAggregateRow(new FunctionVariables(), cacheRow, EFunctionType.Aggregate);
                                Mappings.Reset(EFunctionType.Aggregate);
                                _cachedRows.Enqueue(cacheRow);

                                Mappings.ProcessInputData(new FunctionVariables() { SeriesValue = nextSeriesValue}, PrimaryTransform.CurrentRow);
                                _seriesValue = nextSeriesValue;
                            }
                        }
                        else
                        {
                            var startFilling = false;

                            // if the first record, then load the current row.
                            if (_seriesValue == null)
                            {
                                startFilling = true;
                                if (_seriesStart != null)
                                {
                                    _seriesValue = _seriesStart;
                                }
                                else
                                {
                                    _seriesValue = _seriesMapping.NextValue(0, PrimaryTransform.CurrentRow);
                                }
                            }

                            var fillCount = 1;
                            var nextSeriesValue = _seriesMapping.NextValue(0, PrimaryTransform.CurrentRow);
                            
                            //filter series start/finish
                            var isAfterStart = _seriesStart == null ? 0 : ((IComparable) _seriesStart)?.CompareTo((IComparable) nextSeriesValue) ?? 0;
                            var isBeforeFinish = _seriesFinish == null ? 0 : ((IComparable) nextSeriesValue)?.CompareTo((IComparable) _seriesFinish) ?? 0;
                            if (isAfterStart > 0 || isBeforeFinish == 1)
                            {
                                continue;
                            }
                            
                            // loop to create filler rows.
                            do
                            {
                                var compareResult = ((IComparable) _seriesValue)?.CompareTo((IComparable) nextSeriesValue) ?? 0;
                                
                                if (compareResult > 0)
                                {
                                    throw new TransformException(
                                        "The group transform failed, as the series column is not sorted.  Use a group transform prior to this group transform to first group and aggregate series values",
                                        _seriesValue);
                                }

                                // if compare is equal
                                if (compareResult == 0)
                                {
                                    Mappings.ProcessInputData(new FunctionVariables() { SeriesValue = nextSeriesValue}, PrimaryTransform.CurrentRow);
                                    var cacheRow = new object[outputRow.Length];
                                    Mappings.MapOutputRow(cacheRow);

                                    break;
                                }
                                else
                                {
                                    if (!startFilling)
                                    {
                                        var cacheRow = new object[outputRow.Length];
                                        Mappings.MapOutputRow(cacheRow);
                                        Mappings.ProcessAggregateRow(new FunctionVariables(), cacheRow, EFunctionType.Aggregate);
                                        Mappings.Reset(EFunctionType.Aggregate);
                                        _cachedRows.Enqueue(cacheRow);
                                        startFilling = true;
                                    }
                                    else
                                    {
                                        // if the series is not greater then create a dummy one, and continue the loop
                                        var fillerRow = new object[PrimaryTransform.FieldCount];
                                        Mappings.CreateFillerRow(PrimaryTransform.CurrentRow, fillerRow, _seriesValue);
                                        Mappings.ProcessInputData(new FunctionVariables() { SeriesValue = _seriesValue}, fillerRow);
                                        var cacheRow = new object[outputRow.Length];
                                        Mappings.MapOutputRow(cacheRow);
                                        _cachedRows.Enqueue(cacheRow);                                            
                                    }

                                    _seriesValue = _seriesMapping.CalculateNextValue(_seriesValue, 1);
                                }

                                

                                if (fillCount > 10000)
                                {
                                    throw new Exception("The series continuation could not be found.");
                                }
                            } while (true);
                        }

                    }
                    // when group has changed
                    else
                    {
                        ProcessGroupChange(ref outputRow);
                        
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
                var cacheRow = new object[outputRow.Length];
                Mappings.MapOutputRow(cacheRow);
                Mappings.ProcessAggregateRow(new FunctionVariables(), cacheRow, EFunctionType.Aggregate);
                _cachedRows.Enqueue(cacheRow);

                // fill any remaining rows.
                if (_seriesMapping.SeriesFill && _seriesMapping.SeriesFinish != null)
                {
                    var seriesFinish = _seriesMapping.GetSeriesFinish();
                    _seriesValue = _seriesMapping.CalculateNextValue(_seriesValue, 1);
                    var compareResult = ((IComparable) seriesFinish)?.CompareTo((IComparable) _seriesValue) ?? 0;

                    // loop while the series value is less than the series finish.
                    while (compareResult >= 0)
                    {
                        // if the series is not greater then create a dummy one, and continue the loop
                        var fillerRow = new object[PrimaryTransform.FieldCount];
                        Mappings.CreateFillerRow(PrimaryTransform.CurrentRow, fillerRow, _seriesValue);
                        Mappings.ProcessInputData(new FunctionVariables() { SeriesValue = _seriesValue, Forecast = true}, fillerRow);

                        var cacheRow1 = new object[outputRow.Length];
                        Mappings.MapOutputRow(cacheRow1);
                        _cachedRows.Enqueue(cacheRow1);

                        _seriesValue = _seriesMapping.CalculateNextValue(_seriesValue, 1);
                        compareResult = ((IComparable) seriesFinish)?.CompareTo((IComparable) _seriesValue) ?? 0;
                    }
                }

                Mappings.Reset(EFunctionType.Aggregate);

                var cacheIndex = 0;
                foreach (var row in _cachedRows)
                {
                    Mappings.ProcessAggregateRow(new FunctionVariables() {Index = cacheIndex++}, row, EFunctionType.Series);
                }
                
                outputRow = _cachedRows.Dequeue();

            }
            else
            {
                outputRow = null;
            }
            
            _seriesValue = null;

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
