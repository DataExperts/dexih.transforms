using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    [Transform(
        Name = "Series",
        Description = "Group columns, fill a date/numeric series and apply analytical rules to the series.",
        TransformType = ETransformType.Series
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

        public override bool RequiresSort => Mappings.OfType<MapGroup>().Any();

        public override string TransformName { get; } = "Series";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }
        public override  async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            // requestQuery = requestQuery?.CloneProperties<SelectQuery>() ?? new SelectQuery();
            
            // get only the required columns
            requestQuery = new SelectQuery();
            requestQuery.Columns = new SelectColumns(Mappings.GetRequiredColumns());

            var requiredSorts = RequiredSortFields();

            if(requestQuery.Sorts != null && requestQuery.Sorts.Count > 0)
            {
                for(var i =0; i < requiredSorts.Count; i++)
                {
                    if (requestQuery.Sorts.Count <= i && requestQuery.Sorts[i].Column.Name == requiredSorts[i].Column.Name)
                        requiredSorts[i].Direction = requestQuery.Sorts[i].Direction;
                    else
                        break;
                }
            }

            requestQuery.Sorts = requiredSorts;

            SetRequestQuery(requestQuery, true);

            var returnValue = await PrimaryTransform.Open(auditKey, requestQuery, cancellationToken);
            
            GeneratedQuery = new SelectQuery()
            {
                Sorts = requestQuery.Sorts,
                Filters = PrimaryTransform.Filters
            };
            
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

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            object[] outputRow ;
            var skipRead = false;
            
            if (_firstRecord)
            {
                var seriesMappings = Mappings.OfType<MapSeries>().ToArray();
                if (seriesMappings.Length != 1)
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

                skipRead = true; // set skipRead = true, so another records is not read from the primarytransform.

                //populate the parameters with the current row.
                // Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
                // Mappings.ProcessInputData(new FunctionVariables() { SeriesValue = _seriesValue}, PrimaryTransform.CurrentRow);
            }

            outputRow = new object[FieldCount];

            // used to track if the group fields have changed
            var groupChanged = false;

            if (skipRead)
            {
                skipRead = false;
                if (_lastRecord && PrimaryTransform.IsClosed)
                {
                    return null;
                }
            }
            else if ( await PrimaryTransform.ReadAsync(cancellationToken) == false && _lastRecord)
            {
                return null;
            }

            var currentRow = PrimaryTransform.CurrentRow;

            do
            {
                _lastRecord = false;

                // get group values of the new row
                var nextGroupValues = Mappings.GetGroupValues(PrimaryTransform.CurrentRow);

                //if it's the first record then the group values are being set for the first time.
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
                        if (nextGroupValues[i] == null && _groupValues?[i] != null ||
                            nextGroupValues[i] != null && _groupValues == null ||
                            !Equals(nextGroupValues[i], _groupValues[i]))
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
                            await Mappings.ProcessInputData(new FunctionVariables() {SeriesValue = nextSeriesValue},
                                PrimaryTransform.CurrentRow, null, cancellationToken);
                            _seriesValue = nextSeriesValue;
                        }
                        else if (Equals(nextSeriesValue, _seriesValue))
                        {
                            //create a cached current row.  this will be output when the group has changed.
                            await Mappings.ProcessInputData(new FunctionVariables() {SeriesValue = nextSeriesValue},
                                PrimaryTransform.CurrentRow, null, cancellationToken);
                            var cacheRow = new object[outputRow.Length];
                            Mappings.MapOutputRow(cacheRow);
                            _seriesValue = nextSeriesValue;
                        }
                        else
                        {
                            var cacheRow = new object[outputRow.Length];
                            Mappings.MapOutputRow(cacheRow);
                            var (_, ignore) = await Mappings.ProcessAggregateRow(new FunctionVariables(), cacheRow, EFunctionType.Aggregate, cancellationToken);
                            
                            Mappings.Reset(EFunctionType.Aggregate);

                            if (ignore)
                            {
                                TransformRowsIgnored += 1;
                            }
                            else
                            {
                                _cachedRows.Enqueue(cacheRow);    
                            }
                            

                            (_, ignore) = await Mappings.ProcessInputData(new FunctionVariables() {SeriesValue = nextSeriesValue},
                                PrimaryTransform.CurrentRow, null, cancellationToken);

                            if (ignore)
                            {
                                TransformRowsIgnored += 1;
                                continue;
                            }
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
                        var isAfterStart = _seriesStart == null ? 0
                            : ((IComparable) _seriesStart)?.CompareTo((IComparable) nextSeriesValue) ?? 0;
                        
                        var isBeforeFinish = _seriesFinish == null ? 0
                            : ((IComparable) nextSeriesValue)?.CompareTo((IComparable) _seriesFinish) ?? 0;
                        
                        if (isAfterStart > 0 || isBeforeFinish == 1)
                        {
                            _seriesValue = null;
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
                                var (_, ignore) = await Mappings.ProcessInputData(new FunctionVariables() {SeriesValue = nextSeriesValue},
                                    PrimaryTransform.CurrentRow, null, cancellationToken);

                                if (ignore)
                                {
                                    TransformRowsIgnored += 1;
                                    continue;
                                }
                                else
                                {
                                    var cacheRow = new object[outputRow.Length];
                                    Mappings.MapOutputRow(cacheRow);
                                }
                                break;
                            }

                            if (!startFilling)
                            {
                                var cacheRow = new object[outputRow.Length];
                                Mappings.MapOutputRow(cacheRow);
                                var (_, ignore) = await Mappings.ProcessAggregateRow(new FunctionVariables(), cacheRow, EFunctionType.Aggregate, cancellationToken);
                                Mappings.Reset(EFunctionType.Aggregate);

                                if (ignore)
                                {
                                    TransformRowsIgnored += 1;
                                }
                                else
                                {
                                    _cachedRows.Enqueue(cacheRow);    
                                }
                                
                                startFilling = true;
                            }
                            else
                            {
                                // if the series is not greater then create a dummy one, and continue the loop
                                var fillerRow = new object[PrimaryTransform.FieldCount];
                                Mappings.CreateFillerRow(currentRow, fillerRow, _seriesValue);
                                var (_, ignore) = await Mappings.ProcessInputData(new FunctionVariables() {SeriesValue = _seriesValue},
                                    fillerRow, null, cancellationToken);

                                if (ignore)
                                {
                                    TransformRowsIgnored += 1;
                                }
                                else
                                {
                                    var cacheRow = new object[outputRow.Length];
                                    Mappings.MapOutputRow(cacheRow);
                                    await Mappings.ProcessFillerRow(new FunctionVariables(), cacheRow, EFunctionType.Aggregate, cancellationToken);
                                    _cachedRows.Enqueue(cacheRow);
                                }
                            }

                            _seriesValue = _seriesMapping.CalculateNextValue(_seriesValue, 1);

                            if (fillCount++ > 10000)
                            {
                                throw new Exception("The series continuation could not be found.");
                            }
                        } while (true);
                    }

                }
                // when group has changed
                else
                {
                    outputRow = await ProcessGroupChange(outputRow, currentRow, cancellationToken);

                    //store the last groupvalues read to start the next grouping.
                    _groupValues = nextGroupValues;

                }

                _firstRecord = false;

                if (groupChanged)
                {
                    break;
                }

                currentRow = PrimaryTransform.CurrentRow;

            } while (await PrimaryTransform.ReadAsync(cancellationToken));


            if (_firstRecord)
            {
                return null;
            }

            if (groupChanged == false) //if the reader has finished with no group change, write the values and set last record
            {
                outputRow = await ProcessGroupChange(outputRow, currentRow, cancellationToken);

                _lastRecord = true;
            }
            
            _firstRecord = false;
            return outputRow;

        }

        private async Task<object[]> ProcessGroupChange(object[] outputRow, object[] previousRow, CancellationToken cancellationToken)
        {
            // if the group has changed, update all cached rows with aggregate functions.
            if (_cachedRows != null && _cachedRows.Count > 0)
            {
                //create a cached current row.  this will be output when the group has changed.
                var cacheRow = new object[outputRow.Length];
                Mappings.MapOutputRow(cacheRow);
                var (_, ignore) = await Mappings.ProcessAggregateRow(new FunctionVariables(), cacheRow, EFunctionType.Aggregate, cancellationToken);
                if (ignore)
                {
                    TransformRowsIgnored += 1;
                }
                else
                {
                    _cachedRows.Enqueue(cacheRow);    
                }

                // fill any remaining rows.
                if (_seriesMapping.SeriesFill && _seriesFinish != null)
                {
                    _seriesValue = _seriesMapping.CalculateNextValue(_seriesValue, 1);
                    var compareResult = ((IComparable) _seriesFinish)?.CompareTo((IComparable) _seriesValue) ?? 0;

                    // loop while the series value is less than the series finish.
                    while (compareResult >= 0)
                    {
                        // if the series is not greater then create a dummy one, and continue the loop
                        var fillerRow = new object[PrimaryTransform.FieldCount];
                        Mappings.CreateFillerRow(previousRow, fillerRow, _seriesValue);
                        (_, ignore) = await Mappings.ProcessInputData(new FunctionVariables() { SeriesValue = _seriesValue, Forecast = true}, fillerRow, null, cancellationToken);

                        if (ignore)
                        {
                            TransformRowsIgnored += 1;
                        }
                        else
                        {
                            var cacheRow1 = new object[outputRow.Length];
                            Mappings.MapOutputRow(cacheRow1);
                            _cachedRows.Enqueue(cacheRow1);
                        }

                        _seriesValue = _seriesMapping.CalculateNextValue(_seriesValue, 1);
                        compareResult = ((IComparable) _seriesFinish)?.CompareTo((IComparable) _seriesValue) ?? 0;
                    }
                }

               Mappings.Reset(EFunctionType.Aggregate);

                var cacheIndex = 0;
                foreach (var row in _cachedRows)
                {
                    (_, ignore) = await Mappings.ProcessAggregateRow(new FunctionVariables() {Index = cacheIndex++}, row, EFunctionType.Series, cancellationToken);
                    if (ignore)
                    {
                        TransformRowsIgnored += 1;
                    }
                }
                
                outputRow = _cachedRows.Dequeue();

            }
            else
            {
                outputRow = null;
            }
            
            _seriesValue = null;

            return outputRow;

        }

        public override Sorts RequiredSortFields()
        {
            var sortFields = new Sorts(Mappings.OfType<MapGroup>().Select(c=> new Sort { Column = c.InputColumn, Direction = ESortDirection.Ascending }));

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
