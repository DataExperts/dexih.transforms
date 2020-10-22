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
using Dexih.Utils.DataType;

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
        
        private IComparable _seriesStart;
        private IComparable _seriesFinish;
        private int _seriesProject;

        private MapSeries _seriesMapping;

        private Queue<(IComparable seriesValue, object[] row)> _cachedRows;

        public override bool RequiresSort => true;

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
            (IComparable seriesValue, object[] row) outputRow ;
            
            // on first record initialize some variables.
            if (_firstRecord)
            {
                var seriesMappings = Mappings.OfType<MapSeries>().ToArray();
                if (seriesMappings.Length != 1)
                {
                    throw new TransformException("The series transform must have one (and only on) series mapping defined.");
                }

                _seriesMapping = seriesMappings[0];
                _seriesStart = (IComparable) _seriesMapping.GetSeriesStart();
                _seriesFinish = (IComparable)_seriesMapping.GetSeriesFinish();
                _seriesProject = _seriesMapping.SeriesProject;
                _cachedRows = new Queue<(IComparable seriesValue, object[] row)>();
            }
            
            //if there are records in the cache, then empty them out before getting new records.
            if( _cachedRows.Count > 0)
            {
                outputRow = _cachedRows.Dequeue();

                if (_seriesFinish == null || Operations.LessThanOrEqual(outputRow.seriesValue, _seriesFinish))
                {
                    return outputRow.row;
                }

                _cachedRows.Clear();
            }
            
            //if all rows have been iterated through, reset the cache and add the stored row for the next group 
            if(!_firstRecord && !_lastRecord)
            {
                //reset the aggregate functions
                Mappings.Reset(EFunctionType.Aggregate);
                Mappings.Reset(EFunctionType.Series);

                if (PrimaryTransform.IsClosed)
                {
                    return null;
                }
            }
            else if ( await PrimaryTransform.ReadAsync(cancellationToken) == false)
            {
                return null;
            }

            // set the return row
            outputRow = (null, new object[FieldCount]);

            // used to track if the group fields have changed
            var groupChanged = false;

            // retain the current row, as we read forward to check for group changes.
            object[] currentRow = null;

            // previous row is kept to determine gaps in series
            object[] previousRow = null;

            var previousRowIsFiller = false;

            // loop through building a cache of all rows in the next group
            do
            {
                _lastRecord = false;
                currentRow = PrimaryTransform.CurrentRow;

                // get group values of the new row
                var nextGroupValues = Mappings.GetGroupValues(currentRow);

                //if it's the first record then the group values are being set for the first time.
                if (previousRow == null)
                {
                    _groupValues = nextGroupValues;
                    previousRowIsFiller = previousRowIsFiller | await ProcessFillers(currentRow, null, cancellationToken);
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

                if (groupChanged)
                {
                    outputRow = await ProcessGroupChange(outputRow, previousRow, cancellationToken);

                    //store the last group values read to start the next grouping.
                    _groupValues = nextGroupValues;
                }
                else
                {
                    var previousSeriesValue = previousRow == null ? null : (IComparable) _seriesMapping.SeriesValue(false, previousRow);
                    var nextSeriesValue = (IComparable) _seriesMapping.SeriesValue(false, currentRow);
            
                    if (previousSeriesValue == null || Equals(nextSeriesValue, previousSeriesValue))
                    {
                        previousRowIsFiller = false;
                        await Mappings.ProcessInputData(new FunctionVariables() {SeriesValue = nextSeriesValue},
                            currentRow, null, cancellationToken);
                    }
                    else
                    {
                        if (!previousRowIsFiller)
                        {
                            await ProcessAggregate(previousSeriesValue, cancellationToken);
                        }

                        // logic for series with fillers.
                        if (_seriesMapping.SeriesFill)
                        {
                             await ProcessFillers(currentRow, previousRow, cancellationToken);
                        }
                        
                        var (_, ignore) = await Mappings.ProcessInputData(
                            new FunctionVariables() {SeriesValue = nextSeriesValue},
                            currentRow, null, cancellationToken);

                        previousRowIsFiller = false;
                        
                        if (ignore)
                        {
                            TransformRowsIgnored += 1;
                        }
                    }
                }

                _firstRecord = false;

                if (groupChanged && outputRow.row != null)
                {
                    break;
                }

                if (groupChanged)
                {
                    Mappings.Reset(EFunctionType.Series);
                    Mappings.Reset(EFunctionType.Aggregate);
                }

                previousRow = currentRow;

                // if the row returned null, this means all the records were filtered
                if (outputRow.row == null)
                {
                    // if the row returned null, this means all the records were filtered
                    groupChanged = false;
                    outputRow.row = new object[FieldCount];
                    var nextSeriesValue = (IComparable) _seriesMapping.SeriesValue(false, currentRow);
                    await Mappings.ProcessInputData(new FunctionVariables() {SeriesValue = nextSeriesValue},
                        currentRow, null, cancellationToken);
                }
                
            } while (await PrimaryTransform.ReadAsync(cancellationToken));
            
            if (_firstRecord)
            {
                return null;
            }

            if (!groupChanged) //if the reader has finished with no group change, write the values and set last record
            {
                outputRow = await ProcessGroupChange(outputRow, currentRow, cancellationToken);
                _lastRecord = true;
            }
            
            _firstRecord = false;
            return outputRow.row;
        }
        
        private async Task ProcessAggregate(IComparable seriesValue, CancellationToken cancellationToken)
        {
            var cacheRow = new object[FieldCount];
            Mappings.MapOutputRow(cacheRow);
            var (_, ignore) = await Mappings.ProcessAggregateRow(new FunctionVariables(), cacheRow,
                EFunctionType.Aggregate, cancellationToken);

            Mappings.Reset(EFunctionType.Aggregate);

            if (ignore)
            {
                TransformRowsIgnored += 1;
            }
            else
            {
                _cachedRows.Enqueue((seriesValue, cacheRow));
            }
        }

        private async Task<bool> ProcessFillers(object[] currentRow, object[] previousRow, CancellationToken cancellationToken)
        {
            var hasFilled = false;
            // if there is no previous series value, fill from the start date until current
            if (previousRow == null)
            {
                if (_seriesStart != null)
                {
                    var currentSeriesValue = currentRow != null ? (IComparable)_seriesMapping.SeriesValue(false, currentRow) : _seriesFinish;
                    
                    if (currentSeriesValue == null)
                    {
                        return false;
                    }

                    IComparable fillSeriesValue;
                    IComparable finishValue;
                    if (Operations.LessThan(currentSeriesValue, _seriesStart))
                    {
                        return false;
                    }
                    else
                    {
                        fillSeriesValue = _seriesStart;
                        finishValue = currentSeriesValue;
                    }
                    
                    var fillCount = 0;
                    while (Operations.LessThan(fillSeriesValue, finishValue))
                    {
                        await CreateFillerRow(currentRow, fillSeriesValue, false, cancellationToken);
                        fillSeriesValue = (IComparable) _seriesMapping.CalculateNextValue(fillSeriesValue);
                        hasFilled = true;

                        if (fillCount++ > 10000)
                        {
                            throw new Exception("The series continuation could not be found.");
                        }
                    }
                }
            } else if (currentRow == null)
            {
                if (_seriesFinish != null || _seriesProject > 0)
                {
                    var currentSeriesValue = (IComparable)_seriesMapping.SeriesValue(true, previousRow);
                    var fillSeriesValue = currentSeriesValue;
                    var fillCount = 0;
                    var projectionCount = 0;
                    while (true)
                    {
                        // if the series has passed the series finish, then start calculating the projections.
                        if (_seriesFinish == null || Operations.GreaterThan(fillSeriesValue, _seriesFinish))
                        {
                            projectionCount++;

                            if (projectionCount > _seriesProject)
                            {
                                break;
                            }
                        }

                        await CreateFillerRow(currentRow, fillSeriesValue, true, cancellationToken);
                        fillSeriesValue = (IComparable) _seriesMapping.CalculateNextValue(fillSeriesValue);
                        hasFilled = true;
                        
                        if (fillCount++ > 10000)
                        {
                            throw new Exception("The series continuation could not be found.");
                        }
                    }
                }   
            }
            // if there is a previousRow, then generate fillers between the previous and current.
            else
            {
                var currentSeriesValue = (IComparable)_seriesMapping.SeriesValue(false, currentRow);
                var fillSeriesValue = (IComparable) _seriesMapping.SeriesValue(true, previousRow);
                var fillCount = 0;
                while (Operations.LessThan(fillSeriesValue, currentSeriesValue))
                {
                    await CreateFillerRow(currentRow, fillSeriesValue, false, cancellationToken);
                    fillSeriesValue = (IComparable) _seriesMapping.CalculateNextValue(fillSeriesValue);
                    hasFilled = true;

                    if (fillCount++ > 10000)
                    {
                        throw new Exception("The series continuation could not be found.");
                    }
                }
            }

            return hasFilled;
        }

        /// <summary>
        /// Create a dummy filler row based on the currentRow key, and the seriesValue
        /// </summary>
        /// <param name="currentRow"></param>
        /// <param name="seriesValue"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task CreateFillerRow(object[] currentRow, IComparable seriesValue, bool forecast, CancellationToken cancellationToken)
        {
            // if the series is not greater then create a dummy one, and continue the loop
            var fillerRow = new object[PrimaryTransform.FieldCount];
            
            Mappings.CreateFillerRow(currentRow, fillerRow, seriesValue);
            
            var (_, ignore) = await Mappings.ProcessInputData(
                new FunctionVariables() {SeriesValue = seriesValue, Forecast = forecast},
                fillerRow, null, cancellationToken);

            if (ignore)
            {
                TransformRowsIgnored += 1;
            }
            else
            {
                await ProcessAggregate(seriesValue, cancellationToken);
            }
        }

        private async Task<(IComparable seriesValue, object[] row)> ProcessGroupChange((IComparable seriesValue, object[] row) outputRow, object[] previousRow, CancellationToken cancellationToken)
        {
            // if the group has changed, update all cached rows with aggregate functions.
            if (_cachedRows != null)
            {
                // if there is not series finish the fillers may not have run from the Start to the current
                var seriesValue = (IComparable)_seriesMapping.SeriesValue(false, previousRow);
                await ProcessAggregate(seriesValue, cancellationToken);

                if (_seriesMapping.SeriesFill)
                {
                    await ProcessFillers(null, previousRow, cancellationToken);
                }

                Mappings.Reset(EFunctionType.Aggregate);

                var cacheIndex = 0;
                // process the series functions.
                foreach (var row in _cachedRows)
                {
                    var (_, ignore) = await Mappings.ProcessAggregateRow(new FunctionVariables() {Index = cacheIndex++}, row.row, EFunctionType.Series, cancellationToken);
                    if (ignore)
                    {
                        TransformRowsIgnored += 1;
                    }
                }

                if (_seriesStart != null)
                {
                    // remove cached items that start before the start date.
                    while (_cachedRows.Count > 0 && _seriesStart.CompareTo((IComparable) _cachedRows.Peek().seriesValue) > 0)
                    {
                        _cachedRows.Dequeue();
                    }
                }

                if (_cachedRows.Count > 0)
                {
                    outputRow = _cachedRows.Dequeue();
                    if (_seriesFinish != null && Operations.GreaterThan(outputRow.seriesValue, _seriesFinish))
                    {
                        outputRow = (null, null);
                        _cachedRows.Clear();
                    }
                }
                else
                {
                    outputRow = (null, null);
                }
            }
            else
            {
                outputRow = (null, null);
            }
            
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
