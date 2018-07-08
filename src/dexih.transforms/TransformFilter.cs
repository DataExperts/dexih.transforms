using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Transforms;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    [Transform(
        Name = "Filter",
        Description = "Filter incoming rows based on a set of conditions.",
        TransformType = TransformAttribute.ETransformType.Filter
    )]
    public class TransformFilter : Transform
    {

        public TransformFilter() { }

        public TransformFilter(Transform inReader, List<TransformFunction> conditions, List<FilterPair> filterPairs)
        {
            Conditions = conditions;
            FilterPairs = filterPairs;
            SetInTransform(inReader);
        }

        public List<TransformFunction> Conditions
        {
            get => Functions;
            set => Functions = value;
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = PrimaryTransform.CacheTable.Copy();
            CacheTable.Name = "Filter";
            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return true;
        }

        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;
        
        private int[] _filterColumn1Ordinals;
        private int[] _filterColumn2Ordinals;

        
        public override Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            if (query == null)
                query = new SelectQuery();

            if (query.Filters == null)
                query.Filters = new List<Filter>();

            //add any of the conditions that can be translated to filters
            if (Conditions != null)
            {
                foreach (var condition in Conditions)
                {
                    var filter = Filter.GetFilterFromFunction(condition);
                    if (filter != null)
                    {
                        filter.AndOr = Filter.EAndOr.And;
                        query.Filters.Add(filter);
                    }
                }
            }

            if (FilterPairs != null)
            {
                foreach (var filterPair in FilterPairs)
                {
                    if (filterPair.Column2 == null)
                    {
                        var filter = new Filter(filterPair.Column1, filterPair.Compare, filterPair.FilterValue);
                        query.Filters.Add(filter);
                    }
                    else
                    {
                        var filter = new Filter()
                        {
                            Column1 = filterPair.Column1,
                            Operator = filterPair.Compare,
                            Column2 = filterPair.Column2
                        };
                        query.Filters.Add(filter);
                    }
                }
            }

            //store the ordinals for the joins to improve performance.
            if (FilterPairs == null)
            {
                _filterColumn1Ordinals = new int[0];
                _filterColumn2Ordinals = new int[0];
            }
            else
            {
                _filterColumn1Ordinals = new int[FilterPairs.Count];
                _filterColumn2Ordinals = new int[FilterPairs.Count];

                for (var i = 0; i <  FilterPairs.Count; i++)
                {
                    _filterColumn1Ordinals[i] = FilterPairs[i].Column1 == null ? -1 : PrimaryTransform.GetOrdinal(FilterPairs[i].Column1.Name);
                    _filterColumn2Ordinals[i] = FilterPairs[i].Column2 == null ? -1 : PrimaryTransform.GetOrdinal(FilterPairs[i].Column2.Name);
                }
            }
            
            var returnValue = PrimaryTransform.Open(auditKey, query, cancellationToken);
            return returnValue;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if (await PrimaryTransform.ReadAsync(cancellationToken) == false)
                return null;

            var showRecord = true;
            
            if (Conditions?.Count > 0 || FilterPairs?.Count > 0)
            {
                do //loop through the records util the filter is true
                {
                    showRecord = true;

                    if (FilterPairs != null)
                    {
                        for(var i = 0; i < FilterPairs.Count; i++)
                        {
                            var value1 = PrimaryTransform[_filterColumn1Ordinals[i]];
                            var value2 = FilterPairs[i].Column2 == null ? FilterPairs[i].FilterValue : PrimaryTransform[_filterColumn2Ordinals[i]];

                            if (FilterPairs[i].Compare == Filter.ECompare.IsNotNull)
                            {
                                showRecord = value1 != null && !(value1 is DBNull);
                                continue;
                            }

                            if (FilterPairs[i].Compare == Filter.ECompare.IsNull)
                            {
                                showRecord = value1 == null || value1 is DBNull;
                                continue;
                            }

                            var compare = DataType.Compare(FilterPairs[i].Column1.DataType, value1, value2);

                            switch (FilterPairs[i].Compare)
                            {
                                case Filter.ECompare.GreaterThan:
                                    if (compare != DataType.ECompareResult.Greater)
                                    {
                                        showRecord = false;
                                    }
                                    break;
                                case Filter.ECompare.IsEqual:
                                    if (compare != DataType.ECompareResult.Equal)
                                    {
                                        showRecord = false;
                                    }
                                    break;
                                case Filter.ECompare.GreaterThanEqual:
                                    if (compare == DataType.ECompareResult.Less)
                                    {
                                        showRecord = false;
                                    }
                                    break;
                                case Filter.ECompare.LessThan:
                                    if (compare != DataType.ECompareResult.Less)
                                    {
                                        showRecord = false;
                                    }
                                    break;
                                case Filter.ECompare.LessThanEqual:
                                    if (compare == DataType.ECompareResult.Greater)
                                    {
                                        showRecord = false;
                                    }
                                    break;
                                case Filter.ECompare.NotEqual:
                                    if (compare == DataType.ECompareResult.Equal)
                                    {
                                        showRecord = false;
                                    }
                                    break;
                                default:
                                    throw new ArgumentOutOfRangeException();
                            }

                        }
                    }
                    
                    if (Conditions != null && showRecord)
                    {
                        foreach (var condition in Conditions)
                        {
                            foreach (var input in condition.Inputs.Where(c => c.IsColumn))
                            {
                                try
                                {
                                    input.SetValue(PrimaryTransform[input.Column]);
                                }
                                catch (Exception ex)
                                {
                                    throw new TransformException(
                                        $"The filter transform {Name} failed as the column {input.Column.TableColumnName()} has incompatible data values.  {ex.Message}.",
                                        ex, PrimaryTransform[input.Column.TableColumnName()]);
                                }
                            }

                            try
                            {
                                var invokeresult = condition.Invoke();

                                if ((bool) invokeresult == false)
                                {
                                    showRecord = false;
                                    break;
                                }
                            }
                            catch (FunctionIgnoreRowException)
                            {
                                TransformRowsIgnored++;
                                showRecord = false;
                                break;
                            }
                            catch (Exception ex)
                            {
                                throw new TransformException(
                                    $"The filter transform {Name} failed running the condition {condition.FunctionName}.  {ex.Message}.",
                                    ex);
                            }

                        }
                    }

                    if (showRecord) break;

                    TransformRowsFiltered += 1;

                } while (await PrimaryTransform.ReadAsync(cancellationToken));
            }

            object[] newRow;

            if (showRecord)
            {
                newRow = new object[FieldCount];
                PrimaryTransform.GetValues(newRow);
            }
            else
                newRow = null;

            return newRow;
        }

        public override bool ResetTransform()
        {
            return true;
        }

        public override string Details()
        {
            return "Filter: Number of conditions= " + Conditions?.Count ;
        }

        public override List<Sort> RequiredSortFields()
        {
            return null;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
