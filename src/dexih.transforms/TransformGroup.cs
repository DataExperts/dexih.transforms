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
        Name = "Group",
        Description = "Group columns and apply specific aggregation rules to other columns.",
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
        
        public override bool RequiresSort => Mappings.OfType<MapGroup>().Any();

        public override string TransformName { get; } = "Group";
        public override string TransformDetails => ( Mappings.PassThroughColumns ? "All columns passed through, " : "") + "Columns:" + Mappings.OfType<MapGroup>().Count() + ", Functions: " + Mappings.OfType<MapAggregate>().Count();

        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            if (selectQuery == null)
            {
                selectQuery = new SelectQuery();
            }

            var requiredSorts = RequiredSortFields();

            if(selectQuery.Sorts != null && selectQuery.Sorts.Count > 0)
            {
                for(var i =0; i<requiredSorts.Count; i++)
                {
                    if (selectQuery.Sorts[i].Column == requiredSorts[i].Column)
                        requiredSorts[i].Direction = selectQuery.Sorts[i].Direction;
                    else
                        break;
                }
            }

            selectQuery.Sorts = requiredSorts;

            var returnValue = await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);
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
            var outputRow = new object[FieldCount];

            if (!_firstRecord && !_lastRecord)
            {
                await Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
            }

            // used to track if the group fields have changed
            var groupChanged = false;
            
            if (PrimaryTransform.IsReaderFinished || await PrimaryTransform.ReadAsync(cancellationToken) == false)
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
                    
                    //if it's the first record then the group values are being set for the first time.
                    if (_firstRecord)
                    {
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
                        await Mappings.ProcessInputData(PrimaryTransform.CurrentRow);
                    }
                    // when group has changed
                    else
                    {
                        await ProcessGroupChange(outputRow);
                        
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
                await ProcessGroupChange(outputRow);

                _lastRecord = true;
            }
            
            _firstRecord = false;
            return outputRow;

        }

        private async Task ProcessGroupChange(object[] outputRow)
        {
            Mappings.MapOutputRow(outputRow);
            await Mappings.ProcessAggregateRow(new FunctionVariables(), outputRow, EFunctionType.Aggregate);
            Mappings.Reset(EFunctionType.Aggregate);
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
