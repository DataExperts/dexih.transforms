using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.CopyProperties;

namespace dexih.transforms
{
    [Transform(
        Name = "Rows",
        Description = "Groups columns, generates rows, and can un-group column nodes.",
        TransformType = TransformAttribute.ETransformType.Rows
    )]
    public class TransformRows : Transform
    {
        public TransformRows() { }

        public TransformRows(Transform inTransform, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inTransform, null);
        }

        private bool _firstRecord;
        
        public override string TransformName { get; } = "Rows";
        public override string TransformDetails => "Columns:" + Mappings.OfType<MapGroup>().Count() + ", Functions: " + Mappings.OfType<MapAggregate>().Count();


        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;
            
            if (selectQuery == null)
            {
                selectQuery = new SelectQuery();
            }
            else
            {
                selectQuery = selectQuery.CloneProperties<SelectQuery>(true);
            }

            var groupFields = Mappings.OfType<MapGroup>().ToArray();
            
            // pass through sorts where the column is part of the group field
            if (selectQuery.Sorts != null && selectQuery.Sorts.Count > 0)
            {
                if (groupFields.Any())
                {
                    var groupNames = groupFields.Select(c => c.InputColumn.Name).ToArray();
                    selectQuery.Sorts = selectQuery.Sorts.Where(c => c.Column != null && groupNames.Contains(c.Column.Name)).ToList();
                }
                else
                {
                    selectQuery.Sorts = null;
                }
            }
            
            // pass through filters where the columns are part of the group fields.
            if (selectQuery.Filters != null && selectQuery.Filters.Count > 0)
            {
                if (groupFields.Any())
                {
                    var groupNames = groupFields.Select(c => c.InputColumn.Name).ToArray();
                    selectQuery.Filters = selectQuery.Filters.Where(c =>
                        (c.Column1 != null && groupNames.Contains(c.Column1.Name)) && 
                        (c.Column2 != null &&  groupNames.Contains((c.Column2.Name)))).ToList();
                }
                else
                {
                    selectQuery.Filters = null;
                }
            }            

            var returnValue = await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);
            _firstRecord = true;
            return returnValue;
        }

        public override bool ResetTransform()
        {
            Mappings.Reset(EFunctionType.Rows);
            _firstRecord = true;
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            var firstRead = false;
            if (_firstRecord)
            {
                if(!await PrimaryTransform.ReadAsync(cancellationToken))
                {
                    return null;
                }

                firstRead = true;
                _firstRecord = false;
            }

            do
            {
                // if the row generation function returns true, then add the row
                var (showRow, ignoreRow) =
                    await Mappings.ProcessInputData(PrimaryTransform.CurrentRow, cancellationToken);

                if (ignoreRow)
                {
                    TransformRowsIgnored += 1;
                }
                else if (showRow || firstRead)
                {
                    var newRow = new object[FieldCount];
                    Mappings.MapOutputRow(newRow);
                    return newRow;
                }

                if (!await PrimaryTransform.ReadAsync(cancellationToken))
                {
                    return null;
                }

                firstRead = true;

            } while (true);

        }


        public override List<Sort> RequiredSortFields()
        {
            // return GroupFields.Select(c=> new Sort { Column = c.SourceColumn, Direction = Sort.EDirection.Ascending }).ToList();
            return null;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
