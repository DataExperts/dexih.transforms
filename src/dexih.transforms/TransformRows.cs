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
        TransformType = ETransformType.Rows
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

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;
            
            requestQuery = requestQuery?.CloneProperties() ?? new SelectQuery();
            
            // get only the required columns
            requestQuery.Columns = new SelectColumns(Mappings.GetRequiredColumns());


            var groupFields = Mappings.OfType<MapGroup>().ToArray();
            
            // pass through sorts where the column is part of the group field
            if (requestQuery.Sorts != null && requestQuery.Sorts.Count > 0)
            {
                if (groupFields.Any())
                {
                    var groupNames = groupFields.Select(c => c.InputColumn.Name).ToArray();
                    requestQuery.Sorts =
                        new Sorts(requestQuery.Sorts.Where(c => c.Column != null && groupNames.Contains(c.Column.Name)));
                }
                else
                {
                    requestQuery.Sorts = null;
                }
            }
            
            // pass through filters where the columns are part of the group fields.
            if (requestQuery.Filters != null && requestQuery.Filters.Count > 0)
            {
                if (groupFields.Any())
                {
                    var groupNames = groupFields.Select(c => c.InputColumn.Name).ToArray();
                    requestQuery.Filters = new Filters(requestQuery.Filters.Where(c =>
                        c.Column1 != null && groupNames.Contains(c.Column1.Name) && c.Column2 != null && groupNames.Contains(c.Column2.Name)));
                }
                else
                {
                    requestQuery.Filters = null;
                }
            }

            SetRequestQuery(requestQuery, true);

            var returnValue = await PrimaryTransform.Open(auditKey, requestQuery, cancellationToken);
            
            GeneratedQuery = new SelectQuery()
            {
                Sorts = PrimaryTransform.SortFields,
                Filters = PrimaryTransform.Filters
            };
            
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
                if (firstRead)
                {
                    Mappings.Reset(EFunctionType.Rows);
                }
                
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


        public override Sorts RequiredSortFields()
        {
            // return GroupFields.Select(c=> new Sort { Column = c.SourceColumn, Direction = EDirection.Ascending }).ToList();
            return null;
        }

        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
