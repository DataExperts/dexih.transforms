using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Mappings;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    [Transform(
        Name = "Rows",
        Description = "Groups columns and generates rows.",
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

        public override bool InitializeOutputFields()
        {
            CacheTable = Mappings.Initialize(PrimaryTransform.CacheTable);
            _firstRecord = true;
            return true;
        }


        public override Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            if (query == null)
            {
                query = new SelectQuery();
            }

            var groupFields = Mappings.OfType<MapGroup>().ToArray();
            
            // pass through sorts where the column is part of the group field
            if (query.Sorts != null && query.Sorts.Count > 0)
            {
                if (groupFields.Any())
                {
                    var groupNames = groupFields.Select(c => c.InputColumn.Name).ToArray();
                    query.Sorts = query.Sorts.Where(c => c.Column != null && groupNames.Contains(c.Column.Name)).ToList();
                }
                else
                {
                    query.Sorts = null;
                }
            }
            
            // pass through filters where the columns are part of the group fields.
            if (query.Filters != null && query.Filters.Count > 0)
            {
                if (groupFields.Any())
                {
                    var groupNames = groupFields.Select(c => c.InputColumn.Name).ToArray();
                    query.Filters = query.Filters.Where(c =>
                        (c.Column1 != null && groupNames.Contains(c.Column1.Name)) && 
                        (c.Column2 != null &&  groupNames.Contains((c.Column2.Name)))).ToList();
                }
                else
                {
                    query.Filters = null;
                }
            }            

            var returnValue = PrimaryTransform.Open(auditKey, query, cancellationToken);
            return returnValue;
        }

        public override bool ResetTransform()
        {
            Mappings.Reset(EFunctionType.Rows);
            _firstRecord = true;
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if (_firstRecord)
            {
                if(!await PrimaryTransform.ReadAsync(cancellationToken))
                {
                    return null;
                }

                _firstRecord = false;
            }

            do
            {
                // if the row generation function returns true, then add the row
                if (Mappings.ProcessInputData(PrimaryTransform.CurrentRow))
                {
                    var newRow = new object[FieldCount];
                    Mappings.ProcessOutputRow(newRow);
                    return newRow;
                }
                else
                {
                    if (!await PrimaryTransform.ReadAsync(cancellationToken))
                    {
                        return null;
                    }
                }
            } while (true);

        }

        public override string Details()
        {
            return "Row Transform";
            // return "Row Transform: " + (Mappings.PassThroughColumns ? "All columns passed through, " : "") + "Grouped Columns:" + (GroupFields?.Count.ToString() ?? "Nill") + ", Series/Aggregate Functions:" + (Functions?.Count.ToString() ?? "Nill");
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
