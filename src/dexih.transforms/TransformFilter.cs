using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{
    public class TransformFilter : Transform
    {

        public TransformFilter() { }

        public TransformFilter(Transform inReader, List<Function> conditions)
        {
            Conditions = conditions;
            SetInTransform(inReader);
        }

        public List<Function> Conditions
        {
            get
            {
                return Functions;
            }
            set
            {
                Functions = value;
            }
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

        public override async Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
        {
            AuditKey = auditKey;

            if (query == null)
                query = new SelectQuery();

            if (query.Filters == null)
                query.Filters = new List<Filter>();

            //add any of the conditions that can be translated to filters
            foreach (var condition in Conditions)
            {
                var filter = Filter.GetFilterFromFunction(condition);
                if(filter != null)
                {
                    filter.AndOr = Filter.EAndOr.And;
                    query.Filters.Add(filter);
                }
            }
            var returnValue = await PrimaryTransform.Open(auditKey, query, cancelToken);
            return returnValue;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if (await PrimaryTransform.ReadAsync(cancellationToken) == false)
                return null;

            bool showRecord = true;
            if (Conditions != null && Conditions.Count > 0)
            {
                do //loop through the records util the filter is true
                {
                    showRecord = true;
                    foreach (Function condition in Conditions)
                    {
                        foreach (Parameter input in condition.Inputs.Where(c => c.IsColumn))
                        {
                            try
                            {
                                input.SetValue(PrimaryTransform[input.Column.SchemaColumnName()]);
                            }
                            catch(Exception ex)
                            {
                                throw new TransformException($"The filter failed as the column {input.Column.SchemaColumnName()} has incompatible data values.  {ex.Message}.", ex, PrimaryTransform[input.Column.SchemaColumnName()]);
                            }
                        }

                        try
                        {
                            var invokeresult = condition.Invoke();

                            if ((bool)invokeresult == false)
                            {
                                showRecord = false;
                                break;
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new TransformException($"The filter could not run the condition {condition.FunctionName} failed.  {ex.Message}.", ex);
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
