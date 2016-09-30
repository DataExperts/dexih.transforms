using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;

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
            CacheTable.TableName = "Filter";
            CacheTable.OutputSortFields = PrimaryTransform.CacheTable.OutputSortFields;

            return true;
        }

        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query)
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
                if(filter.Success)
                {
                    filter.Value.AndOr = Filter.EAndOr.And;
                    query.Filters.Add(filter.Value);
                }
            }
            var returnValue = await PrimaryTransform.Open(auditKey, query);
            return returnValue;
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            if (await PrimaryTransform.ReadAsync(cancellationToken)== false)
                return new ReturnValue<object[]>(false, null);

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
                            var result = input.SetValue(PrimaryTransform[input.Column.SchemaColumnName()]);
                            if (result.Success == false)
                                throw new Exception("Error setting condition values: " + result.Message);
                        }

                        var invokeresult = condition.Invoke();
                        if (invokeresult.Success == false)
                            throw new Exception("Error invoking condition function: " + invokeresult.Message);

                        if ((bool)invokeresult.Value == false)
                        {
                            showRecord = false;
                            break;
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
                //for (int i = 0; i < FieldCount; i++)
                //    newRow[i] = PrimaryTransform[i];
            }
            else
                newRow = null;

            return new ReturnValue<object[]>(showRecord, newRow);
        }

        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true); // nothing to reset.
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
