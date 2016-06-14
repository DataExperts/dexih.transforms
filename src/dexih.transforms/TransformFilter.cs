using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;

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

        public override ReturnValue Open(List<Filter> filters = null, List<Sort> sorts = null)
        {
            if (filters == null)
                filters = new List<Filter>();

            //add any of the conditions that can be translated to filters
            foreach (var condition in Conditions)
            {
                var filter = Filter.GetFilterFromFunction(condition);
                if(filter.Success)
                {
                    filter.Value.AndOr = Filter.EAndOr.And;
                    filters.Add(filter.Value);
                }
            }
            var returnValue = PrimaryTransform.Open(filters, sorts);
            return returnValue;
        }

        protected override ReturnValue<object[]> ReadRecord()
        {
            if (PrimaryTransform.Read() == false)
                return new ReturnValue<object[]>(false, null);
            bool showRecord;
            do //loop through the records util the filter is true
            {
                showRecord = true;
                if (Conditions != null)
                {
                    foreach (Function condition in Conditions)
                    {
                        foreach (Parameter input in condition.Inputs.Where(c => c.IsColumn))
                        {
                            var result = input.SetValue(PrimaryTransform[input.ColumnName]);
                            if (result.Success == false)
                                throw new Exception("Error setting condition values: " + result.Message);
                        }

                        var invokeresult = condition.Invoke();
                        if(invokeresult.Success == false)
                            throw new Exception("Error invoking condition function: " + invokeresult.Message);

                        if ((bool)invokeresult.Value == false)
                        {
                            showRecord = false;
                            break;
                        }
                    }
                }

                if (showRecord) break;
            } while (PrimaryTransform.Read());

            object[] newRow;

            if (showRecord)
            {
                newRow = new object[FieldCount];
                for (int i = 0; i < FieldCount; i++)
                    newRow[i] = PrimaryTransform[i];
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
