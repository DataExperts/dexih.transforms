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

        public bool SetConditions(List<Function> conditions)
        {
            Conditions = conditions;
            return true;
        }

        public override bool Initialize()
        {
            CachedTable = Reader.CachedTable.Copy();
            CachedTable.TableName = "Filter";
            CachedTable.OutputSortFields = Reader.CachedTable.OutputSortFields;

            return true;
        }

        /// <summary>
        /// checks if filter can execute against the database query.
        /// </summary>
        public override bool CanRunQueries
        {
            get
            {
                return Conditions.Exists(c => c.CanRunSql == false) && Reader.CanRunQueries;
            }
        }

        public override bool PrefersSort => false;
        public override bool RequiresSort => false;

        protected override bool ReadRecord()
        {
            if (Reader.Read() == false)
                return false;
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
                            var result = input.SetValue(Reader[input.ColumnName]);
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
            } while (Reader.Read());

            if (showRecord)
            {
                CurrentRow = new object[FieldCount];
                for (int i = 0; i < FieldCount; i++)
                    CurrentRow[i] = Reader[i];
            }
            else
                CurrentRow = null;

            return showRecord;
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

        public override List<Sort> RequiredJoinSortFields()
        {
            return null;
        }

    }
}
