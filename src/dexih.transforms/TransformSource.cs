using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;

namespace dexih.transforms
{
    /// <summary>
    /// TransformSource is a starting point in a chain of transforms.  This requires a DbDataReader to be inintialized and passed to it.
    /// </summary>
    public class TransformSource : Transform
    {
        public TransformSource() { }

        /// <summary>
        /// Initialises a transform source.  
        /// </summary>
        /// <param name="inReader">An initialized DbDataReader.</param>
        /// <param name="sortFields">A list of already sorted fields in the inReader.</param>
        public TransformSource(DbDataReader inReader, List<Sort> sortFields = null)
        {
            InReader = InReader;
            SortFields = sortFields;
        }

        public DbDataReader InReader { get; set; }

        protected Dictionary<string, object[]> LookupCache;

        public override bool CanRunQueries
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override int FieldCount
        {
            get
            {
                return InReader.FieldCount;
            }
        }

        public override bool PrefersSort { get; } = false;
        public override bool RequiresSort { get; } = false;

        public override string Details()
        {
            return "DataSource";
        }

        public override string GetName(int i)
        {
            return InReader.GetName(i);
        }

        public override int GetOrdinal(string columnName)
        {
            return InReader.GetOrdinal(columnName);
        }

        public override bool Initialize()
        {
            return true;
        }

        public override Task<ReturnValue> LookupRow(List<Filter> filters)
        {
            throw new NotImplementedException();
        }

        public override List<Sort> OutputSortFields()
        {
            throw new NotImplementedException();
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            return new List<Sort>();
        }

        public override List<Sort> RequiredSortFields()
        {
            return new List<Sort>();
        }

        public override bool ResetValues()
        {
            return true;
        }

        protected override bool ReadRecord()
        {
            return InReader.Read();
        }
    }
}
