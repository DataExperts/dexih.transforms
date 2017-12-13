using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions.File
{
    public abstract class FileHandlerBase : IDisposable
    {
        /// <summary>
        /// Infers the source columns from the table.sample data
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public abstract Task<ICollection<TableColumn>> GetSourceColumns(Stream stream);

        public abstract Task SetStream(Stream stream, ICollection<Filter> filters);

        public abstract Task<object[]> GetRow(object[] baseRow);

        public async Task<ICollection<object[]>> GetAllRows(object[] baseRow)
        {
            var rows = new List<object[]>();

            var row = await GetRow(baseRow);

            while (row != null)
            {
                rows.Add(row);
                row = await GetRow(baseRow);
            }

            return rows;
        }

        /// <summary>
        /// Tests is a row should be filtered based on the filters provided.  
        /// </summary>
        /// <param name="row"></param>
        /// <param name="filters"></param>
        /// <param name="table"></param>
        /// <returns>true = don't filter, false = filtered</returns>
        protected bool EvaluateRowFilter(IReadOnlyList<object> row, ICollection<Filter> filters, Table table)
        {
            if (filters != null && filters.Count > 0)
            {
                var filterResult = true;
                var isFirst = true;

                foreach (var filter in filters)
                {
                    var column1Value = filter.Column1 == null
                        ? null
                        : row[table.GetOrdinal(filter.Column1.Name)];
                    var column2Value = filter.Column2 == null
                        ? null
                        : row[table.GetOrdinal(filter.Column2.Name)];

                    if (isFirst)
                    {
                        filterResult = filter.Evaluate(column1Value, column2Value);
                        isFirst = false;
                    }
                    else if (filter.AndOr == Filter.EAndOr.And)
                    {
                        filterResult = filterResult && filter.Evaluate(column1Value, column2Value);
                    }
                    else
                    {
                        filterResult = filterResult || filter.Evaluate(column1Value, column2Value);
                    }
                }

                return filterResult;
            }
            else
            {
                return true;
            }
        }

        public virtual void Dispose()
        {
        }
    }
}