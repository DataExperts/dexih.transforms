using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using dexih.functions.Query;

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

        public abstract Task SetStream(Stream stream, SelectQuery selectQuery);

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

        public virtual void Dispose()
        {
        }
    }
}