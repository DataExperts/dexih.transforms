using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms.File
{
    public abstract class FileHandlerBase : IDisposable
    {
        public abstract string FileType { get; }
        
        /// <summary>
        /// Infers the source columns from the table.sample data
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public abstract Task<ICollection<TableColumn>> GetSourceColumns(Stream stream);

        public abstract Task SetStream(Stream stream, SelectQuery selectQuery);

        public abstract Task<object[]> GetRow();

        public async Task<ICollection<object[]>> GetAllRows()
        {
            var rows = new List<object[]>();

            var row = await GetRow();

            while (row != null)
            {
                rows.Add(row);
                row = await GetRow();
            }

            return rows;
        }

        public virtual void Dispose()
        {
        }
    }
}