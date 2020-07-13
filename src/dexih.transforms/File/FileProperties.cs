using System;
using System.Threading.Tasks;
using Dexih.Utils.MessageHelpers;

namespace dexih.transforms.File
{
    public class FileProperties
    {
        public string FileName { get; set; }
        public virtual DateTime LastModified { get; set; }
        public virtual long Length { get; set; }
        public virtual string ContentType { get; set; }
        public string Owner { get; set; }

        /// <summary>
        /// Loads attributes for file connections which require additional lookups 
        /// </summary>
        public Func<Task> LoadAttributes { get; set; } = () => Task.CompletedTask;
    }
}
