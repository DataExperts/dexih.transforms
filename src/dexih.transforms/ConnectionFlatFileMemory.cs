using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.File;

namespace dexih.transforms
{
    public class ConnectionFlatFileMemory: ConnectionFlatFile
    {
        private readonly Dictionary<(EFlatFilePath path, string fileName), (FlatFile flatfile, Stream stream)> _files = new Dictionary<(EFlatFilePath path, string fileName), (FlatFile flatfile, Stream stream)>();
        
        public override string GetFullPath(FlatFile file, EFlatFilePath path) => "";
        
        public override Task<List<string>> GetFileShares(CancellationToken cancellationToken) => Task.FromResult(new List<string>());

        public override Task<bool> CreateDirectory(FlatFile file, EFlatFilePath path, CancellationToken cancellationToken) => Task.FromResult(true);

        public override Task<bool> MoveFile(FlatFile file, EFlatFilePath fromDirectory, EFlatFilePath toDirectory,
            string fileName, CancellationToken cancellationToken)
        {
            if (_files.TryGetValue((fromDirectory, fileName), out var stream))
            {
                _files.Remove((fromDirectory, fileName));
                _files.Add((toDirectory, fileName), stream);
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
            
        }

        public override Task<bool> DeleteFile(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
        {
            if (_files.ContainsKey((path, fileName)))
            {
                _files.Remove((path, fileName));
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }

        public override async IAsyncEnumerable<FileProperties> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var files = _files.Where(c => c.Key.path == path).Select(c => c.Key.fileName);
            foreach (var fileName in files)
            {
                yield return await Task.FromResult(new FileProperties()
                {
                    FileName = fileName,
                });
            }
        }

        public override Task<Stream> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(_files[(path, fileName)].stream);
        }

        public override Task<Stream> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken) => throw new InvalidOperationException();

        public override Task<bool> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName,
            Stream stream, CancellationToken cancellationToken)
        {
            _files.Add((path, fileName), (file, stream));
            return Task.FromResult(true);
        }

        public override Task<bool> TestFileConnection(CancellationToken cancellationToken) => Task.FromResult(true);
        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken = default) => Task.FromResult(true);
    }
}