using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions.Exceptions;

namespace dexih.functions.BuiltIn
{
    public class IOFunctions
    {
        public enum EDuplicateFile
        {
            MakeUnique,
            Overwrite,
            Ignore,
            Abend
        }
        
        public enum EMissingFile
        {
            Ignore,
            Abend
        }
        
        public GlobalVariables GlobalVariables { get; set; }

        private string GetFileNameUnique(string filePrefix, string fileNameBody, string filePostfix, string extension,
            int uniqueCount = 0)
        {
            string unique = "";
            if (uniqueCount > 0)
            {
                unique = "_" + uniqueCount.ToString();
            }

            string fileName;
            
            if (string.IsNullOrEmpty(filePrefix) || string.IsNullOrEmpty(filePostfix) ||
                string.IsNullOrEmpty(fileNameBody))
            {
                fileName = string.Concat(filePrefix, fileNameBody, filePostfix, unique);
            }
            else
            {
                // create a random file name
                fileName = string.Concat("dexih_", DateTime.Now.Ticks.ToString("x"));
            }
            
            if (!string.IsNullOrEmpty(extension))
            {
                fileName = string.Concat(fileName, ".", extension);
            }

            return fileName;
        }

        /// <summary>
        /// Gets a unique filepath and checks permissions that user can write to the path.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="filePrefix"></param>
        /// <param name="fileNameBody"></param>
        /// <param name="filePostfix"></param>
        /// <param name="extension"></param>
        /// <param name="duplicateFileOptions"></param>
        /// <returns></returns>
        /// <exception cref="FunctionException"></exception>
        private string GetWriteFileName(string path, string filePrefix, string fileNameBody, string filePostfix, string extension, EDuplicateFile duplicateFileOptions)
        {
            GlobalVariables.FilePermissions?.ValidatePath(path);
            
            var fileName = GetFileNameUnique(filePrefix, fileNameBody, filePostfix, extension);
            var filePath = Path.Combine(path, fileName);
            
            GlobalVariables.FilePermissions?.ValidatePath(filePath);

            if (duplicateFileOptions == EDuplicateFile.Overwrite)
            {
                return fileName;
            }
            
            if (System.IO.File.Exists(filePath))
            {
                switch (duplicateFileOptions)
                {
                    case EDuplicateFile.Abend:
                        throw new FunctionException($"The file {filePath} already exists, and the duplicate file settings are set to abend.");
                    case EDuplicateFile.Ignore:
                        return null;
                    case EDuplicateFile.MakeUnique:
                        var uniqueCount = 1;
                        while (true)
                        {
                            fileName = GetFileNameUnique(filePrefix, fileNameBody, filePostfix, extension, uniqueCount);
                            filePath = Path.Combine(path, fileName);
                            uniqueCount++;
                            if(System.IO.File.Exists(filePath)) continue;

                            // safety check.
                            if (uniqueCount > 1000000)
                            {
                                throw new FunctionException($"The file {filePath} contains too many unique values.");
                            }
                            return fileName;
                        }  
                }
            }

            return fileName;
        }
        
        [TransformFunction(
            FunctionType = EFunctionType.Map, Category = "File Operations", Name = "Save to Binary File",
            Description = "Saves the binary data to a local file.  Returns the filename.")]
        public async Task<string> SaveBinaryFile(byte[] fileData, string path, string filePrefix, string fileNameBody, string filePostfix, string extension, EDuplicateFile duplicateFileOption)
        {
            if (fileData == null || fileData.Length == 0)
            {
                return null;
            }
            
            var fileName = GetWriteFileName(path, filePrefix, fileNameBody, filePostfix, extension, duplicateFileOption);

            if (fileName != null)
            {
                var filePath = Path.Combine(path, fileName);
                await System.IO.File.WriteAllBytesAsync(filePath, fileData);
                return fileName;
            }
            else
            {
                return null;
            }
        }
        
        [TransformFunction(
            FunctionType = EFunctionType.Map, Category = "File Operations", Name = "Save to Text File",
            Description = "Saves the text data to a local file.  Returns the filename.")]
        public async Task<string> SaveTextFile(string fileData, string path, string filePrefix, string fileNameBody, string filePostfix, string extension, EDuplicateFile duplicateFileOption)
        {
            if (string.IsNullOrEmpty(fileData))
            {
                return null;
            }
            
            var fileName = GetWriteFileName(path, filePrefix, fileNameBody, filePostfix, extension, duplicateFileOption);

            if (fileName != null)
            {
                var filePath = Path.Combine(path, fileName);
                await System.IO.File.WriteAllTextAsync(filePath, fileData);
                return fileName;
            }
            else
            {
                return null;
            }
        }

        private string GetReadFileName(string path, string filePrefix, string fileNameBody, string filePostfix,
            string extension, EMissingFile missingFileOption)
        {
            GlobalVariables.FilePermissions?.ValidatePath(path);

            var fileName = GetFileNameUnique(filePrefix, fileNameBody, filePostfix, extension);
            var filePath = Path.Combine(path, fileName);

            GlobalVariables.FilePermissions?.ValidatePath(filePath);

            if (System.IO.File.Exists(filePath))
            {
                return fileName;
            }
            else
            {
                if (missingFileOption == EMissingFile.Abend)
                {
                    throw new FunctionException($"File file {filePath} could not be found.");
                }
                else
                {
                    return null;
                }
            }
            
        }

        [TransformFunction(
            FunctionType = EFunctionType.Map, Category = "File Operations", Name = "Read from Binary File",
            Description = "Reads a local binary file.  Returns binary data if file exists.")]
        public async Task<byte[]> ReadBinaryFile(string path, string filePrefix, string fileNameBody, string filePostfix, string extension, EMissingFile missingFileOption)
        {
            var fileName = GetReadFileName(path, filePrefix, fileNameBody, filePostfix, extension, missingFileOption);

            if (fileName != null)
            {
                var filePath = Path.Combine(path, fileName);
                var data = await System.IO.File.ReadAllBytesAsync(filePath);
                return data;
            }
            else
            {
                return null;
            }
        }
        
        [TransformFunction(
            FunctionType = EFunctionType.Map, Category = "File Operations", Name = "Read from Text File",
            Description = "Reads a local text file.  Returns text data is file exists.")]
        public async Task<string> ReadTextFile(string path, string filePrefix, string fileNameBody, string filePostfix, string extension, EMissingFile missingFileOption)
        {
            var fileName = GetReadFileName(path, filePrefix, fileNameBody, filePostfix, extension, missingFileOption);

            if (fileName != null)
            {
                var filePath = Path.Combine(path, fileName);
                var data = await System.IO.File.ReadAllTextAsync(filePath);
                return data;
            }
            else
            {
                return null;
            }
        }
        
        [TransformFunction(
            FunctionType = EFunctionType.Map, Category = "File Operations", Name = "Get List of Files in a path",
            Description = "Gets a list of files in the specified path.")]
        public string[] GetFileList(string path, string searchPattern, SearchOption searchOption, bool includePath)
        {
            GlobalVariables.FilePermissions?.ValidatePath(path);

            if (Directory.Exists(path))
            {
                if (includePath)
                {
                    return Directory.GetFiles(path, searchPattern, searchOption);
                }
                else
                {
                    var skipDirectory = path.Length;

                    // because we don't want it to be prefixed by a slash
                    // if dirPath like "C:\MyFolder", rather than "C:\MyFolder\"
                    if(!path.EndsWith("" + Path.DirectorySeparatorChar)) skipDirectory++;
                    return Directory.GetFiles(path, searchPattern, searchOption).Select(c => c.Substring(skipDirectory)).ToArray();
                }
            }
            return null;
        }

        private IEnumerator<string> _fileEnumerator;

        public void FileListInfoReset()
        {
            _fileEnumerator = null;
        }

        [TransformFunction(
            FunctionType = EFunctionType.Rows, Category = "File Operations", Name = "Get List of Files in a path",
            Description = "Gets a list of files in the specified path.", ResetMethod = nameof(FileListInfoReset))]
        public bool GetFileListInfo(string path, string searchPattern, SearchOption searchOption, out string fileName, out string filePath, out DateTime creationTime, out DateTime lastAccessTime, out DateTime lastWriteTime, out long fileSize, out string fullName, out string extension, out bool isReadonly )
        {
            if (_fileEnumerator == null)
            {
                GlobalVariables.FilePermissions?.ValidatePath(path);

                if (Directory.Exists(path))
                {
                    _fileEnumerator = Directory.EnumerateFiles(path, searchPattern, searchOption).GetEnumerator();
                }
                
            }

            if (_fileEnumerator != null && _fileEnumerator.MoveNext())
            {
                var info = new System.IO.FileInfo(_fileEnumerator.Current);

                filePath = _fileEnumerator.Current;
                fileName = info.Name;
                creationTime = info.CreationTime;
                lastAccessTime = info.LastAccessTime;
                lastWriteTime = info.LastWriteTime;
                fileSize = info.Length;
                fullName = info.FullName;
                extension = info.Extension;
                isReadonly = info.IsReadOnly;
                return true;
            }
            else
            {
                filePath = null;
                fileName = null;
                creationTime = default;
                lastAccessTime = default;
                lastWriteTime = default;
                fileSize = 0;
                fullName = null;
                extension = null;
                isReadonly = false;
                return false;
            }
        }
        
        [TransformFunction(
            FunctionType = EFunctionType.Map, Category = "File Operations", Name = "File Information",
            Description = "Gets information about the specified file, return false if the file does not exist.")]
        public bool GetFileInformation(string path, string fileName, out DateTime creationTime, out DateTime lastAccessTime, out DateTime lastWriteTime, out long fileSize, out string fullName, out string extension, out bool isReadonly)
        {
            var filePath = Path.Combine(path, fileName);
            GlobalVariables.FilePermissions?.ValidatePath(filePath);

            if (System.IO.File.Exists(filePath))
            {
                var info = new System.IO.FileInfo(path);

                creationTime = info.CreationTime;
                lastAccessTime = info.LastAccessTime;
                lastWriteTime = info.LastWriteTime;
                fileSize = info.Length;
                fullName = info.FullName;
                extension = info.Extension;
                isReadonly = info.IsReadOnly;
                return true;
            }
            else
            {
                creationTime = default;
                lastAccessTime = default;
                lastWriteTime = default;
                fileSize = 0;
                fullName = null;
                extension = null;
                isReadonly = false;
                return false;
            }

        }
    }
}