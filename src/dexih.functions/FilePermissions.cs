using System;
using System.IO;

namespace dexih.functions
{
    public class ForbiddenPathException : Exception
    {
        public string Path { get; }
        
        public ForbiddenPathException(string message, string path) : base(message)
        {
            Path = path;
        }
    }
    
    public class FilePermissions
    {
        /// <summary>
        /// Allow agent to read/write files to the local filesystem
        /// </summary>
        public bool AllowLocalFiles { get; set; } = true;

        /// <summary>
        /// Allow agent to access files anywhere.
        /// </summary>
        public bool AllowAllPaths { get; set; } = false;
        
        /// <summary>
        /// If AllowAllPaths = false, a list of the file paths the remote agent can access.
        /// </summary>
        public string[] AllowedPaths { get; set; }
        
       /// <summary>
        /// Applies checks on the path to ensure files can not be written to unauthorized locations
        /// </summary>
        /// <param name="path"></param>
        /// <exception cref="ForbiddenPathException"></exception>
        public void ValidatePath(string path)
        {
            if (!AllowLocalFiles)
            {
                throw new ForbiddenPathException($"Local file access is forbidden.", path);
            }
            
            if (string.IsNullOrEmpty(path))
            {
                throw new ForbiddenPathException($"An empty path is forbidden.  Path must start with a '/' or 'c:\\'.'.", path);
            }
            
            //check the path starts with a / or \ or c:\
            if (!(path.StartsWith("/") || path.StartsWith("\\") || (char.IsLetter(path[0]) && path[1] == ':')))
            {
                throw new ForbiddenPathException($"The path {path} is invalid as it does not start with a '/' or 'c:\\'.", path);
            }

            if (path.IndexOf("..", StringComparison.Ordinal) >= 0)
            {
                throw new ForbiddenPathException($"The path {path} is invalid as it contains a '..'.", path);
            }

            if (path.IndexOf("~", StringComparison.Ordinal) >= 0)
            {
                throw new ForbiddenPathException($"The path {path} is invalid as it contains a '~'.", path);
            }

            var normailzePath = NormalizePath(path);

            if (normailzePath.StartsWith(Directory.GetCurrentDirectory()))
            {
                throw new ForbiddenPathException($"The path {path} is invalid as it contains the remote agent binaries.", path);
            }

            if (AllowAllPaths)
            {
                return;
            }

            if (AllowedPaths == null || AllowedPaths.Length == 0)
            {
                throw new ForbiddenPathException($"The path cannot be validated as there are no allowed paths specified in the appsettings.json on the remote agent.", path);
            }

            foreach (var allowedPath in AllowedPaths)
            {
                if (normailzePath.StartsWith(NormalizePath(allowedPath)))
                {
                    return;
                }
            }
            
            throw new ForbiddenPathException($"The path {path} is forbidden as it is not within allowed paths.  To include this path, modify the AllowedFileDirectories section of the appsettings.json file on the remote agent.", path);
        }
        
        public static string NormalizePath(string path)
        {
            return Path.GetFullPath(new Uri(path).LocalPath)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .ToUpperInvariant();
        }
    }
}