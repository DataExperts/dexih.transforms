using System;

namespace dexih.connections.flatfile
{
    public class ForbiddenPathException : Exception
    {
        public string Path { get; }
        
        public ForbiddenPathException(string message, string path) : base(message)
        {
            Path = path;
        }
      
        
    }
    
}