using dexih.functions;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace dexih.connections
{
    
    public class FileFormat
    {
        public FileFormat()
        {
            SetDefaults();
        }

        private void SetDefaults()
        {
            BufferSize = DefaultBufferSize;
            Delimiter = DefaultDelimiter;
            Escape = DefaultEscape;
            Comment = DefaultComment;
        }

        private int SubscriptionKey { get; set; }

        public const int DefaultBufferSize = 0x1000;
        public const string DefaultDelimiter = ",";
        public const string DefaultEscape = "\"";
        public const string DefaultComment = "#";


        public enum EParseErrorAction
        {
            RaiseEvent = 0,
            AdvanceToNextLine = 1,
            ThrowException = 2,
        }

        public enum EMissingFieldAction
        {
            ParseError = 0,
            ReplaceByEmpty = 1,
            ReplaceByNull = 2,
        }

        public enum EValueTrimmingOptions
        {
            None = 0,
            UnquotedOnly = 1,
            QuotedOnly = 2,
            All = UnquotedOnly | QuotedOnly
        }

        public int BufferSize { get; set; }

        public string Delimiter { get; set; }

        public string Quote { get; set; }

        public string Escape { get; set; }

        public string Comment { get; set; }

        public bool Headers { get; set; }

        public EParseErrorAction ParseErrorAction {get;set;}

        public EMissingFieldAction MissingFieldAction { get; set; }

        public bool SupportsMutiline { get; set; }

        public bool SkipEmptyLines { get; set; }

        public EValueTrimmingOptions ValueTrimmingOptions { get; set; }


    }

}
