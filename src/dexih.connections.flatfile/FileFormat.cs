﻿﻿using dexih.functions;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.connections.flatfile
{ 
    
    public class FileFormat
    {
        public FileFormat()
        {
            SetDefaults();
        }

        public void SetDefaults()
        {
            BufferSize = DefaultBufferSize;
            Delimiter = DefaultDelimiter;
            Escape = DefaultEscape;
            Comment = DefaultComment;
            Headers = DefaultHeaders;
            MissingFieldAction = DefaultMissingFieldAction;
            ParseErrorAction = DefaultParseErrorAction;
            Quote = DefaultQuote;
            SkipBlankLines = DefaultSkipEmptyLines;
            SupportsMultiline = DefaultSupportsMutiline;
            ValueTrimmingOptions = DefaultValueTrimmingOptions;
        }

        private long HubKey { get; set; }

        public const int DefaultBufferSize = 0x1000;
        public const char DefaultDelimiter = ',';
        public const char DefaultEscape = '\"';
        public const char DefaultComment = '#';
        public const bool DefaultHeaders = true;
        public const EMissingFieldAction DefaultMissingFieldAction = EMissingFieldAction.ReplaceByNull;
        public const EParseErrorAction DefaultParseErrorAction = EParseErrorAction.ThrowException;
        public const char DefaultQuote = '\"';
        public const bool DefaultSkipEmptyLines = true;
        public const bool DefaultSupportsMutiline = true;
        public const EValueTrimmingOptions DefaultValueTrimmingOptions = EValueTrimmingOptions.All;

        [JsonConverter(typeof(StringEnumConverter))]
        public enum EParseErrorAction
        {
            RaiseEvent = 0,
            AdvanceToNextLine = 1,
            ThrowException = 2,
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public enum EMissingFieldAction
        {
            ParseError = 0,
            ReplaceByEmpty = 1,
            ReplaceByNull = 2,
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public enum EValueTrimmingOptions
        {
            None = 0,
            UnquotedOnly = 1,
            QuotedOnly = 2,
            All = UnquotedOnly | QuotedOnly
        }

        public int BufferSize { get; set; }

        public char Delimiter { get; set; }

        public char Quote { get; set; }

        public char Escape { get; set; }

        public char Comment { get; set; }

        public bool Headers { get; set; }

        public EParseErrorAction ParseErrorAction {get;set;}

        public EMissingFieldAction MissingFieldAction { get; set; }

        public bool SupportsMultiline { get; set; }

        public bool SkipBlankLines { get; set; }

        public EValueTrimmingOptions ValueTrimmingOptions { get; set; }


    }

}