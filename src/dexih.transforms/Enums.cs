namespace dexih.transforms
{
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum ETransformType
    {
        Mapping = 1, Filter, Sort, Group, Aggregate, Series, Join, Rows, Lookup, Validation, Delta, Concatenate, Profile, Internal
    }

    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EDuplicateStrategy
    {
        Abend = 0,
        First,
        Last,
        All
    }

    public enum ECacheMethod
    {
        NoCache = 0,
        DemandCache,
        PreLoadCache,
        LookupCache
    }

    public enum EEncryptionMethod
    {
        NoEncryption = 0,
        EncryptDecryptSecureFields = 1,
        MaskSecureFields = 2
    }
    
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EFunctionCaching
    {
        NoCache = 0,
        EnableCache,
        CallOnce
    }
    
    public enum EUpdateStrategy
    {
        Reload = 1, //truncates the table and reloads
        Append, //inserts records.  use if the data feed is always new data.
        AppendUpdate, //inserts new records, and updates records.  use if the data feed is new and existing data.
        AppendUpdateDelete, //inserts new records, updates existing records, and (logically) deletes removed records.  use to maintain an exact copy of the data feed.
        AppendUpdatePreserve, //inserts new records, updates existing records, and preserves the changes.
        AppendUpdateDeletePreserve // inserts new records, updates existing records, (logically) deletes removed records.
    }
}
