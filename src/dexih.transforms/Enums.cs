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
}
