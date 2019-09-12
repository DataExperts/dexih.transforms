using System;
using System.Collections.Generic;
using System.Text;

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
        Abend = 1,
        First,
        Last,
        All
    }

    public enum ECacheMethod
    {
        NoCache = 1,
        DemandCache = 2,
        // PreLoadCache = 3,
        LookupCache = 4
    }

    public enum EEncryptionMethod
    {
        NoEncryption = 1,
        EncryptDecryptSecureFields = 2,
        MaskSecureFields = 3
    }
}
