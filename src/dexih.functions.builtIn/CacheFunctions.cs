namespace dexih.functions.BuiltIn
{
    public class CacheFunctions
    {
        private object _cacheValue;
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Caching", Name = "Previous Row",
            Description = "Returns the value from the previous row.")]
        public object PreviousRow(object value)
        {
            var returnValue = _cacheValue;
            _cacheValue = value;
            return returnValue;
        }

        private long _rowNumber = 1;
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Caching", Name = "Row Number",
            Description = "Returns the current row number")]
        public long RowNumber()
        {
            return _rowNumber++;
        }
    }
}