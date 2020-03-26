using System.Collections.Generic;

namespace dexih.functions.BuiltIn
{
    public class CacheFunctions<T>
    {
        private T _cacheValue;
        
        public bool Reset()
        {
            _cacheValue = default;
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Caching", Name = "Previous Row",
            Description = "Returns the value from the previous row.", GenericType = EGenericType.All, ResetMethod = nameof(Reset))]
        public T PreviousRow(T value)
        {
            var returnValue = _cacheValue;
            _cacheValue = value;
            return returnValue;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Caching", Name = "Previous Row If Null",
            Description = "Returns the value from the previous row if the current value is null.", GenericType = EGenericType.All, ResetMethod = nameof(Reset))]
        public T PreviousRowIfNull(T value)
        {
            if(EqualityComparer<T>.Default.Equals(value, default(T))) {
                return _cacheValue;
            }
            else
            {
                _cacheValue = value;
                return value;
            }
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