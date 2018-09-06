namespace dexih.functions.BuiltIn
{
    public class CacheFunctions
    {
        public GlobalVariables GlobalVariables { get; set; }

        private object cacheValue;
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Caching", Name = "Previous Row",
            Description = "Returns the value from the previous row.")]
        public object PreviousRow(object value)
        {
            var returnValue = cacheValue;
            cacheValue = value;
            return returnValue;
        }
    }
}