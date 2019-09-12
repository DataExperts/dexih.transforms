using System;

namespace dexih.functions
{
    public enum EFunctionVariable
    {
        Index = 1,
        SeriesValue,
        Forecast 
    }
    
    public struct FunctionVariables
    {
        public int Index { get; set; }
        public object SeriesValue { get; set; }
        public bool Forecast { get; set; }

        public object GetVariable(EFunctionVariable functionVariable)
        {
            switch (functionVariable)
            {
                case EFunctionVariable.Index:
                    return Index;
                case EFunctionVariable.SeriesValue:
                    return SeriesValue;
                case EFunctionVariable.Forecast:
                    return Forecast;
                default:
                    throw new ArgumentOutOfRangeException(nameof(functionVariable), functionVariable, null);
            }
        }
    }
}