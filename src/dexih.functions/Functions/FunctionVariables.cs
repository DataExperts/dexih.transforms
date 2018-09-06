using System;

namespace dexih.functions
{
    public enum EFunctionVariable
    {
        Index,
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
                    break;
                case EFunctionVariable.SeriesValue:
                    return SeriesValue;
                    break;
                case EFunctionVariable.Forecast:
                    return Forecast;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(functionVariable), functionVariable, null);
            }
        }
    }
}