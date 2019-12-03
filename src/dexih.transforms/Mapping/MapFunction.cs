using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Parameter;
using dexih.functions.Query;
using Dexih.Utils.DataType;



namespace dexih.transforms.Mapping
{
    public class MapFunction: Mapping
    {


        public MapFunction()
        {
        }
        
        public MapFunction(TransformFunction function, Parameters parameters, EFunctionCaching functionCaching)
        {
            Function = function;
            Parameters = parameters;
            FunctionCaching = functionCaching;
        }
        
        public TransformFunction Function { get; set; }
        public Parameters Parameters { get; set; }
        public EFunctionCaching FunctionCaching { get; set; }

        public object ReturnValue;
        private IEnumerator _returnEnumerator;
        protected object[] Outputs;

        public object ResultReturnValue;
        private object[] _resultOutputs;

        private Dictionary<object[], (object, object[])> _cache;
        private bool _isFirst = true;


        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            Parameters.InitializeColumns(table, joinTable);
        }

        public override void AddOutputColumns(Table table)
        {
            Parameters.InitializeOutputOrdinals(table);
        }

        public override async Task<bool> ProcessInputRowAsync(FunctionVariables functionVariables, object[] row, object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            IgnoreRow = false;
            
            if (_returnEnumerator != null)
            {
                if (_returnEnumerator.MoveNext())
                {
                    ReturnValue = _returnEnumerator.Current;
                    return true;
                }
                else
                {
                    _returnEnumerator = null;
                    return false;
                }
            }
            
            Parameters.SetFromRow(row, joinRow);

            //gets the parameters.
            var parameters = Parameters.GetFunctionParameters();

            var runFunction = true;

            if (FunctionCaching == EFunctionCaching.EnableCache)
            {
                if (_cache == null)
                {
                    _cache = new Dictionary<object[], (object, object[])>(new FunctionCacheComparer());
                }

                if (_cache.TryGetValue(parameters, out var result))
                {
                    ReturnValue = result.Item1;
                    Outputs = result.Item2;
                    runFunction = false;
                }
            }

            if (FunctionCaching == EFunctionCaching.CallOnce)
            {
                if (!_isFirst)
                {
                    runFunction = false;
                }
            }

            _isFirst = false;

            if (runFunction)
            {
                
                if (Function.FunctionMethod.IsAsync)
                {
                    (ReturnValue, IgnoreRow) = await Function.RunFunctionAsync(functionVariables, parameters, cancellationToken);
                }
                else
                {
                    // ReSharper disable once VSTHRD103
                    (ReturnValue, IgnoreRow) = Function.RunFunction(functionVariables, parameters, out Outputs, cancellationToken);
                }

                if (FunctionCaching == EFunctionCaching.EnableCache)
                {
                    _cache.Add(parameters, (ReturnValue, Outputs));
                }
            }

            if (ReturnValue == null)
            {
                return false;
            }
            
            if (ReturnValue is bool boolReturn)
            {
                return boolReturn;
            }

            if (Function.FunctionType == EFunctionType.Rows)
            {
                if (ReturnValue is IEnumerable returnEnumerator)
                {
                    _returnEnumerator = returnEnumerator.GetEnumerator();

                    if (_returnEnumerator.MoveNext())
                    {
                        ReturnValue = _returnEnumerator.Current;
                    }
                    else
                    {
                        _returnEnumerator = null;
                        ReturnValue = null;
                        return false;
                    }
                }
            }
                
            return true;
        }

        public override void Reset(EFunctionType functionType)
        {
            if (Function.FunctionType == functionType)
            {
                ReturnValue = null;
                Outputs = null;
                ResultReturnValue = null;
                _resultOutputs = null;
                Function.Reset();
            }
        }

        public override object GetOutputValue(object[] row = null)
        {
            throw new NotSupportedException();
        }

        public override string Description()
        {
            return
                $"{Function.FunctionName}({string.Join(",", Parameters.Inputs.Select(c => c.Name))}, {string.Join(",", Parameters.Outputs.Select(c => "out " + c.Name))}";
        }

        public override void MapOutputRow(object[] data)
        {
            Parameters.SetFunctionResult(ReturnValue, Outputs, data);
        }
        
        public override async Task<bool> ProcessResultRowAsync(FunctionVariables functionVariables, object[] row, EFunctionType functionType, CancellationToken cancellationToken)
        {
            IgnoreRow = false;
            
            if (Function.FunctionType == functionType && Function.ResultMethod != null)
            {
                var parameters = Parameters.GetResultFunctionParameters();

                if (Function.ResultMethod.IsAsync)
                {
                    (ResultReturnValue, IgnoreRow) = await Function.RunResultAsync(functionVariables, parameters, cancellationToken);
                }
                else
                {
                    // ReSharper disable once VSTHRD103
                    (ResultReturnValue, IgnoreRow) = Function.RunResult(functionVariables, parameters, out _resultOutputs, cancellationToken);    
                }
                
                Parameters.SetResultFunctionResult(ResultReturnValue, _resultOutputs, row);
                
                if (Function.GeneratesRows)
                {
                    if (ResultReturnValue != null)
                    {
                        if (ResultReturnValue is bool boolReturn)
                        {
                            return boolReturn;
                        }

                        return true;
                    }

                    return false;
                }
            }

            return false;
        }

        public virtual MapFunction Copy()
        {
            var mapFunction = new MapFunction()
            {
                Function =  Function,
                Parameters = Parameters.Copy(),
                FunctionCaching = FunctionCaching 
            };

            return mapFunction;
        }

        /// <summary>
        /// Converts a standard function to a filter object.
        /// </summary>
        public Filter GetFilterFromFunction()
        {
//            if (mapFunction.Function.Parameters.ReturnParameter.DataType != ETypeCode.Boolean)
//            {
//                throw new QueryException(
//                    $"The function {mapFunction.Function.FunctionName} does not have a return type of boolean and cannot be used as a filter.");
//            }

            if (Function.Compare == null)
            {
                return null;
            }

            var inputsArray = Parameters.Inputs.ToArray();
            if (inputsArray.Length != 2)
            {
                return null;
            }
            
            var compare = (ECompare) Function.Compare;

            if (compare == ECompare.IsIn)
            {
                var filter = new Filter()
                {
                    Column1 = inputsArray[0] is ParameterColumn parameterColumn1 ? parameterColumn1.Column : null,
                    Value1 = null,
                    Column2 = null,
                    Value2 = inputsArray[1] is ParameterArray parameterValue2
                        ? parameterValue2.Parameters.Select(c => c.Value).ToArray()
                        : null,
                    CompareDataType = inputsArray[0].DataType,
                    Operator = compare
                };

                return filter;

            }
            {
                var filter = new Filter
                {

                    Column1 = inputsArray[0] is ParameterColumn parameterColumn1 ? parameterColumn1.Column : null,
                    Value1 = inputsArray[0] is ParameterColumn parameterValue1 ? parameterValue1.Value : null,
                    Column2 = inputsArray[1] is ParameterColumn parameterColumn2 ? parameterColumn2.Column : null,
                    Value2 = inputsArray[1] is ParameterColumn parameterValue2 ? parameterValue2.Value : null,
                    CompareDataType = inputsArray[0].DataType,
                    Operator = compare
                };

                return filter;
            }
        }

        public override IEnumerable<SelectColumn> GetRequiredColumns()
        {
            var columns = Parameters.Inputs.SelectMany(c => c.GetRequiredColumns());
            var columns2 = columns.Concat(Parameters.ResultInputs.SelectMany(c => c.GetRequiredColumns()));
            return columns2;
        }
    }
    
    public class FunctionCacheComparer : IEqualityComparer<object[]>
    {
        public bool Equals(object[] x, object[] y)
        {
            if (x.Length != y.Length)
            {
                return false;
            }
            for (int i = 0; i < x.Length; i++)
            {
                if (!Equals(x[i],y[i]))
                {
                    return false;
                }
            }
            return true;
        }

        public int GetHashCode(object[] obj)
        {
            int result = 17;
            for (int i = 0; i < obj.Length; i++)
            {
                unchecked
                {
                    result = result * 23 + obj[i].GetHashCode();
                }
            }
            return result;
        }
        

    }
}