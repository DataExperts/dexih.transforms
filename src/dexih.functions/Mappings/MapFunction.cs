using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Parameter;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions.Mappings
{
    public class MapFunction: Mapping
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EFunctionCaching
        {
            NoCache,
            EnableCache,
            CallOnce
        }

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
        protected object[] Outputs;

        public object ResultReturnValue;
        private object[] _resultOutputs;
        
        private Dictionary<object[], (object, object[])> _cache;
        private bool isFirst = true;

        public override void InitializeColumns(Table table, Table joinTable = null)
        {
            Parameters.InitializeColumns(table, joinTable);
        }

        public override void AddOutputColumns(Table table)
        {
            Parameters.InitializeOutputOrdinals(table);
        }

        public override async Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null)
        {
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
                if (!isFirst)
                {
                    runFunction = false;
                }
            }

            isFirst = false;

            if (runFunction)
            {
                var taskReturn = Function.RunFunction(functionVariables, parameters, out Outputs);

                if (!taskReturn.IsCompleted)
                {
                    await taskReturn;
                }

                var resultProp = taskReturn.GetType().GetProperty("Result");
                ReturnValue = resultProp.GetValue(taskReturn);

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

        public override object GetInputValue(object[] row = null)
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
        
        public override bool ProcessResultRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType)
        {
            if (Function.FunctionType == functionType && Function.ResultMethod != null)
            {
                var parameters = Parameters.GetResultFunctionParameters();
                ResultReturnValue = Function.RunResult(functionVariables, parameters, out _resultOutputs);
                Parameters.SetResultFunctionResult(ResultReturnValue, _resultOutputs, row);
                
                if (Function.GeneratesRows && ResultReturnValue != null && ResultReturnValue is bool boolReturn)
                {
                    return boolReturn;
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