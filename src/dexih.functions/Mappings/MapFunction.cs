using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Parameter;
using Dexih.Utils.CopyProperties;

namespace dexih.functions.Mappings
{
    public class MapFunction: Mapping
    {
        public MapFunction()
        {
            
        }
        
        public MapFunction(TransformFunction function, Parameters parameters)
        {
            Function = function;
            Parameters = parameters;
        }
        
        public TransformFunction Function { get; set; }
        public Parameters Parameters { get; set; }

        public object ReturnValue;
        protected object[] Outputs;

        public object ResultReturnValue;
        private object[] _resultOutputs;

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
            
            var taskReturn = Function.RunFunction(functionVariables, parameters, out Outputs);

            if (!taskReturn.IsCompleted)
            {
                await taskReturn;
            }

            var resultProp = taskReturn.GetType().GetProperty("Result");
            ReturnValue = resultProp.GetValue(taskReturn);
            
            if (ReturnValue != null && ReturnValue is bool boolReturn)
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
                Parameters = Parameters.Copy()
            };

            return mapFunction;
        }

    }
}