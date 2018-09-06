using System;
using System.Linq;
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
        private object[] _outputs;

        public object ResultReturnValue;
        private object[] _resultOutputs;

        public override void InitializeInputOrdinals(Table table, Table joinTable = null)
        {
            Parameters.InitializeInputOrdinals(table, joinTable);
        }

        public override void AddOutputColumns(Table table)
        {
            Parameters.InitializeOutputOrdinals(table);
        }

        public override bool ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null)
        {
            Parameters.SetFromRow(row, joinRow);

            //gets the parameters.
            var parameters = Parameters.GetFunctionParameters();
            
            ReturnValue = Function.Invoke(functionVariables, parameters, out _outputs);
            
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
                _outputs = null;
                ResultReturnValue = null;
                _resultOutputs = null;
                Function.Reset();
            }
        }

        public override object GetInputValue(object[] row = null)
        {
            throw new NotSupportedException();
        }

        public override void ProcessOutputRow(object[] data)
        {
            Parameters.SetFunctionResult(ReturnValue, _outputs, data);
        }
        
        public override void ProcessResultRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType)
        {
            if (Function.FunctionType == functionType && Function.ResultMethod != null)
            {
                var parameters = Parameters.GetResultFunctionParameters();
                ResultReturnValue = Function.ReturnValue(functionVariables, parameters, out _resultOutputs);
                Parameters.SetResultFunctionResult(ResultReturnValue, _resultOutputs, row);
            }
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