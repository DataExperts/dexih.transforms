using System.Linq;
using dexih.functions;
using dexih.functions.Parameter;
using dexih.functions.Query;

namespace dexih.transforms.Mapping
{
    public class MapValidation: MapFunction
    {
        public MapValidation(TransformFunction function, Parameters parameters)
        {
            Function = function;
            Parameters = parameters;
        }

        public bool Validated(out string reason)
        {
            if (ReturnValue is bool returnValue)
            {
                if (returnValue)
                {
                    reason = null;
                    return true;
                }
                else
                {
                    //TODO Need to improve reason string.
                    var parameters = string.Join(",", Parameters.Inputs.OfType<ParameterColumn>().Select(c => $"{c.Column?.Name??c.Value}={c.Value}"));
                    reason = $"Function: {Function.FunctionName} ({parameters})";
                    return false;
                }
            }

            reason = null;
            return true;
        }
        
        public override void MapOutputRow(object[] data)
        {
            // only map the result when the validation is false.
            if (ReturnValue is bool returnValue)
            {
                if (!returnValue)
                {
                    Parameters.SetFunctionResult(ReturnValue, Outputs, data);        
                }
            }
            
        }
        
        public override bool MatchesSelectQuery(SelectQuery selectQuery)
        {
            return false;
        }

    }
}