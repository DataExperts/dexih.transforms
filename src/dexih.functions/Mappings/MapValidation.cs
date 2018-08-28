using System;
using System.ComponentModel;
using System.Linq;
using dexih.functions.Parameter;
using Dexih.Utils.MessageHelpers;

namespace dexih.functions.Mappings
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
                    var parameters = string.Join(",", Parameters.Inputs.OfType<ParameterColumn>().Select(c => $"{c.Column?.Name??c.Value}"));
                    reason = $"Function: {Function.FunctionName} ({parameters})";
                }
            }

            reason = null;
            return true;
        }
    }
}