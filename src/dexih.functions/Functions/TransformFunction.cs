using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using dexih.functions.Exceptions;
using dexih.functions.Parameter;
using dexih.functions.Query;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;

namespace dexih.functions
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EErrorAction
    {
        Abend,
        Null,
		Ignore,
        Execute
    }

	/// <summary>
	/// The function class is used by transforms to run functions for conditions, mappings, and aggregations.
	/// </summary>
	public class TransformFunction
	{
		public MethodInfo FunctionMethod { get; set; }
		public MethodInfo ResetMethod { get; set; }
		public MethodInfo ResultMethod { get; set; }
		public MethodInfo ImportMethod { get; set; }
		public object ObjectReference { get; set; }
		
		public EFunctionType FunctionType { get; set; }

		private object _returnValue;

		[JsonConverter(typeof(StringEnumConverter))]
		public enum EInvalidAction
		{
			Pass = 1, //record passes with no action.
			Clean = 2, //record pass with cleanup parameters applied.
			RejectClean = 3, //2 records, one pass with cleanup, and one reject.
			Reject = 4, //record reject.
			Discard = 5, //record completely discarded.
			Abend = 6 //job abended.
		}

		/// <summary>
		/// A name that describes the function.
		/// </summary>
		public string FunctionName { get; set; }

		/// <summary>
		/// Action to take if there is an error in the function.
		/// </summary>
		public EErrorAction OnError { get; set; } = EErrorAction.Abend;

		/// <summary>
		/// Action to take if there is a null value received by the function.
		/// </summary>
		public EErrorAction OnNull { get; set; } = EErrorAction.Execute;

		/// <summary>
		/// If this is a boolean function, return the "NOT" result.
		/// </summary>
		public bool NotCondition { get; set; }

		public EInvalidAction InvalidAction { get; set; } = EInvalidAction.Reject;

		public Filter.ECompare? CompareEnum { get; set; }
		
		public GlobalVariables GlobalVariables { get; set; }

		/// <summary>
		/// Createa a new function from a "Delegate".
		/// </summary>
		/// <param name="functionMethod">Reference to the function that will be executed.</param>
		/// <param name="parameters"></param>
		public TransformFunction(Delegate functionMethod, Parameters parameters, GlobalVariables globalVariables) :
			this(functionMethod.Target, functionMethod.GetMethodInfo(), parameters, globalVariables)
		{
		}

		/// <summary>
		/// Creates a new function from a class/method reference.
		/// </summary>
		/// <param name="targetType">Type of the class which contains the method.  This class must contain a parameterless constructor.</param>
		/// <param name="methodName">The name of the method to call.</param>
		/// <param name="parameters"></param>
		public TransformFunction(Type targetType, string methodName, Parameters parameters, GlobalVariables globalVariables)
		{
			FunctionName = methodName;
			Initialize(Activator.CreateInstance(targetType), targetType.GetMethod(methodName), parameters, globalVariables);
		}

		/// <summary>
		/// Creates a new function from a class/method reference.
		/// </summary>
		/// <param name="target">An instantiated instance of the class containing the method.  Ensure a new instance of Target is created for each function to avoid issues with cached data.</param>
		/// <param name="methodName">The name of the method to call.</param>
		/// <param name="parameters"></param>
		/// <param name="globalVariables"></param>
		public TransformFunction(object target, string methodName, Parameters parameters, GlobalVariables globalVariables)
		{
			FunctionName = methodName;
			Initialize(target, target.GetType().GetMethod(methodName), parameters, globalVariables);
		}

		public TransformFunction(object target, MethodInfo functionMethod, Parameters parameters, GlobalVariables globalVariables)
		{
			Initialize(target, functionMethod, parameters, globalVariables);
		}

		private void Initialize(object target, MethodInfo functionMethod, Parameters parameters, GlobalVariables globalVariables)
		{
			FunctionMethod = functionMethod;
			GlobalVariables = globalVariables;

			var attribute = functionMethod.GetCustomAttribute<TransformFunctionAttribute>();
			var targetType = target.GetType();

			// Get the ResetMethod/ResultMethod which are used for aggregate functions.
			if (attribute != null)
			{
				FunctionType = attribute.FunctionType;

				ResetMethod = string.IsNullOrEmpty(attribute.ResetMethod)
					? null
					: targetType.GetMethod(attribute.ResetMethod);

				ResultMethod = string.IsNullOrEmpty(attribute.ResultMethod)
					? null
					: targetType.GetMethod(attribute.ResultMethod);

				ImportMethod = string.IsNullOrEmpty(attribute.ImportMethod)
					? null
					: targetType.GetMethod(attribute.ImportMethod);
			}

			// sets the global variables to the object if the property exists.
			var globalProperty = targetType.GetProperty("GlobalVariables");
			if (GlobalVariables != null && globalProperty != null)
			{
				globalProperty.SetValue(target, GlobalVariables);
			}
			
			// sets the array parameters of the object if the property exists.
			var parametersProperty = targetType.GetProperty("Parameters");
			if (parameters != null && parametersProperty != null)
			{
				parametersProperty.SetValue(target, parameters);
			}

			ObjectReference = target;
		}

		public TransformFunction()
		{
		}

		private (object[] parameters, int outputPos) SetParameters(ParameterInfo[] functionParameters, FunctionVariables functionVariables, object[] inputParameters)
		{
			var parameters = new object[functionParameters.Length];
			var outputPos = -1;

			var inputPosition = 0;
			var pos = 0;
			foreach (var parameter in functionParameters)
			{
				if (parameter.IsOut)
				{
					outputPos = pos;
					break;
				}

				if (inputPosition >= functionParameters.Length)
				{
					pos++;
					continue;
				}
				
				var variable = functionParameters[pos].GetCustomAttribute<TransformFunctionVariableAttribute>();
				if (variable is null)
				{
					if (functionParameters[pos].ParameterType.IsEnum)
					{
						if (inputParameters[inputPosition] is string stringValue)
						{
							parameters[pos] = Enum.Parse(functionParameters[pos].ParameterType, stringValue);
						}
					}
					else
					{
						parameters[pos] = inputParameters[inputPosition];	
					}

					pos++;
					inputPosition++;
				}
				else
				{
					parameters[pos++] = functionVariables.GetVariable(variable.FunctionParameter);
				}
			}

			return (parameters, outputPos);
		}

		public object Invoke(object[] inputParameters)
		{
			return Invoke(new FunctionVariables(), inputParameters, out _);
		}

		public object Invoke(object[] inputParameters, out object[] outputs)
		{
			return Invoke(new FunctionVariables(), inputParameters, out outputs);
		}


		public object Invoke(FunctionVariables functionVariables, object[] inputParameters)
		{
			return Invoke(functionVariables, inputParameters, out _);
		}
		
		public object Invoke(FunctionVariables functionVariables, object[] inputParameters, out object[] outputs)
		{
			var parameters = SetParameters(FunctionMethod.GetParameters(), functionVariables, inputParameters);

			_returnValue = FunctionMethod.Invoke(ObjectReference, parameters.parameters);

			if (parameters.outputPos >= 0)
			{
				outputs = parameters.parameters.Skip(parameters.outputPos).ToArray();
			}
			else
			{
				outputs = new object[0];
			}
			
			return _returnValue;
		}

		public object ReturnValue(object[] inputParameters, out object[] outputs)
		{
			return ReturnValue(new FunctionVariables(), inputParameters, out outputs);
		}
		
		public object ReturnValue(FunctionVariables functionVariables, object[] inputParameters, out object[] outputs)
		{
			var parameters = SetParameters(ResultMethod.GetParameters(), functionVariables, inputParameters);
			
			_returnValue = ResultMethod.Invoke(ObjectReference, parameters.parameters);

			if (parameters.outputPos >= 0)
			{
				outputs = parameters.parameters.Skip(parameters.outputPos).ToArray();
			}
			else
			{
				outputs = new object[0];
			}

			return _returnValue;
		}

        public void Reset()
        {
            try
            {
                if (ResetMethod != null)
                {
                    ResetMethod.Invoke(ObjectReference, null);
                }
            }
            catch(Exception ex)
            {
                throw new FunctionException($"The ResetMethod on the function {FunctionName} failed.  " + ex.Message, ex);
            }
        }

	    public string[] Import(object[] values)
	    {
		    try
		    {
			    if (ImportMethod != null)
			    {
				    return (string[]) ImportMethod.Invoke(ObjectReference, values);
			    }
			    
			    throw new FunctionException($"There is no import function for {FunctionName}.");
			    
		    }
		    catch(Exception ex)
		    {
			    throw new FunctionException($"The ImportMethod on the function {FunctionName} failed.  " + ex.Message, ex);
		    }
	    }

//        public string FunctionDetail()
//        {
//            var detail = GetType() + " ( ";
//            for (var i = 0; i < Inputs.Length; i++)
//                detail += Inputs[i].Name + "=" + (Inputs[i].Value == null ? "null" : Inputs[i].Value.ToString()) + (i < Inputs.Length - 1 ? "," : ")");
//            return detail;
//        }
    }


}
