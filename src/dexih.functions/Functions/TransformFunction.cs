using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Exceptions;
using dexih.functions.Parameter;
using dexih.functions.Query;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

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

    public enum EParameterClass
    {
	    Variable,
	    Enum,
	    EnumArray,
	    Cancellation,
	    Value
    }

	public class TransformParameter
	{
		public string Name { get; set; }
		public bool IsOut { get; set; }
		public Type ParameterType { get; set; }
		public TransformFunctionVariableAttribute Variable { get; set; }

		public EParameterClass ParameterClass { get; set; }
	}

	/// <summary>
	/// Stores required method information for use during invoke.
	/// </summary>
	public class TransformMethod
	{
		public MethodInfo MethodInfo { get; set; }
		public TransformParameter[] ParameterInfo { get; set; }
		public bool IsAsync { get; set; }
		

		public TransformMethod(MethodInfo methodInfo, Type genericType = null)
		{
            if(methodInfo.IsGenericMethod)
            {
                MethodInfo = methodInfo.MakeGenericMethod(genericType);
            }
            else
            {
                MethodInfo = methodInfo;
            }
            
			ParameterInfo = methodInfo.GetParameters().Select(p =>
			{
				var variable = p.GetCustomAttribute<TransformFunctionVariableAttribute>();
				EParameterClass parameterClass;
				if (variable != null)
				{
					parameterClass = EParameterClass.Variable;
				} 
				else if (p.ParameterType.IsEnum)
				{
					parameterClass = EParameterClass.Enum;
				} 
				else if(p.ParameterType.IsArray && p.ParameterType.GetElementType().IsEnum)
				{
					parameterClass = EParameterClass.EnumArray;
				} 
				else if (p.ParameterType.IsAssignableFrom(typeof(CancellationToken)))
				{
					parameterClass = EParameterClass.Cancellation;
				}
				else
				{
					parameterClass = EParameterClass.Value;
				}

				return new TransformParameter()
				{
					Name = p.Name,
					IsOut = p.IsOut,
					ParameterType = p.ParameterType,
					Variable = variable,
					ParameterClass = parameterClass
				};
			}).ToArray();

			var asyncAttribute = (AsyncStateMachineAttribute)methodInfo.GetCustomAttribute(typeof(AsyncStateMachineAttribute));

			IsAsync = asyncAttribute != null || methodInfo.ReturnType.BaseType == typeof(Task);
		}
	}

	/// <summary>
	/// The function class is used by transforms to run functions for conditions, mappings, and aggregations.
	/// </summary>
	public class TransformFunction : IDisposable
	{
		public TransformMethod InitializeMethod { get; set; }
		public TransformMethod FunctionMethod { get; set; }
		public TransformMethod ResetMethod { get; set; }
		public TransformMethod ResultMethod { get; set; }
		public TransformMethod ImportMethod { get; set; }
		public object ObjectReference { get; set; }
		
		public EFunctionType FunctionType { get; set; }

		public bool GeneratesRows = false;

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

		public TransformFunction(Delegate functionMethod):
			this(functionMethod.Target, functionMethod.GetMethodInfo(), null, null, null)
		{
			
		}

		/// <summary>
		/// Createa a new function from a "Delegate".
		/// </summary>
		/// <param name="functionMethod">Reference to the function that will be executed.</param>
		/// <param name="parameters"></param>
		public TransformFunction(Delegate functionMethod, Type genericType, Parameters parameters, GlobalVariables globalVariables) :
			this(functionMethod.Target, functionMethod.GetMethodInfo(), genericType, parameters, globalVariables)
		{
		}

		/// <summary>
		/// Creates a new function from a class/method reference.
		/// </summary>
		/// <param name="targetType">Type of the class which contains the method.  This class must contain a parameterless constructor.</param>
		/// <param name="methodName">The name of the method to call.</param>
		/// <param name="parameters"></param>
		public TransformFunction(Type targetType, string methodName, Type genericType, Parameters parameters, GlobalVariables globalVariables)
		{
			FunctionName = methodName;
            if(targetType.IsGenericTypeDefinition)
            {
                targetType = targetType.MakeGenericType(genericType);
            }
            Constructor(Activator.CreateInstance(targetType), targetType.GetMethod(methodName), genericType, parameters, globalVariables);
        }

		/// <summary>
		/// Creates a new function from a class/method reference.
		/// </summary>
		/// <param name="target">An instantiated instance of the class containing the method.  Ensure a new instance of Target is created for each function to avoid issues with cached data.</param>
		/// <param name="methodName">The name of the method to call.</param>
		/// <param name="parameters"></param>
		/// <param name="globalVariables"></param>
		public TransformFunction(object target, string methodName, Type genericType = null,  Parameters parameters = null, GlobalVariables globalVariables = null)
		{
			FunctionName = methodName;
			Constructor(target, target.GetType().GetMethod(methodName), genericType, parameters, globalVariables);
		}

		public TransformFunction(object target, MethodInfo functionMethod, Type genericType, Parameters parameters, GlobalVariables globalVariables)
		{
			Constructor(target, functionMethod, genericType, parameters, globalVariables);
		}

		private void Constructor(object target, MethodInfo functionMethod, Type genericType, Parameters parameters, GlobalVariables globalVariables)
		{
			FunctionMethod = new TransformMethod(functionMethod, genericType);
			
			
			GlobalVariables = globalVariables;

			var attribute = functionMethod.GetCustomAttribute<TransformFunctionAttribute>();
			var targetType = target.GetType();

			// Get the ResetMethod/ResultMethod which are used for aggregate functions.
			if (attribute != null)
			{
				FunctionType = attribute.FunctionType;

				InitializeMethod = string.IsNullOrEmpty(attribute.InitializeMethod)
					? null
					: new TransformMethod(targetType.GetMethod(attribute.InitializeMethod), genericType);
				ResetMethod = string.IsNullOrEmpty(attribute.ResetMethod)
					? null
					: new TransformMethod(targetType.GetMethod(attribute.ResetMethod), genericType);

				ResultMethod = string.IsNullOrEmpty(attribute.ResultMethod)
					? null
					:  new TransformMethod(targetType.GetMethod(attribute.ResultMethod), genericType);
				
				ImportMethod = string.IsNullOrEmpty(attribute.ImportMethod)
					? null
					:  new TransformMethod(targetType.GetMethod(attribute.ImportMethod), genericType);

				GeneratesRows = attribute.GeneratesRows;
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

		public async Task Initialize(CancellationToken cancellationToken = default)
		{
			try
			{
				if (InitializeMethod != null)
				{
					if (InitializeMethod.MethodInfo.ReturnType.IsAssignableFrom(typeof(Task<>)))
					{
						var task = (Task) InitializeMethod.MethodInfo.Invoke(ObjectReference,
							new object[] {cancellationToken});
						await task;
					}
					else
					{
						InitializeMethod.MethodInfo.Invoke(ObjectReference, new object[] {cancellationToken});
					}
				}
			}
			catch (TargetInvocationException ex)
			{
				if (ex.InnerException != null)
				{
					throw new FunctionException(
						$"The initialize method on the function {FunctionName} failed.  " + ex.InnerException.Message,
						ex.InnerException);
				}

				throw;
			}
			catch (Exception ex)
			{
				throw new FunctionException($"The initialize method on the function {FunctionName} failed.  " + ex.Message,
					ex);
			}

		}

		public TransformFunction()
		{
		}

		private (object[] parameters, int outputPos) SetParameters(TransformParameter[] functionParameters, FunctionVariables functionVariables, object[] inputParameters, CancellationToken cancellationToken = default)
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

				switch (parameter.ParameterClass)
				{
					case EParameterClass.Variable:
						var variable = functionParameters[pos].Variable;
						parameters[pos++] = functionVariables.GetVariable(variable.FunctionParameter);
						break;
					case EParameterClass.Enum:
						if (inputParameters[inputPosition] is string stringValue)
						{
							parameters[pos] = Enum.Parse(functionParameters[pos].ParameterType, stringValue);
						}
						else
						{
							parameters[pos] = inputParameters[inputPosition];
						}
						pos++;
						inputPosition++;

						break;
					case EParameterClass.EnumArray:
						var enumType = functionParameters[pos].ParameterType.GetElementType();
						var array = (Array)inputParameters[inputPosition];
						var newArray = Array.CreateInstance(enumType, array.Length);

						var i = 0;
						foreach (var item in array)
						{
							if (item is string stringValue2)
							{
								newArray.SetValue(Enum.Parse(enumType, stringValue2), i);
							}
							else
							{
								newArray.SetValue(item, i);
							}

							i++;
						}

						parameters[pos] = newArray;
						

						break;
					case EParameterClass.Cancellation:
						parameters[pos] = cancellationToken;
						pos++;
						break;
					case EParameterClass.Value:
						parameters[pos] = inputParameters[inputPosition];
						pos++;
						inputPosition++;
						break;
					default:
						throw new ArgumentOutOfRangeException();
				}
			}

			return (parameters, outputPos);
		}

		public object RunFunction(object[] inputParameters, CancellationToken cancellationToken)
		{
			return RunFunction(new FunctionVariables(), inputParameters, out _, cancellationToken);
		}

		public object RunFunction(object[] inputParameters, out object[] outputs, CancellationToken cancellationToken)
		{
			return RunFunction(new FunctionVariables(), inputParameters, out outputs, cancellationToken);
		}


		public object RunFunction(FunctionVariables functionVariables, object[] inputParameters, CancellationToken cancellationToken)
		{
			return RunFunction(functionVariables, inputParameters, out _, cancellationToken);
		}
		
		public object RunFunction(FunctionVariables functionVariables, object[] inputParameters, out object[] outputs, CancellationToken cancellationToken)
		{
			return Invoke(FunctionMethod, functionVariables, inputParameters, out outputs, cancellationToken);
		}

		public object RunResult(object[] inputParameters, out object[] outputs, CancellationToken cancellationToken)
		{
			return Invoke(ResultMethod, new FunctionVariables(), inputParameters, out outputs, cancellationToken);
		}
		
		public object RunResult(FunctionVariables functionVariables, object[] inputParameters, out object[] outputs, CancellationToken cancellationToken)
		{
			return Invoke(ResultMethod, functionVariables, inputParameters, out outputs, cancellationToken);
		}
		
		public Task<object> RunFunctionAsync(FunctionVariables functionVariables, object[] inputParameters, CancellationToken cancellationToken)
		{
			return InvokeAsync(FunctionMethod, functionVariables, inputParameters, cancellationToken);
		}

		public Task<object> RunResultAsync(FunctionVariables functionVariables, object[] inputParameters, CancellationToken cancellationToken)
		{
			return InvokeAsync(ResultMethod, functionVariables, inputParameters, cancellationToken);
		}

		private async Task<object> InvokeAsync(TransformMethod methodInfo, FunctionVariables functionVariables, object[] inputParameters, CancellationToken cancellationToken)
		{
			try
			{
				var (parameters, outputPos) = SetParameters(methodInfo.ParameterInfo, functionVariables, inputParameters, cancellationToken);
				var task = (Task) methodInfo.MethodInfo.Invoke(ObjectReference, parameters);
				await task.ConfigureAwait(false);
				var resultProperty = task.GetType().GetProperty("Result");
				_returnValue = resultProperty.GetValue(task);
				return _returnValue;
			}
			catch (TargetInvocationException ex)
			{
				if (ex.InnerException != null)
				{
					throw new FunctionException(
						$"The function {FunctionName} failed.  " + ex.InnerException.Message,
						ex.InnerException);
				}

				throw;
			}
			catch(Exception ex)
			{
				throw new FunctionException($"The the function {FunctionName} failed.  " + ex.Message, ex);
			}
		}

		private object Invoke(TransformMethod methodInfo, FunctionVariables functionVariables, object[] inputParameters, out object[] outputs, CancellationToken cancellationToken)
		{
			try
			{
				var (parameters, outputPos) =
					SetParameters(methodInfo.ParameterInfo, functionVariables, inputParameters, cancellationToken);

				var returnValue = methodInfo.MethodInfo.Invoke(ObjectReference, parameters);
				_returnValue = returnValue;

				if (outputPos >= 0)
				{
					outputs = parameters.Skip(outputPos).ToArray();
				}
				else
				{
					outputs = new object[0];
				}

				return _returnValue;
			}
			catch (TargetInvocationException ex)
			{
				if (ex.InnerException != null)
				{
					throw new FunctionException(
						$"The function {FunctionName} failed.  " + ex.InnerException.Message,
						ex.InnerException);
				}

				throw;
			}
			catch(Exception ex)
			{
				throw new FunctionException($"The function {FunctionName} failed.  " + ex.Message, ex);
			}
		}
		
        public void Reset()
        {
            try
            {
	            ResetMethod?.MethodInfo.Invoke(ObjectReference, null);
            }
            catch (TargetInvocationException ex)
            {
	            if (ex.InnerException != null)
	            {
		            throw new FunctionException(
			            $"The reset method on the function {FunctionName} failed.  " + ex.InnerException.Message,
			            ex.InnerException);
	            }

	            throw;
            }
            catch(Exception ex)
            {
	            throw new FunctionException($"The reset method on the function {FunctionName} failed.  " + ex.Message, ex);
            }
        }

	    public string[] Import(object[] values)
	    {
		    try
		    {
			    if (ImportMethod != null)
			    {
				    return (string[]) ImportMethod.MethodInfo.Invoke(ObjectReference, values);
			    }

			    throw new FunctionException($"There is no import function for {FunctionName}.");

		    }
		    catch (TargetInvocationException ex)
		    {
			    if (ex.InnerException != null)
			    {
				    throw new FunctionException(
					    $"The Import method on the function {FunctionName} failed.  " + ex.InnerException.Message,
					    ex.InnerException);
			    }

			    throw;
		    }
		    catch(Exception ex)
		    {
			    throw new FunctionException($"The Import method on the function {FunctionName} failed.  " + ex.Message, ex);
		    }
	    }

//        public string FunctionDetail()
//        {
//            var detail = GetType() + " ( ";
//            for (var i = 0; i < Inputs.Length; i++)
//                detail += Inputs[i].Name + "=" + (Inputs[i].Value == null ? "null" : Inputs[i].Value.ToString()) + (i < Inputs.Length - 1 ? "," : ")");
//            return detail;
//        }
		public void Dispose()
		{
			if (ObjectReference is IDisposable objectReference)
			{
				objectReference.Dispose();
			}
		}
	}


}
