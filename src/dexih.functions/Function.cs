using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
    public class Function
    {
        public MethodInfo FunctionMethod { get; set; }
        public MethodInfo ResetMethod { get; set; }
        public MethodInfo ResultMethod { get; set; }
        public object ObjectReference { get; set; }

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
        /// A name the describes the function.
        /// </summary>
        public string FunctionName { get; set; }

        /// <summary>
        /// List of input parameters
        /// </summary>
        public Parameter[] Inputs { get; set; }

        /// <summary>
        /// list of output parameters.
        /// </summary>
        public Parameter[] Outputs { get; set; }

        /// <summary>
        /// The datatype returned by the function.
        /// </summary>
        public ETypeCode ReturnType { get; set; }

        public TableColumn TargetColumn { get; set; } //default column to map return type to

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
        
        /// <summary>
        /// Createa a new function from a "Delegate".
        /// </summary>
        /// <param name="functionMethod">Reference to the function that will be executed.</param>
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
        public Function(Delegate functionMethod, TableColumn[] inputMappings, TableColumn targetColumn, TableColumn[] outputMappings) :
            this(functionMethod.Target, functionMethod.GetMethodInfo(), inputMappings, targetColumn, outputMappings)
        {
        }

        /// <summary>
        /// Creates a new function from a class/method reference.
        /// </summary>
        /// <param name="targetType">Type of the class which contains the method.  This class must contain a parameterless constructor.</param>
        /// <param name="methodName">The name of the method to call.</param>
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
        public Function(Type targetType, string methodName, TableColumn[] inputMappings, TableColumn targetColumn, TableColumn[] outputMappings)
        {
            FunctionName = methodName;
            Initialize(Activator.CreateInstance(targetType), targetType.GetMethod(methodName), inputMappings, targetColumn, outputMappings);
        }

        /// <summary>
        /// Creates a new function from a class/method reference.
        /// </summary>
        /// <param name="targetType">Type of the class which contains the method.  This class must contain a parameterless constructor.</param>
        /// <param name="methodName">The name of the method to call.</param>
        /// <param name="resetMethodName"></param>
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
        /// <param name="resultMethodName"></param>
        public Function(Type targetType, string methodName, string resultMethodName, string resetMethodName, TableColumn[] inputMappings, TableColumn targetColumn, TableColumn[] outputMappings)
        {
            FunctionName = methodName;
            ResultMethod = targetType.GetMethod(resultMethodName);
            ResetMethod = targetType.GetMethod(resetMethodName);

            Initialize(Activator.CreateInstance(targetType), targetType.GetMethod(methodName), inputMappings, targetColumn, outputMappings);
        }

        /// <summary>
        /// Creates a new function from a class/method reference.
        /// </summary>
        /// <param name="target">An instantiated instance of the class containing the method.  Ensure a new instance of Target is created for each function to avoid issues with cached data.</param>
        /// <param name="methodName">The name of the method to call.</param>
        /// <param name="resetMethodName"></param>
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
        /// <param name="resultMethodName"></param>
        public Function(object target, string methodName, string resultMethodName, string resetMethodName, TableColumn[] inputMappings, TableColumn targetColumn, TableColumn[] outputMappings)
            
        {
            FunctionName = methodName;
            ResultMethod = target.GetType().GetMethod(resultMethodName);
            ResetMethod = target.GetType().GetMethod(resetMethodName);

            Initialize(target, target.GetType().GetMethod(methodName), inputMappings, targetColumn, outputMappings);
        }

        public Function(object target, MethodInfo functionMethod, TableColumn[] inputMappings, TableColumn targetColumn, TableColumn[] outputMappings)
        {
            Initialize(target, functionMethod, inputMappings, targetColumn, outputMappings);
        }

        private void Initialize(object target, MethodInfo functionMethod, TableColumn[] inputMappings, TableColumn targetColumn, TableColumn[] outputMappings)
        {
            FunctionMethod = functionMethod;
            ObjectReference = target;

            TargetColumn = targetColumn;

            ReturnType = GetTypeCode(FunctionMethod.ReturnType);
            var inputParameters = functionMethod.GetParameters().Where(c => !c.IsOut).ToArray();

            if (inputMappings == null)
                inputMappings = new TableColumn[inputParameters.Length];

            Inputs = new Parameter[inputMappings.Length];

            var parameterCount = 0;
            for (var i = 0; i < inputMappings.Length; i++)
            {
                if (parameterCount > inputParameters.Length)
                {
                    throw new Exception("The input parameters could not be intialized as there are " + inputMappings.Length + " input mappings, however the function only has " + inputParameters.Length + " input parameters.");
                }

                Inputs[i] = new Parameter();
                Inputs[i].Column = inputMappings[i];
                Inputs[i].Name = inputParameters[parameterCount].Name;
                Inputs[i].IsColumn = true;

                var parameterType = inputParameters[parameterCount].ParameterType;
                Inputs[i].IsArray = parameterType.IsArray;
                if(parameterType.IsArray)
                    Inputs[i].DataType = GetTypeCode(parameterType.GetElementType());
                else
                    Inputs[i].DataType = GetTypeCode(parameterType);

                //if (Inputs[i].DataType == ETypeCode.Unknown)
                //{
                //    throw new Exception("The datatype: " + inputParameters[i].GetType().ToString() + " for parameter " + inputParameters[i].Name + " is not a supported datatype.");
                //}

                //when an array is found in a method, all parameters are mapped to this.  
                if (!parameterType.IsArray) parameterCount++;
            }

            ParameterInfo[] outputParameters;

            if (ResultMethod == null)
                outputParameters = functionMethod.GetParameters().Where(c => c.IsOut).ToArray();
            else
                outputParameters = ResultMethod.GetParameters().Where(c => c.IsOut).ToArray();

            parameterCount = 0;
            if (outputParameters.Length > 0)
            {
                Outputs = new Parameter[outputParameters.Length];

                if (outputMappings == null)
                    outputMappings = new TableColumn[outputParameters.Length];

                for (var i = 0; i < outputMappings.Length; i++)
                {
                    if (parameterCount > inputParameters.Length)
                    {
                        throw new Exception("The output parameters could not be intialized as there are " + outputMappings.Length + " output mappings, however the function only has " + outputParameters.Length + " output parameters.");
                    }

                    Outputs[i] = new Parameter();
                    Outputs[i].Column = outputMappings[i];
                    Outputs[i].Name = outputParameters[parameterCount].Name;

                    var parameterType = outputParameters[parameterCount].ParameterType.GetElementType();
                    Outputs[i].IsArray = parameterType.IsArray;
                    if (parameterType.IsArray)
                        Outputs[i].DataType = GetTypeCode(parameterType.GetElementType());
                    else
                        Outputs[i].DataType = GetTypeCode(parameterType);

                    //if (Outputs[i].DataType == ETypeCode.Unknown)
                    //{
                    //    throw new Exception("The datatype: " + outputParameters[i].GetType().ToString() + " for parameter " + outputParameters[i].Name + " is not a supported datatype.");
                    //}

                    //when an array is found in a method, all parameters are mapped to this.  
                    if (!parameterType.IsArray) parameterCount++;
                }
            }
        }

        public Function() { }

        public void SetVariableValues(string[] parametersValues)
        {
            if (Inputs.Length != parametersValues.Length)
            {
                throw new FunctionInvalidParametersException($"The number of inputs parameters of {parametersValues.Length} does not match expected {Inputs.Length} input values.");
            }

            for (var i = 0; i < Inputs.Length; i++)
            {
                try
                {
                    Inputs[i].SetValue(parametersValues[i]);
                } catch(Exception ex)
                {
#if DEBUG
                    throw new AggregateException($"The input parameter {Inputs[i].Name} with value {parametersValues[i]} could not be set.  " + ex.Message, ex);
#else
                    throw new AggregateException($"The input parameter {Inputs[i].Name} could not be set.  " + ex.Message, ex); //don't include values in the release version as this might be a sensative value.
#endif
                }
            }
        }

        public object RunFunction(object[] values, string[] outputNames = null)
        {
            //first add array parameters to the inputs field.
            if(Inputs.Length > 0 && Inputs[Inputs.Length - 1].IsArray)
            {
                var newInputs = new Parameter[values.Length];
                for (var i = 0; i < Inputs.Length; i++)
                    newInputs[i] = Inputs[i];

                for(var i = Inputs.Length; i< values.Length; i++)
                {
                    newInputs[i] = new Parameter { DataType = Inputs[Inputs.Length - 1].DataType, IsArray = true };
                }

                Inputs = newInputs;
            }

            if(outputNames != null)
            {
                var newOutputs = new Parameter[outputNames.Length];
                for (var i = 0; i < Outputs.Length; i++)
                    newOutputs[i] = Outputs[i];

                for (var i = Outputs.Length; i < outputNames.Length; i++)
                {
                    newOutputs[i] = new Parameter { Name = outputNames[i], DataType = Outputs[Outputs.Length - 1].DataType, IsArray = true };
                }

                Outputs = newOutputs;
            }

            if (values.Length != Inputs.Length )
            {
                throw new FunctionInvalidParametersException($"The number of inputs parameters of {values.Length} does not match expected {Inputs.Length} input values.");
            }
            for (var i = 0; i < values.Length; i++)
            {
                try
                {
                    Inputs[i].SetValue(values[i]);
                }
                catch (Exception ex)
                {
#if DEBUG
                    throw new AggregateException($"The input parameter {Inputs[i].Name} with value {values[i]} could not be set.  " + ex.Message, ex);
#else
                    throw new AggregateException($"The input parameter {Inputs[i].Name} could not be set.  " + ex.Message, ex); //don't include values in the release version as this might be a sensative value.
#endif
                }
            }

            return Invoke();
        }

 

        public object Invoke()
        {
			try
			{
				var mappingFunction = FunctionMethod;
				var inputsCount = Inputs?.Length ?? 0;
				var outputsCount = 0;
				if (ResultMethod == null)
					outputsCount = Outputs?.Length ?? 0;

				var parameters = new object[inputsCount + outputsCount];

				var parameterNumber = 0;

				var nullInputFound = false;

				List<object> arrayValues = null;
				var arrayType = ETypeCode.String;
				for (var i = 0; i < inputsCount; i++)
				{
					//FYI: this code will only accommodate for array being last parameter.
					if (Inputs != null && Inputs[i].IsArray)
					{
						if (arrayValues == null)
						{
							arrayValues = new List<object>();
							arrayType = Inputs[i].DataType;
						}

						try
						{
							var parseValue = TryParse(Inputs[i].DataType, Inputs[i].Value);

							if (parseValue == null)
							{
								if (OnNull == EErrorAction.Abend)
								{
									throw new FunctionNullValueException($"The input parameter {Inputs[i].Name} has a null value, and the function is set to abend on nulls.");
								}
								if (OnNull == EErrorAction.Ignore)
								{
									throw new FunctionIgnoreRowException();
								}
								nullInputFound = true;
							}

							arrayValues.Add(parseValue);
						}
						catch (Exception ex)
						{
#if DEBUG
							throw new AggregateException($"The input parameter {Inputs[i].Name} with value {Inputs[i].Value} could not be parsed.  " + ex.Message, ex);
#else
                        throw new AggregateException($"The input parameter {Inputs[i].Name} could not be parsed.  " + ex.Message, ex);
#endif
						}
					}
					else
					{
						parameters[parameterNumber] = Inputs?[i].Value;
						if (parameters[parameterNumber] == null || parameters[parameterNumber] is DBNull) parameters[parameterNumber] = null;

						if (parameters[parameterNumber] == null)
						{
							if (OnNull == EErrorAction.Abend)
							{
								throw new FunctionException($"The input parameter {Inputs[i].Name} has a null value, and the function is set to abend on nulls.");
							}
							if (OnNull == EErrorAction.Ignore)
							{
								throw new FunctionIgnoreRowException();
							}

							nullInputFound = true;
						}

						parameterNumber++;
					}
				}

				if (arrayValues != null)
				{
					try
					{
						//convert the values and load them into the parameter array.
						switch (arrayType)
						{
							case ETypeCode.Byte:
								parameters[parameterNumber] = arrayValues.Select(c => (byte)c).ToArray();
								break;
							case ETypeCode.SByte:
								parameters[parameterNumber] = arrayValues.Select(c => (sbyte)c).ToArray();
								break;
							case ETypeCode.UInt16:
								parameters[parameterNumber] = arrayValues.Select(c => (ushort)c).ToArray();
								break;
							case ETypeCode.UInt32:
								parameters[parameterNumber] = arrayValues.Select(c => (uint)c).ToArray();
								break;
							case ETypeCode.UInt64:
								parameters[parameterNumber] = arrayValues.Select(c => (ulong)c).ToArray();
								break;
							case ETypeCode.Int16:
								parameters[parameterNumber] = arrayValues.Select(c => (short)c).ToArray();
								break;
							case ETypeCode.Int32:
								parameters[parameterNumber] = arrayValues.Select(c => (int)c).ToArray();
								break;
							case ETypeCode.Int64:
								parameters[parameterNumber] = arrayValues.Select(c => (long)c).ToArray();
								break;
							case ETypeCode.Decimal:
								parameters[parameterNumber] = arrayValues.Select(c => (decimal)c).ToArray();
								break;
							case ETypeCode.Double:
								parameters[parameterNumber] = arrayValues.Select(c => (double)c).ToArray();
								break;
							case ETypeCode.Single:
								parameters[parameterNumber] = arrayValues.Select(c => (float)c).ToArray();
								break;
							case ETypeCode.String:
								parameters[parameterNumber] = arrayValues.Select(c => (string)c).ToArray();
								break;
							case ETypeCode.Boolean:
								parameters[parameterNumber] = arrayValues.Select(c => (bool)c).ToArray();
								break;
							case ETypeCode.DateTime:
								parameters[parameterNumber] = arrayValues.Select(c => (DateTime)c).ToArray();
								break;
							case ETypeCode.Time:
								parameters[parameterNumber] = arrayValues.Select(c => (DateTime)c).ToArray();
								break;
							case ETypeCode.Guid:
								parameters[parameterNumber] = arrayValues.Select(c => (Guid)c).ToArray();
								break;
							case ETypeCode.Unknown:
							default:
								parameters[parameterNumber] = arrayValues.ToArray();
								break;
						}
					}
					catch (Exception ex)
					{
#if DEBUG
						throw new AggregateException($"The input array with values {string.Join(",", arrayValues)} could not be converted.  " + ex.Message, ex);
#else
                    throw new AggregateException($"The input array could not be converted.  " + ex.Message, ex);
#endif

					}
					parameterNumber++;
				}

				var outputParameterNumber = parameterNumber;

				//if there is no resultfunction, then this function will require the output parameters
				if (ResultMethod == null)
				{
					arrayValues = null;
					for (var i = 0; i < outputsCount; i++)
					{
						//FYI: this code will only accommodate for array being last parameter.
						if (Outputs != null && Outputs[i].IsArray)
						{
							if (arrayValues == null) arrayValues = new List<object>();
							arrayValues.Add(null);
						}
						else
						{
							parameters[parameterNumber] = Outputs?[i].Value;
							if (parameters[parameterNumber] != null && parameters[parameterNumber] is DBNull) parameters[parameterNumber] = null;

							parameterNumber++;
						}
					}

					if (arrayValues != null)
					{
						//parameters[parameterNumber] = arrayValues.Select(c => c.ToString()).ToArray();
						parameters[parameterNumber] = new string[arrayValues.Count];
						parameterNumber++;
					}
				}

				Array.Resize(ref parameters, parameterNumber);

				// execute the function.
				if (nullInputFound && OnNull == EErrorAction.Null)
				{
					_returnValue = null;
				}
				else
				{
					_returnValue = mappingFunction.Invoke(ObjectReference, parameters);
				}

				if (ResultMethod == null)
				{
					var arrayNumber = 0;
					for (var i = 0; i < outputsCount; i++)
					{

						try
						{

							if (Outputs != null && Outputs[i].IsArray)
							{
								var parametersArray = (object[])parameters[outputParameterNumber];
								if (parametersArray == null)
									Outputs[i].SetValue(DBNull.Value);
								else
									Outputs[i].SetValue(arrayNumber >= parametersArray.Length ? DBNull.Value : parametersArray[arrayNumber]);

								arrayNumber++;
							}
							else
							{
								Outputs[i].SetValue(parameters[outputParameterNumber]);
								outputParameterNumber++;
							}

						}
						catch (Exception ex)
						{
#if DEBUG
							throw new AggregateException($"The function {FunctionName} with the return parameter {Outputs[i].Name} with value {parameters[outputParameterNumber]} could not be converted.  " + ex.Message, ex);
#else
                        throw new AggregateException($"The function {FunctionName} with the return parameter {Outputs[i].Name} could not be converted.  " + ex.Message, ex);
#endif
						}
					}
				}

				if (ReturnType == ETypeCode.Boolean && NotCondition)
					_returnValue = _returnValue == null ? null :  !(bool?)_returnValue;

				return _returnValue;
			}
			catch(FunctionIgnoreRowException)
			{
				throw;
			}
			catch(Exception ex)
			{
				// based on onerror setting, either return null or rethrow the error.
				switch (OnError)
				{
					case EErrorAction.Abend:
						throw new FunctionException($"The function {FunctionName} failed.  " + ex.Message, ex);
					case EErrorAction.Ignore:
						throw new FunctionIgnoreRowException();
					default:
						return null;
				}
			}
        }

        /// <summary>
        /// Get the return value from an aggregate function.  
        /// </summary>
        /// <param name="index">Index represents result row number within the grouping, and is used for series functions that return multiple results from one aggregation.</param>
        /// <returns></returns>
        public object ReturnValue(int? index = 0)
        {
            if (ResultMethod != null)
            {
                var outputsCount = Outputs?.Length ?? 0;
                int indexAdjust;
                object[] parameters;

                //if the result method has an "index" as the first parameter, then add the index
                if (ResultMethod.GetParameters().Count() > 0 && ResultMethod.GetParameters()[0].Name == "index")
                {
                    parameters = new object[outputsCount + 1];
                    parameters[0] = index;
                    indexAdjust = 1;
                }
                else
                {
                    parameters = new object[outputsCount];
                    indexAdjust = 0;
                }

                List<object> arrayValues = null;
                for (var i = 0; i < outputsCount; i++)
                {
                    //FYI: this code will only accommodate for array being last parameter.
                    if (Outputs != null && Outputs[i].IsArray)
                    {
                        if (arrayValues == null) arrayValues = new List<object>();
                        arrayValues.Add(null);
                    }
                    else
                    {
                        parameters[i + indexAdjust] = null; 
                    }
                }

                if (arrayValues != null)
                {
                    parameters[outputsCount + indexAdjust] = arrayValues.Select(c => Convert.ChangeType(c, Type.GetType("System." + Outputs.Last().DataType)));
                }

                _returnValue = ResultMethod.Invoke(ObjectReference, parameters);

                var arrayNumber = 0;
                for (var i = 0; i < outputsCount; i++)
                {
                    if (Outputs != null && Outputs[i].IsArray)
                    {
                        var array = (object[])parameters[i + indexAdjust];
                        try
                        {
                            Outputs[i].SetValue(arrayNumber >= array.Length ? DBNull.Value : array[arrayNumber]);
                        }
                        catch (Exception ex)
                        {
#if DEBUG
                            throw new AggregateException($"The function {FunctionName} with the output array {Outputs[i].Name} with values {string.Join(",", array)} could not be converted.  " + ex.Message, ex);
#else
                            throw new AggregateException($"The function {FunctionName} with the output array {Outputs[i].Name} could not be converted.  " + ex.Message, ex);
#endif
                        }
                        arrayNumber++;
                    }
                    else
                    {
                        try
                        {
                            Outputs[i].SetValue(parameters[i + indexAdjust]);
                        }
                        catch (Exception ex)
                        {
#if DEBUG
                            throw new AggregateException($"The function {FunctionName} with the output parameter {Outputs[i].Name} with value {parameters[i + indexAdjust]} could not be converted.  " + ex.Message, ex);
#else
                                throw new AggregateException($"The function {FunctionName} with the output parameter {Outputs[i].Name} could not be converted.  " + ex.Message, ex);
#endif

                        }

                    }
                }
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

        public string FunctionDetail()
        {
            var detail = GetType() + " ( ";
            for (var i = 0; i < Inputs.Length; i++)
                detail += Inputs[i].Name + "=" + (Inputs[i].Value == null ? "null" : Inputs[i].Value.ToString()) + (i < Inputs.Length - 1 ? "," : ")");
            return detail;
        }
    }


}
