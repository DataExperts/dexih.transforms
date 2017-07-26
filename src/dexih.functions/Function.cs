using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using static dexih.functions.DataType;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.functions
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EErrorAction
    {
        Abend,
        Null,
        Reject,
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
        /// <summary>
        /// Invalid action when a validation function fails.  Order of these is important as determines priority(i.e. abend overrides a clean).
        /// </summary>
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
        public EErrorAction OnError { get; set; }

        /// <summary>
        /// Action to take if there is a null value received by the function.
        /// </summary>
        public EErrorAction OnNull { get; set; }

        /// <summary>
        /// If this is a boolean function, return the "NOT" result.
        /// </summary>
        public Boolean NotCondition { get; set; }

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
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
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
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
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
            this.FunctionMethod = functionMethod;
            ObjectReference = target;

            TargetColumn = targetColumn;

            ReturnType = GetTypeCode(this.FunctionMethod.ReturnType);
            ParameterInfo[] inputParameters = functionMethod.GetParameters().Where(c => !c.IsOut).ToArray();

            if (inputMappings == null)
                inputMappings = new TableColumn[inputParameters.Length];

            Inputs = new Parameter[inputMappings.Length];

            int parameterCount = 0;
            for (int i = 0; i < inputMappings.Length; i++)
            {
                if (parameterCount > inputParameters.Length)
                {
                    throw new Exception("The input parameters could not be intialized as there are " + inputMappings.Length + " input mappings, however the function only has " + inputParameters.Length + " input parameters.");
                }

                Inputs[i] = new Parameter();
                Inputs[i].Column = inputMappings[i];
                Inputs[i].Name = inputParameters[parameterCount].Name;
                Inputs[i].IsColumn = true;

                Type parameterType = inputParameters[parameterCount].ParameterType;
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

                for (int i = 0; i < outputMappings.Length; i++)
                {
                    if (parameterCount > inputParameters.Length)
                    {
                        throw new Exception("The output parameters could not be intialized as there are " + outputMappings.Length + " output mappings, however the function only has " + outputParameters.Length + " output parameters.");
                    }

                    Outputs[i] = new Parameter();
                    Outputs[i].Column = outputMappings[i];
                    Outputs[i].Name = outputParameters[parameterCount].Name;

                    Type parameterType = outputParameters[parameterCount].ParameterType.GetElementType();
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

        public ReturnValue SetVariableValues(string[] parametersValues)
        {
            if (Inputs.Length != parametersValues.Length)
            {
                return new ReturnValue(false, "The number of inputs parameters does not match expected " + Inputs.Length + " values.", null);
            }

            for (int i = 0; i < Inputs.Length; i++)
            {
                var result = Inputs[i].SetValue(parametersValues[i]);
                if (result.Success == false)
                    return result;
            }
            return new ReturnValue(true);
        }

        public ReturnValue<object> RunFunction(object[] values, string[] outputNames = null)
        {
            //first add array parameters to the inputs field.
            if(Inputs.Length > 0 && Inputs[Inputs.Length - 1].IsArray)
            {
                Parameter[] newInputs = new Parameter[values.Length];
                for (int i = 0; i < Inputs.Length; i++)
                    newInputs[i] = Inputs[i];

                for(int i = Inputs.Length; i< values.Length; i++)
                {
                    newInputs[i] = new Parameter { DataType = Inputs[Inputs.Length - 1].DataType, IsArray = true };
                }

                Inputs = newInputs;
            }

            if(outputNames != null)
            {
                Parameter[] newOutputs = new Parameter[outputNames.Length];
                for (int i = 0; i < Outputs.Length; i++)
                    newOutputs[i] = Outputs[i];

                for (int i = Outputs.Length; i < outputNames.Length; i++)
                {
                    newOutputs[i] = new Parameter { Name = outputNames[i], DataType = Outputs[Outputs.Length - 1].DataType, IsArray = true };
                }

                Outputs = newOutputs;
            }

            if (values.Length != Inputs.Length )
            {
                return new ReturnValue<object>(false, "The number of parameters input does not matching the number expected.", null);
            }
            for (int i = 0; i < values.Length; i++)
            {
                var result = Inputs[i].SetValue(values[i]);
                if (result.Success == false)
                    return new ReturnValue<object>(result);
            }

            return Invoke();
        }

 

        public ReturnValue<object> Invoke()
        {
            MethodInfo mappingFunction = FunctionMethod;
            try
            {
                int inputsCount = Inputs?.Length ?? 0;
                int outputsCount = 0;
                if (ResultMethod == null)
                    outputsCount = Outputs?.Length ?? 0;

                object[] parameters = new object[inputsCount + outputsCount];

                int parameterNumber = 0;

                List<object> arrayValues = null;
                ETypeCode arrayType = ETypeCode.String;
                for (int i = 0; i < inputsCount; i++)
                {
                    //FYI: this code will only accommodate for array being last parameter.
                    if (Inputs != null && Inputs[i].IsArray)
                    {
                        if (arrayValues == null)
                        {
                            arrayValues = new List<object>();
                            arrayType = Inputs[i].DataType;
                        }
                        var try1 = DataType.TryParse(Inputs[i].DataType, Inputs[i].Value);
                        if (try1.Success == false)
                            return try1;
                        arrayValues.Add(try1.Value);
                    }
                    else
                    {
                        parameters[parameterNumber] = Inputs[i].Value;
                        if (parameters[parameterNumber] == null || parameters[parameterNumber] is DBNull) parameters[parameterNumber] = null;
                        parameterNumber++;
                    }
                }

                if (arrayValues != null)
                {
                    //convert the values and load them into the parameter array.
                    switch (arrayType)
                    {
                        case ETypeCode.Byte:
                            parameters[parameterNumber] = arrayValues.Select(c=>(Byte)c).ToArray();
                            break;
                        case ETypeCode.SByte:
                            parameters[parameterNumber] = arrayValues.Select(c => (SByte)c).ToArray();
                            break;
                        case ETypeCode.UInt16:
                            parameters[parameterNumber] = arrayValues.Select(c => (UInt16)c).ToArray();
                            break;
                        case ETypeCode.UInt32:
                            parameters[parameterNumber] = arrayValues.Select(c => (UInt32)c).ToArray();
                            break;
                        case ETypeCode.UInt64:
                            parameters[parameterNumber] = arrayValues.Select(c => (UInt64)c).ToArray();
                            break;
                        case ETypeCode.Int16:
                            parameters[parameterNumber] = arrayValues.Select(c => (Int16)c).ToArray();
                            break;
                        case ETypeCode.Int32:
                            parameters[parameterNumber] = arrayValues.Select(c => (Int32)c).ToArray();
                            break;
                        case ETypeCode.Int64:
                            parameters[parameterNumber] = arrayValues.Select(c => (Int64)c).ToArray();
                            break;
                        case ETypeCode.Decimal:
                            parameters[parameterNumber] = arrayValues.Select(c => (Decimal)c).ToArray();
                            break;
                        case ETypeCode.Double:
                            parameters[parameterNumber] = arrayValues.Select(c => (Double)c).ToArray();
                            break;
                        case ETypeCode.Single:
                            parameters[parameterNumber] = arrayValues.Select(c => (Single)c).ToArray();
                            break;
                        case ETypeCode.String:
                            parameters[parameterNumber] = arrayValues.Select(c => (String)c).ToArray();
                            break;
                        case ETypeCode.Boolean:
                            parameters[parameterNumber] = arrayValues.Select(c => (Boolean)c).ToArray();
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
                    parameterNumber++;
                }

                int outputParameterNumber = parameterNumber;

                //if there is no resultfunction, then this function will require the output parameters
                if (ResultMethod == null)
                {
                    arrayValues = null;
                    for (int i = 0; i < outputsCount; i++)
                    {
                        //FYI: this code will only accommodate for array being last parameter.
                        if (Outputs != null && Outputs[i].IsArray)
                        {
                            if (arrayValues == null) arrayValues = new List<object>();
                            arrayValues.Add(null);
                        }
                        else
                        {
                            parameters[parameterNumber] = Outputs[i].Value;
                            if (parameters[parameterNumber] != null && parameters[parameterNumber].GetType() == typeof(DBNull)) parameters[parameterNumber] = null;

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

                try
                {
                    _returnValue = mappingFunction.Invoke(ObjectReference, parameters);
                }
                catch (Exception ex)
                {
                    throw new Exception("Error occurred running the custom function " + (FunctionName?? "") + ". The error message was: " + ex.Message + ".  Stacktrace: " + ex.StackTrace + ".  InnerException: " + ex.InnerException?.Message + ".");
                }

                if (ResultMethod == null)
                {
                    int arrayNumber = 0;
                    for (int i = 0; i < outputsCount; i++)
                    {

                        ReturnValue result1;

                        if (Outputs != null && Outputs[i].IsArray)
                        {
                            object[] parametersArray = (object[])parameters[outputParameterNumber];
                            if (parametersArray == null)
                                result1 = Outputs[i].SetValue(DBNull.Value);
                            else
                                result1 = Outputs[i].SetValue(arrayNumber >= parametersArray.Length ? DBNull.Value : parametersArray[arrayNumber]);

                            arrayNumber++;
                        }
                        else
                        {
                            result1 = Outputs[i].SetValue(parameters[outputParameterNumber]);
                            outputParameterNumber++;
                        }

                        if (result1.Success == false)
                            return new ReturnValue<object>(false, "Error setting return parameter: " + Outputs[i].Name + "=" + parameters[inputsCount + i] + ", message: " + result1.Message, null);
                    }
                }

                if (ReturnType == ETypeCode.Boolean && NotCondition)
                    _returnValue = !(bool)_returnValue;

                return new ReturnValue<object>(true, _returnValue);
            }
            catch (Exception ex)
            {
                return new ReturnValue<object>(false, "Error invoking function: "+ FunctionName, ex);
            }
        }

        /// <summary>
        /// Get the return value from an aggregate function.  
        /// </summary>
        /// <param name="index">Index represents result row number within the grouping, and is used for series functions that return multiple results from one aggregation.</param>
        /// <returns></returns>
        public ReturnValue<object> ReturnValue(int? index = 0)
        {
            if (ResultMethod != null)
            {
                try
                {
                    int outputsCount = Outputs?.Length ?? 0;
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
                    for (int i = 0; i < outputsCount; i++)
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

                    int arrayNumber = 0;
                    for (int i = 0; i < outputsCount; i++)
                    {
                        ReturnValue result;

                        if (Outputs != null && Outputs[i].IsArray)
                        {
                            object[] array = (object[])parameters[i + indexAdjust];
                            result = Outputs[i].SetValue(arrayNumber >= array.Length ? DBNull.Value : array[arrayNumber]);
                            arrayNumber++;
                        }
                        else
                        {
                            result = Outputs[i].SetValue(parameters[i + indexAdjust]);
                        }

                        if (result.Success == false)
                            return new ReturnValue<object>(false, "Error setting return parameter: " + Outputs[i].Name + "=" + parameters[i + 1] + ", message: " + result.Message, null);
                    }
                }
                catch (Exception ex)
                {
                    return new ReturnValue<object>(false, "Error occurred getting a result from the custom function " + FunctionName + ". The error message was: " + ex.InnerException.Message + ".", ex);
                }
            }
            return new ReturnValue<object>(true, _returnValue);
        }

        public ReturnValue Reset()
        {
            try
            {
                //var mappingFunction = CreateFunctionMethod();
                //if(mappingFunction.Success == false)
                //    return mappingFunction;

                ResetMethod.Invoke(ObjectReference, null);
                return new ReturnValue(true); 
            }
            catch(Exception ex)
            {
                return new ReturnValue(false, "The function could not be reset.  Message: " + ex.Message, ex);
            }
        }

        public string FunctionDetail()
        {
            string detail = GetType() + " ( ";
            for (int i = 0; i < Inputs.Length; i++)
                detail += Inputs[i].Name + "=" + (Inputs[i].Value == null ? "null" : Inputs[i].Value.ToString()) + (i < Inputs.Length - 1 ? "," : ")");
            return detail;
        }
    }


}
