using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System.IO;
using Microsoft.CodeAnalysis.Emit;
using static dexih.functions.DataType;
using System.Collections;
#if NET451
#else
using System.Runtime.Loader;
#endif

namespace dexih.functions
{
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
        protected MethodInfo _functionMethod;
        protected MethodInfo _resetMethod;
        protected MethodInfo _resultMethod;

        object _returnValue;
        object _objectReference;

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
        /// C# custom code executed for everyrow.
        /// </summary>
        public string FunctionCode { get; set; }

        /// <summary>
        /// C# custom code used for aggregate functions that calculates the final result.
        /// </summary>
        public string FunctionResultCode { get; set; }

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

        public string TargetColumn { get; set; } //default column to map return type to

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
        

        public Function(string targetColumn, bool isStandardFunction, string functionName, string functionCode, string functionResultCode, ETypeCode returnType, Parameter[] inputs, Parameter[] outputs)
        {
            TargetColumn = targetColumn;
            FunctionName = functionName;
            FunctionCode = functionCode;
            FunctionResultCode = functionResultCode;

            Inputs = inputs;
            Outputs = outputs;
            ReturnType = returnType;
        }

        /// <summary>
        /// Createa a new function from a "Delegate".
        /// </summary>
        /// <param name="functionMethod">Reference to the function that will be executed.</param>
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
        public Function(Delegate functionMethod, string[] inputMappings, string targetColumn, string[] outputMappings) :
            this(functionMethod.Target, functionMethod.GetMethodInfo(), inputMappings, targetColumn, outputMappings)
        {
        }

        /// <summary>
        /// Creates a new function from a class/method reference.
        /// </summary>
        /// <param name="targetType">Type of the class which contains the method.  This class must contain a parameterless constructor.</param>
        /// <param name="MethodName">The name of the method to call.</param>
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
        public Function(Type targetType, string MethodName, string[] inputMappings, string targetColumn, string[] outputMappings)
        {
            FunctionName = MethodName;
            Initialize(Activator.CreateInstance(targetType), targetType.GetMethod(MethodName), inputMappings, targetColumn, outputMappings);
        }

        /// <summary>
        /// Creates a new function from a class/method reference.
        /// </summary>
        /// <param name="targetType">Type of the class which contains the method.  This class must contain a parameterless constructor.</param>
        /// <param name="MethodName">The name of the method to call.</param>
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
        public Function(Type targetType, string MethodName, string ResultMethodName, string ResetMethodName, string[] inputMappings, string targetColumn, string[] outputMappings)
        {
            FunctionName = MethodName;
            _resultMethod = targetType.GetMethod(ResultMethodName);
            _resetMethod = targetType.GetMethod(ResetMethodName);

            Initialize(Activator.CreateInstance(targetType), targetType.GetMethod(MethodName), inputMappings, targetColumn, outputMappings);
        }

        /// <summary>
        /// Creates a new function from a class/method reference.
        /// </summary>
        /// <param name="Target">An instantiated instance of the class containing the method.  Ensure a new instance of Target is created for each function to avoid issues with cached data.</param>
        /// <param name="MethodName">The name of the method to call.</param>
        /// <param name="inputMappings">The input column names to be mapped in the transform.</param>
        /// <param name="targetColumn">The column for the return value of the function to be mapped to.</param>
        /// <param name="outputMappings">The columns for any "out" parameters in the function to be mapped to.</param>
        public Function(object Target, string MethodName, string ResultMethodName, string ResetMethodName, string[] inputMappings, string targetColumn, string[] outputMappings)
            
        {
            FunctionName = MethodName;
            _resultMethod = Target.GetType().GetMethod(ResultMethodName);
            _resetMethod = Target.GetType().GetMethod(ResetMethodName);

            Initialize(Target, Target.GetType().GetMethod(MethodName), inputMappings, targetColumn, outputMappings);
        }

        public Function(object Target, MethodInfo FunctionMethod, string[] inputMappings, string targetColumn, string[] outputMappings)
        {
            Initialize(Target, FunctionMethod, inputMappings, targetColumn, outputMappings);
        }

        private void Initialize(object Target, MethodInfo FunctionMethod, string[] inputMappings, string targetColumn, string[] outputMappings)
        {
            _functionMethod = FunctionMethod;
            _objectReference = Target;

            ReturnType = GetTypeCode(_functionMethod.ReturnType);
            ParameterInfo[] inputParameters = _functionMethod.GetParameters().Where(c => !c.IsOut).ToArray();

            if (inputMappings == null)
                inputMappings = new string[inputParameters.Length];

            Inputs = new Parameter[inputMappings.Length];

            int parameterCount = 0;
            for (int i = 0; i < inputMappings.Length; i++)
            {
                if (parameterCount > inputParameters.Length)
                {
                    throw new Exception("The input parameters could not be intialized as there are " + inputMappings.Length + " input mappings, however the function only has " + inputParameters.Length + " input parameters.");
                }

                Inputs[i] = new Parameter();
                Inputs[i].ColumnName = inputMappings[i];
                Inputs[i].Name = inputParameters[parameterCount].Name;
                Inputs[i].IsColumn = true;

                Type parameterType = inputParameters[parameterCount].ParameterType;
                Inputs[i].IsArray = parameterType.IsArray;
                if(parameterType.IsArray)
                    Inputs[i].DataType = GetTypeCode(parameterType.GetElementType());
                else
                    Inputs[i].DataType = GetTypeCode(parameterType);

                if (Inputs[i].DataType == ETypeCode.Unknown)
                {
                    throw new Exception("The datatype: " + inputParameters[i].GetType().ToString() + " for parameter " + inputParameters[i].Name + " is not a supported datatype.");
                }

                //when an array is found in a method, all parameters are mapped to this.  
                if (!parameterType.IsArray) parameterCount++;
            }

            ParameterInfo[] outputParameters;

            if (_resultMethod == null)
                outputParameters = _functionMethod.GetParameters().Where(c => c.IsOut).ToArray();
            else
                outputParameters = _resultMethod.GetParameters().Where(c => c.IsOut).ToArray();

            parameterCount = 0;
            if (outputParameters.Length > 0)
            {
                Outputs = new Parameter[outputParameters.Length];

                if (outputMappings == null)
                    outputMappings = new string[outputParameters.Length];

                for (int i = 0; i < outputMappings.Length; i++)
                {
                    if (parameterCount > inputParameters.Length)
                    {
                        throw new Exception("The output parameters could not be intialized as there are " + outputMappings.Length + " output mappings, however the function only has " + outputParameters.Length + " output parameters.");
                    }

                    Outputs[i] = new Parameter();
                    Outputs[i].ColumnName = outputMappings[i];
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

        /// <summary>
        /// this function is used to dump the current function call to a program which can be debugged.
        /// </summary>
        /// <returns></returns>
        public string GetTestCode()
        {
            string code = CreateFunctionCode();

            string parameter = "";

            int inputsCount = Inputs?.Length ?? 0;
            int outputsCount = Outputs?.Length ?? 0;

            for (int i = 0; i < inputsCount; i++)
            {
                if (Inputs != null && DataType.GetBasicType(Inputs[i].DataType) == DataType.EBasicType.Numeric)
                    parameter = parameter + Inputs[i].Value + ",";
                else
                    parameter = parameter + "\"" + Inputs[i].Value + "\",";
            }

            for (int i = 0; i < outputsCount; i++)
            {
                if (Inputs != null && DataType.GetBasicType(Inputs[i].DataType) == DataType.EBasicType.Numeric)
                    parameter = "out " + parameter + Inputs[i].Value + ",";
                else
                    parameter = "out " + parameter + "\"" + Inputs[i].Value + "\",";
            }

            if (parameter != "")
                parameter = parameter.Substring(0, parameter.Length - 1);

            code = code.Replace("//TestReplace", "Console.WriteLine(CustomFunction(" + parameter + "));");

            return code;
        }

        /// <summary>
        /// Generates the function code using the custom code.
        /// </summary>
        /// <returns></returns>
        public string CreateFunctionCode()
        {
            StringBuilder code = new StringBuilder();
            code.Append(@"
using System;
using System.Collections;

public class Program
{
	static int? CacheInt;
	static double? CacheDouble;
	static string CacheString;
	static Hashtable CacheHashtable;

	public static void Main()
	{
        //To test, uncomment line below and update parameters to test function
		//TestReplace
    }

    public static $FunctionReturn CustomFunction($Parameters)
    {
        $FunctionCode
    }

    public static bool Reset()
    {
        CacheInt = null;
        CacheDouble = null;
        CacheString = null;
        CacheHashtable = null;
        return true;
    }
}
                    ");

            code.Replace("$FunctionCode", FunctionCode);
            code.Replace("$FunctionReturn", ReturnType.ToString());

            string parameterString = "";
            if (Inputs != null)
            {
                foreach (Parameter t in Inputs)
                {
                    string addArray = "";
                    if (t.IsArray) addArray = "[]";
                    parameterString += t.DataType + addArray + " " + t.Name + ",";
                }
            }

            if (Outputs != null)
            {
                foreach (Parameter t in Outputs)
                {
                    string addArray = "";
                    if (t.IsArray) addArray = "[]";
                    parameterString += "out " + t.DataType + addArray + " " + t.Name + ",";
                }
            }

            if (parameterString != "") //remove last comma
                parameterString = parameterString.Substring(0, parameterString.Length - 1);

            code.Replace("$Parameters", parameterString);

            return code.ToString();
        }

        /// <summary>
        /// Creates a reference to a compiled version of the mapping function.
        /// </summary>
        /// <returns></returns>
        public ReturnValue<MethodInfo> CreateFunctionMethod()
        {
            if (_functionMethod == null)
            {
                try
                {
                    string code = CreateFunctionCode();
                    var syntaxTree = CSharpSyntaxTree.ParseText(code);

                    MetadataReference[] references = new MetadataReference[]
                    {
                        MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
                        MetadataReference.CreateFromFile(typeof(Hashtable).GetTypeInfo().Assembly.Location)
                    }; 

                        var compilation = CSharpCompilation.Create("Function" + Guid.NewGuid().ToString() + ".dll",
                        syntaxTrees: new[] { syntaxTree },
                        references: references,
                        options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

                    StringBuilder message = new StringBuilder();

                    using (var ms = new MemoryStream())
                    {
                        EmitResult result = compilation.Emit(ms);

                        if (!result.Success)
                        {
                            IEnumerable<Diagnostic> failures = result.Diagnostics.Where(diagnostic =>
                                diagnostic.IsWarningAsError ||
                                diagnostic.Severity == DiagnosticSeverity.Error);

                            foreach (Diagnostic diagnostic in failures)
                            {
                                message.AppendFormat("{0}: {1}", diagnostic.Id, diagnostic.GetMessage());
                            }

                            return new ReturnValue<MethodInfo>(false, "The following compile errors were encountered: " + message.ToString(), null);
                        }
                        else
                        {
                                
                            ms.Seek(0, SeekOrigin.Begin);

#if NET451
                            Assembly assembly = Assembly.Load(ms.ToArray());
#else
                            AssemblyLoadContext context = AssemblyLoadContext.Default;
                            Assembly assembly = context.LoadFromStream(ms);
#endif

                            Type mappingFunction = assembly.GetType("Program");
                            _functionMethod = mappingFunction.GetMethod("CustomFunction");
                            _resetMethod = mappingFunction.GetMethod("Reset");
                        }
                    }
                }
                catch (Exception ex)
                {
                    return new ReturnValue<MethodInfo>(false, "The following exception was encountered when compiling the function: " + ex.Message, ex);
                }
            }
            return new ReturnValue<MethodInfo>(true, _functionMethod);
        }

        public ReturnValue<object> Invoke()
        {
            var result = CreateFunctionMethod();
            if (result.Success == false)
                return new ReturnValue<object>(result);

            MethodInfo mappingFunction = result.Value;
            try
            {
                int inputsCount = Inputs?.Length ?? 0;
                int outputsCount = 0;
                if (_resultMethod == null)
                    outputsCount = Outputs?.Length ?? 0;

                object[] parameters = new object[inputsCount + outputsCount];

                int parameterNumber = 0;

                List<object> arrayValues = null;
                for (int i = 0; i < inputsCount; i++)
                {
                    //FYI: this code will only accommodate for array being last parameter.
                    if (Inputs != null && Inputs[i].IsArray)
                    {
                        if (arrayValues == null) arrayValues = new List<object>();
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
                    parameters[parameterNumber] = arrayValues.Select(c => c?.ToString()).ToArray();
                    parameterNumber++;
                }

                int outputParameterNumber = parameterNumber;

                //if there is no resultfunction, then this function will require the output parameters
                if (string.IsNullOrEmpty(FunctionResultCode))
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
                            parameters[parameterNumber] = null; // Outputs[i].Value;
                                                                //if (Parameters[ParameterNumber].GetType() == typeof(DBNull)) Parameters[ParameterNumber] = null;
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
                    _returnValue = mappingFunction.Invoke(_objectReference, parameters);
                }
                catch (Exception ex)
                {
                    throw new Exception("Error occurred running the custom function " + (FunctionName?? "") + ". The error message was: " + ex.Message + ".  Stacktrace: " + ex.StackTrace + ".  InnerException: " + ex.InnerException?.Message + ".");
                }

                if (string.IsNullOrEmpty(FunctionResultCode))
                {
                    int arrayNumber = 0;
                    for (int i = 0; i < outputsCount; i++)
                    {

                        ReturnValue result1;

                        if (Outputs != null && Outputs[i].IsArray)
                        {
                            object[] parametersArray = (object[])parameters[outputParameterNumber];
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
                ex.Source = CreateFunctionCode();
                //Clipboard.SetText(GetTestCode());
                return new ReturnValue<object>(false, "Error invoking function: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Get the return value from an aggregate function.  
        /// </summary>
        /// <param name="index">Index represents result row, and is used for series functions that return multiple results from one aggregation.</param>
        /// <returns></returns>
        public ReturnValue<object> ReturnValue(int? index = 0)
        {
            if (_resultMethod != null)
            {
                try
                {
                    int outputsCount = Outputs?.Length ?? 0;
                    int indexAdjust;
                    object[] parameters;

                    //if the result method has an "index" as the first parameter, then add the index
                    if (_resultMethod.GetParameters().Count() > 0 && _resultMethod.GetParameters()[0].Name == "index")
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

                    _returnValue = _resultMethod.Invoke(_objectReference, parameters);

                    int arrayNumber = 0;
                    for (int i = 0; i < outputsCount; i++)
                    {
                        ReturnValue result;

                        if (Outputs != null && Outputs[i].IsArray)
                        {
                            object[] Array = (object[])parameters[i + indexAdjust];
                            result = Outputs[i].SetValue(arrayNumber >= Array.Length ? DBNull.Value : Array[arrayNumber]);
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
                var mappingFunction = CreateFunctionMethod();
                if(mappingFunction.Success == false)
                    return mappingFunction;

                _resetMethod.Invoke(_objectReference, null);
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
