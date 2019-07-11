using System;
using System.Linq;
using System.Reflection;
using dexih.functions.Exceptions;
using Microsoft.ML;

namespace dexih.functions.ml
{
    public class RegressionPredictor
    {
        // instance of the prection engine.
        private readonly object _predictionEngine;
        
        // method to run the "predict"
        private readonly MethodInfo _predictMethod;
        private readonly Type _dataType;

        /// <summary>
        /// Extracts the "Predict" method from the "CreatePredictionEngine".
        /// This requires reflection as the "type" is unknown prior to compilation.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="modelBytes"></param>
        /// <param name="labels"></param>
        public RegressionPredictor(Type type, byte[] modelBytes, string[] labels)
        {
            var mlContext = new MLContext();
            var model = Helpers.LoadModel(mlContext, modelBytes, out var schema);

            var fields = labels.Select(label =>
            {
                var column = schema.GetColumnOrNull(label);

                if (column is null)
                {
                    throw new FunctionException(
                        $"The input label {label} does not have a matching field in the model.  Ensure the field names are one of the following: {string.Join(", ", schema.Select(c => c.Name))}");
                }
                
                return new DynamicTypeProperty(column.Value.Name, column.Value.Type.RawType);
            }).Append(new DynamicTypeProperty("PredictedLabel", typeof(float))).ToList();
            
            _dataType = DynamicType.CreateDynamicType(fields);
            
            var createPredictionEngineBase = mlContext.Model.GetType().GetMethods().First(c => c.Name == "CreatePredictionEngine" && c.IsGenericMethod && c.GetParameters().Length == 4);
            var createPredictionEngine = createPredictionEngineBase.MakeGenericMethod(_dataType, type);
            _predictionEngine = createPredictionEngine.Invoke(mlContext.Model, new object[] {model, true, null, null});
            _predictMethod = _predictionEngine.GetType().GetMethods().First(c => c.Name == "Predict" && c.GetParameters().Length == 1);
        }

        public object Run(object[] values)
        {
            var data = DynamicType.CreateDynamicItem(_dataType, values);
            var prediction = _predictMethod.Invoke(_predictionEngine, new[] {data});
            return prediction;
        }
    }
}