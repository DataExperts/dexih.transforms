using System;
using System.Linq;
using System.Reflection;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace dexih.functions.ml
{
    public class TransactionPrediction
    {
        [ColumnName("PredictedLabel")]
        public string PredictedLabel;
    }
    
    public class Prediction
    {
        // instance of the prediction engine.
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
        /// <param name="encodings"></param>
        public Prediction(Type type, byte[] modelBytes, string[] labels, EEncoding[] encodings)
        {
            var fields = labels.Select((label, index) => Helpers.NewDynamicTypeProperty(label, encodings[index]));

            if (!labels.Contains("Label"))
            {
                fields = fields.Append(Helpers.NewDynamicTypeProperty("Label", EEncoding.Label));
            }
            
            _dataType = DynamicType.CreateDynamicType(fields);
            
            var mlContext = new MLContext();
            var model = Helpers.LoadModel(mlContext, modelBytes, out _);
            
            var createPredictionEngineBase = mlContext.Model.GetType().GetMethods().First(c => c.Name == "CreatePredictionEngine" && c.IsGenericMethod && c.GetParameters().Length == 4);
            var createPredictionEngine = createPredictionEngineBase.MakeGenericMethod(_dataType, type);
            _predictionEngine = createPredictionEngine.Invoke(mlContext.Model, new object[] {model, true, null, null});
            _predictMethod = _predictionEngine.GetType().GetMethods().First(c => c.Name == "Predict" && c.GetParameters().Length == 1);
        }

        public Prediction(Type type, Type dataType, MLContext mlContext, ITransformer model)
        {
            _dataType = dataType;
            var createPredictionEngineBase = mlContext.Model.GetType().GetMethods().First(c => c.Name == "CreatePredictionEngine" && c.IsGenericMethod && c.GetParameters().Length == 4);
            var createPredictionEngine = createPredictionEngineBase.MakeGenericMethod(_dataType, type);
            _predictionEngine = createPredictionEngine.Invoke(mlContext.Model, new object[] {model, true, null, null});
            _predictMethod = _predictionEngine.GetType().GetMethods().First(c => c.Name == "Predict" && c.GetParameters().Length == 1);
        }

        public T Run<T>(object[] values)
        {
            var data = DynamicType.CreateDynamicItem(_dataType, values);
            var parameters = new[] {data};
            var prediction = (T) _predictMethod.Invoke(_predictionEngine, parameters);
            return prediction;
        }
        
        public T Run<T>(object data)
        {
            var parameters = new[] {data};
            var prediction = (T) _predictMethod.Invoke(_predictionEngine, parameters);
            return prediction;
        }
    }
    
}