using System;
using System.Linq;
using System.Reflection;
using Microsoft.ML;

namespace dexih.functions.ml
{
    public class Prediction
    {
        // instance of the prection engine.
        private readonly object _predictionEngine;
        
        // method to run the "predict"
        private readonly MethodInfo _predictMethod;
        private Type _dataType;
        

        /// <summary>
        /// Extracts the "Predict" method from the "CreatePredictionEngine".
        /// This requires reflection as the "type" is unknown prior to compilation.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="modelBytes"></param>
        /// <param name="labels"></param>
        public Prediction(Type type, byte[] modelBytes, string[] labels)
        {
            var fields = labels.Select((c, index) => new DynamicTypeProperty(c, typeof(Single))).ToList();
            var _dataType = DynamicType.CreateDynamicType(fields);
            
            var mlContext = new MLContext();
            var model = Helpers.LoadModel(mlContext, modelBytes, out _);
            var createPredictionEngineBase = mlContext.Model.GetType().GetMethods().First(c => c.Name == "CreatePredictionEngine" && c.IsGenericMethod && c.GetParameters().Length == 4);
            var createPredictionEngine = createPredictionEngineBase.MakeGenericMethod(_dataType, type);
            _predictionEngine = createPredictionEngine.Invoke(mlContext.Model, new object[] {model, true, null, null});
            _predictMethod = _predictionEngine.GetType().GetMethods().First(c => c.Name == "Predict" && c.GetParameters().Length == 1);
        }

        public object Run(float[] values)
        {
            var data = DynamicType.CreateDynamicItem(_dataType, values);
            var prediction = _predictMethod.Invoke(_predictionEngine, new[] {data});
            return prediction;
        }
    }
}