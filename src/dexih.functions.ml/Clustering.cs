using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using dexih.functions.Exceptions;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace dexih.functions.ml
{
    public class Clustering
    {
        private DynamicList _dynamicList;
        private Prediction _prediction;


        public class ClusterPrediction
        {
            public uint PredictedLabel { get; set; }
            public float[] Score { get; set; }
        }

        public class ClusterLabelPrediction
        {
            public uint PredictedLabel { get; set; }
            public string Data { get; set; }
            public float[] Score { get; set; }
        }
        
        public void Reset()
        {
            _dynamicList = null;
            _prediction = null;
        }

        public string[] ImportModelLabels(byte[] clusteringModel)
        {
            if (clusteringModel == null)
            {
                throw new FunctionException("The clusteringModel is set to null, ensure that this is specified before selecting import.");
            }
            
            var mlContext = new MLContext();
            using (var stream = new MemoryStream(clusteringModel))
            {
                var mlModel = mlContext.Model.Load(stream, out var inputSchema);
                var columns = inputSchema.Select(c => c.Name).Where(c=> c != "Label").ToArray();
                
                return columns;
            }
        }

        public void AddData(string[] labels, float[] values)
        {
            if (_dynamicList == null)
            {
                _dynamicList = new DynamicList(labels, typeof(Single));
            }
            
            _dynamicList.Add(values.Select(c=> (object) Convert.ToSingle(c)).ToArray());
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Clustering (K-Means) Analysis - Train", Description = "Builds a model using k-means clustering based on the training data.", ResultMethod = nameof(ClusteringKMeansTrainResult), ResetMethod = nameof(Reset))]
        public void ClusteringKMeansTrain([TransformFunctionParameterTwin, ParameterLabel] string[] itemLabel, [TransformFunctionParameterTwin] float[] itemValue)
        {
            AddData(itemLabel, itemValue);
        }
        
        public byte[] ClusteringKMeansTrainResult(int numberOfClusters)
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();
            var trainData = _dynamicList.GetDataView(mlContext);

            var featuresColumnName = "Features";
            var pipeline = mlContext.Transforms
                .Concatenate(featuresColumnName, _dynamicList.Labels)
                .Append(mlContext.Clustering.Trainers.KMeans(featuresColumnName, numberOfClusters: numberOfClusters));
            
            var trainedModel = pipeline.Fit(trainData);
            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Clustering (K-Means) Analysis - Evaluate", Description = "Evaluates the accuracy of a model using k-means clustering based on the training data.", ResultMethod = nameof(ClusteringKMeansEvaluateResult), ResetMethod = nameof(Reset))]
        public void ClusteringKMeansEvaluate([TransformFunctionParameterTwin, ParameterLabel] string[] itemLabel, [TransformFunctionParameterTwin] float[] itemValue)
        {
            AddData(itemLabel, itemValue);
        }
        
        public ClusteringMetrics ClusteringKMeansEvaluateResult()
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();
            var trainData = _dynamicList.GetDataView(mlContext);

            var metrics = mlContext.Clustering.Evaluate(trainData);
            return metrics;
        }

        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Machine Learning", Name = "Clustering (K-Means) Analysis - Predict", Description = "Predicts a value using k-means clustering based on the training data.", ResetMethod = nameof(Reset), ImportMethod = nameof(ImportModelLabels))]
        public ClusterPrediction ClusteringKMeansPredict(byte[] clusteringModel, [TransformFunctionParameterTwin, ParameterLabel] string[] label, [TransformFunctionParameterTwin] float[] value)
        {
            if (_prediction == null)
            {
                _prediction = new Prediction(typeof(ClusterPrediction), clusteringModel, label);
            }

            return (ClusterPrediction) _prediction.Run(value);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Clustering (Stochastic Dual Coordinate Ascent) Analysis - Train", Description = "Builds a model using Stochastic Dual Coordinate Ascent clustering based on the training data.", ResultMethod = nameof(ClusteringSdcaTrainResult), ResetMethod = nameof(Reset))]
        public void ClusteringSdcaTrain(string label, [TransformFunctionParameterTwin, ParameterLabel] string[] itemLabel, [TransformFunctionParameterTwin] float[] itemValue)
        {
            AddData(itemLabel, itemValue);
        }
        
        public byte[] ClusteringSdcaTrainResult()
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();
            var trainData = _dynamicList.GetDataView(mlContext);

            var pipeline = mlContext.Transforms
                .Concatenate("Features", _dynamicList.Labels)
                .Append(mlContext.Transforms.Conversion.MapValueToKey("Label"), TransformerScope.TrainTest)
                .Append(mlContext.MulticlassClassification.Trainers.SdcaMaximumEntropy())
                .Append(mlContext.Transforms.Conversion.MapKeyToValue("Data", "PredictedLabel"));
            
            var trainedModel = pipeline.Fit(trainData);
            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
        }    
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Clustering (Stochastic Dual Coordinate Ascent) Analysis - Evaluate", Description = "Evaluates a model using Stochastic Dual Coordinate Ascent clustering based on the training data.", ResultMethod = nameof(ClusteringSdcaEvaluateResult), ResetMethod = nameof(Reset), ImportMethod = nameof(ImportModelLabels))]
        public void ClusteringSdcaEvaluate(string label, [TransformFunctionParameterTwin,  ParameterLabel] string[] itemLabel, [TransformFunctionParameterTwin] float[] itemValue)
        {
            AddData(itemLabel, itemValue);
        }
        
        public MulticlassClassificationMetrics ClusteringSdcaEvaluateResult()
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();
            var trainData = _dynamicList.GetDataView(mlContext);

            var metrics = mlContext.MulticlassClassification.Evaluate(trainData);
            return metrics;
        } 
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Machine Learning", Name = "Clustering (Stochastic Dual Coordinate Ascent) Analysis - Predict", Description = "Predicts a value using Stochastic Dual Coordinate Ascent clustering based on the training data.", ResetMethod = nameof(Reset), ImportMethod = nameof(ImportModelLabels))]
        public ClusterLabelPrediction ClusteringSdcaPredict(byte[] clusteringModel, [TransformFunctionParameterTwin, ParameterLabel] string[] label, [TransformFunctionParameterTwin] float[] value)
        {
            if (_prediction == null)
            {
                _prediction = new Prediction(typeof(ClusterPrediction), clusteringModel, label);
            }

            return (ClusterLabelPrediction) _prediction.Run(value);
        }
    }
}