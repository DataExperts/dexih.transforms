using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using dexih.functions.Exceptions;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace dexih.functions.ml
{
    public class SentimentAnalysis
    {

        private PredictionEngine<SentimentIssue, SentimentPredictionResult> _predictionFunction;

        private MLContext _mlContext;
        private List<SentimentIssue> _data;
        private byte[] _sentimentModel;

        public void Reset()
        {
            _mlContext = new MLContext();
            _data = new List<SentimentIssue>();
        }

        private class SentimentIssue
        {
            public SentimentIssue(bool label, string text)
            {
                Label = label;
                Text = text;
            }

            public bool Label { get; set; }
            public string Text { get; set; }
        }
        
        public class SentimentPredictionResult
        {
            [ColumnName("PredictedLabel")]
            public bool Prediction { get; set; }

            [ColumnName("Probability")]
            public float Probability { get; set; }

            [ColumnName("Score")]
            public float Score { get; set; }
        }

       
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Sentiment Analysis - Train", Description = "Builds a sentiment analysis model based on the training data.", ResultMethod = nameof(SentimentTrainResult), ResetMethod = nameof(Reset))]
        public void SentimentTrain(bool classification, string text)
        {
            _data.Add(new SentimentIssue(classification, text));
        }
        
        public byte[] SentimentTrainResult()
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();

            // Turn the data into the ML.NET data view.
            var trainData = mlContext.Data.LoadFromEnumerable(_data);
            var pipeline = mlContext.Transforms.Text.FeaturizeText( "Features", "Text").Append(mlContext.BinaryClassification.Trainers.FastTree());
            var trainedModel = pipeline.Fit(trainData);

            using (var stream = new MemoryStream())
            {
                mlContext.Model.Save(trainedModel, null, stream);
                return stream.ToArray();
            }
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Sentiment Analysis - Evaluate", Description = "Evaluates the prediction accuracy of sentiment analysis model based on the training data.", ResultMethod = nameof(SentimentEvaluateResult), ResetMethod = nameof(Reset))]
        public void SentimentEvaluate(bool classification, string text)
        {
            _data.Add(new SentimentIssue(classification, text));
        }
        
        public CalibratedBinaryClassificationMetrics SentimentEvaluateResult(byte[] sentimentModel)
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();

            // load the sentiment model
            var stream = new MemoryStream( sentimentModel );
            var trainedModel = mlContext.Model.Load(stream, out var inputSchema);

            // Turn the data into the ML.NET data view.
            var trainData = mlContext.Data.LoadFromEnumerable(_data);

            // run the evaluation
            var predictions = trainedModel.Transform(trainData);
            var metrics = mlContext.BinaryClassification.Evaluate(predictions);

            return metrics;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Machine Learning", Name = "Sentiment Analysis - Prediction", Description = "Predicts the sentiment based on the input model produced by the \"Sentiment Analysis - Train\" aggregate function") ]
        public bool SentimentPrediction(byte[] sentimentModel, string text, out double probability, out double score)
        {
            if (_sentimentModel == null || !_sentimentModel.SequenceEqual(sentimentModel))
            {
                _sentimentModel = sentimentModel;
                var mlContext = new MLContext();
                var stream = new MemoryStream( sentimentModel );
                var model = mlContext.Model.Load(stream, out var inputSchema);
                _predictionFunction = mlContext.Model.CreatePredictionEngine<SentimentIssue, SentimentPredictionResult>(model);
            }

            var sentimentIssue = new SentimentIssue(false, text);
            var result = _predictionFunction.Predict(sentimentIssue);

            probability = result.Probability;
            score = result.Score;
            return result.Prediction;
        }
        

    }
}