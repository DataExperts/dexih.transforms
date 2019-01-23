using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace dexih.functions.ml
{
    public class SentimentAnalysis
    {

        private List<SentimentIssue> _data;
        private PredictionEngine<SentimentIssue, SentimentPredictionResult> _predictionFunction;

        private byte[] _sentimentModel;

        public void Reset()
        {
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
        
        public byte[] SentimentTrainResult( out double entropy, out double logLoss, out double logLossReduction, out double accuracy, out double auc, out double auprc, out double f1score, out double negativePrecision, out double negativeRecall, out double positivePrecision, out double positiveRecall)
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();

            // Turn the data into the ML.NET data view.
            var trainData = mlContext.CreateDataView(_data);

            var pipeline = mlContext.Transforms.Text.FeaturizeText("Text", "Features")   
                .Append(mlContext.BinaryClassification.Trainers.FastTree());

            var trainedModel = pipeline.Fit(trainData);
            
            var predictions = trainedModel.Transform(trainData);
            var metrics = mlContext.BinaryClassification.Evaluate(predictions);

            entropy = metrics.Entropy;
            logLoss = metrics.LogLoss;
            logLossReduction = metrics.LogLossReduction;
            accuracy = metrics.Accuracy;
            auc = metrics.Auc;
            auprc = metrics.Auprc;
            f1score = metrics.F1Score;
            negativePrecision = metrics.NegativePrecision;
            negativeRecall = metrics.NegativeRecall;
            positivePrecision = metrics.PositivePrecision;
            positiveRecall = metrics.PositiveRecall;

            using (var stream = new MemoryStream())
            {
                mlContext.Model.Save(trainedModel, stream);
                return stream.ToArray();
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Machine Learning", Name = "Sentiment Analysis - Prediction", Description = "Predicts the sentiment based on the input model produced by the \"Sentiment Analysis - Train\" function") ]
        public bool SentimentPrediction(byte[] sentimentModel, string text, out double probability, out double score)
        {
            if (_sentimentModel == null || !_sentimentModel.SequenceEqual(sentimentModel))
            {
                _sentimentModel = sentimentModel;
                var mlContext = new MLContext();
                var stream = new MemoryStream( sentimentModel );
                var model = mlContext.Model.Load(stream);
                _predictionFunction = model.CreatePredictionEngine<SentimentIssue, SentimentPredictionResult>(mlContext);
            }

            var sentimentIssue = new SentimentIssue(false, text);
            var result = _predictionFunction.Predict(sentimentIssue);

            probability = result.Probability;
            score = result.Score;
            return result.Prediction;
        }
    }
}