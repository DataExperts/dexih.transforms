using System.Collections.Generic;
using System.IO;
using System.Linq;
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

       
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Sentiment Analysis (FastTree) - Train", Description = "Builds a sentiment analysis model using FastTree analysis based on the training data.", ResultMethod = nameof(SentimentFastTreeTrainResult), ResetMethod = nameof(Reset))]
        public void SentimentFastTreeTrain(
            [TransformFunctionParameter(Name = "Classification", Description = "The known sentiment classification." )] bool classification, 
            [TransformFunctionParameter(Name = "Text", Description = "The sentiment text." )]string text)
        {
            if (_data == null)
            {
                _data  = new List<SentimentIssue>();
            }
            
            _data.Add(new SentimentIssue(classification, text));
        }
        
        public byte[] SentimentFastTreeTrainResult(
            [TransformFunctionParameter(Name = "Number of leaves", Description = "The maximum number of leaves per decision tree." )]int numberOfLeaves = 20,
            [TransformFunctionParameter(Name = "Number of trees", Description = "Total number of decision trees to create in the ensemble" )]int numberOfTrees = 100,
            [TransformFunctionParameter(Name = "Min Example Count Per Leaf", Description = "The minimal number of data points required to form a new tree leaf." )]int minimumExampleCountPerLeaf = 10,
            [TransformFunctionParameter(Name = "Learning Rate", Description = "The learning rate." )]double learningRate = 0.2
            )
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();

            // Turn the data into the ML.NET data view.
            var trainData = mlContext.Data.LoadFromEnumerable(_data);
            var trainer = mlContext.BinaryClassification.Trainers.FastTree(labelColumnName:"Label", numberOfLeaves: numberOfLeaves, learningRate: learningRate, numberOfTrees: numberOfLeaves, minimumExampleCountPerLeaf: minimumExampleCountPerLeaf);
            var pipeline = mlContext.Transforms.Text.FeaturizeText( "Features", "Text").Append(trainer);
            var trainedModel = pipeline.Fit(trainData);
            
            using (var stream = new MemoryStream())
            {
                mlContext.Model.Save(trainedModel, null, stream);
                return stream.ToArray();
            }
        }
        
                [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Sentiment Analysis - Train", Description = "Builds a sentiment analysis model based on the training data.", ResultMethod = nameof(SentimentSdcaRegressionTrainResult), ResetMethod = nameof(Reset))]
        public void SentimentSdcaRegressionTrain(
            [TransformFunctionParameter(Name = "Classification", Description = "The known sentiment classification." )] bool classification, 
            [TransformFunctionParameter(Name = "Text", Description = "The sentiment text." )]string text)
        {
            if (_data == null)
            {
                _data  = new List<SentimentIssue>();
            }
            
            _data.Add(new SentimentIssue(classification, text));
        }
        
        public byte[] SentimentSdcaRegressionTrainResult(
            [TransformFunctionParameter(Description = "The L2 weight for [regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics))")] int? l1Regularization = null,
            [TransformFunctionParameter(Description = "The L1 weight for [regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics))")] int? l2Regularization = null
            )
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();

            // Turn the data into the ML.NET data view.
            var trainData = mlContext.Data.LoadFromEnumerable(_data);
            var trainer = mlContext.BinaryClassification.Trainers.SdcaLogisticRegression(labelColumnName:"Label", featureColumnName: "Features", l1Regularization: l1Regularization, l2Regularization: l2Regularization);
            var pipeline = mlContext.Transforms.Text.FeaturizeText( "Features", "Text").Append(trainer);
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
        
        public CalibratedBinaryClassificationMetrics SentimentEvaluateResult(byte[] model)
        {
            // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
            // as a catalog of available operations and as the source of randomness.
            var mlContext = new MLContext();

            // load the sentiment model
            var stream = new MemoryStream( model );
            var trainedModel = mlContext.Model.Load(stream, out var inputSchema);

            // Turn the data into the ML.NET data view.
            var trainData = mlContext.Data.LoadFromEnumerable(_data);

            // run the evaluation
            var predictions = trainedModel.Transform(trainData);
            var metrics = mlContext.BinaryClassification.Evaluate(predictions);

            return metrics;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Machine Learning", Name = "Sentiment Analysis - Prediction", Description = "Predicts the sentiment based on the input model produced by the \"Sentiment Analysis - Train\" aggregate function") ]
        public bool SentimentPrediction(byte[] model, string text, out double probability, out double score)
        {
            if (_sentimentModel == null || !_sentimentModel.SequenceEqual(model))
            {
                _sentimentModel = model;
                var mlContext = new MLContext();
                var stream = new MemoryStream( model );
                var trainedModel = mlContext.Model.Load(stream, out var inputSchema);
                _predictionFunction = mlContext.Model.CreatePredictionEngine<SentimentIssue, SentimentPredictionResult>(trainedModel);
            }

            var sentimentIssue = new SentimentIssue(false, text);
            var result = _predictionFunction.Predict(sentimentIssue);

            probability = result.Probability;
            score = result.Score;
            return result.Prediction;
        }
        

    }
}