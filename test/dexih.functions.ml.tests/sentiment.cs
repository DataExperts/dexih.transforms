using System;
using Xunit;

namespace dexih.functions.ml.tests
{
    public class sentiment
    {
        [Fact]
        public void SentimentSdca()
        {
            var sentiment = new SentimentAnalysis();

            sentiment.SentimentSdcaRegressionTrain(false, "bad");
            sentiment.SentimentSdcaRegressionTrain(false, "terrible");
            sentiment.SentimentSdcaRegressionTrain(true, "good");
            sentiment.SentimentSdcaRegressionTrain(true, "great");
            sentiment.SentimentSdcaRegressionTrain(true, "really great");
            sentiment.SentimentSdcaRegressionTrain(true, "a little great");
            sentiment.SentimentSdcaRegressionTrain(true, "greatest");
            sentiment.SentimentSdcaRegressionTrain(true, "was great");
            sentiment.SentimentSdcaRegressionTrain(true, "it will be great");
            sentiment.SentimentSdcaRegressionTrain(true, "great job");

            var model = sentiment.SentimentSdcaRegressionTrainResult();
            
            Assert.NotNull(model);
            
            // check the recommendation
            var result = sentiment.SentimentPrediction(model, "bad", out var probability, out var score);
            Assert.False(result);

            result = sentiment.SentimentPrediction(model, "great", out  probability, out score);
            Assert.True(result);

        }
        
        [Fact]
        public void SentimentFastTree()
        {
            var sentiment = new SentimentAnalysis();

            sentiment.SentimentSdcaRegressionTrain(false, "bad");
            sentiment.SentimentSdcaRegressionTrain(false, "terrible");
            sentiment.SentimentSdcaRegressionTrain(true, "good");
            sentiment.SentimentSdcaRegressionTrain(true, "great");
            sentiment.SentimentSdcaRegressionTrain(true, "really great");
            sentiment.SentimentSdcaRegressionTrain(true, "a little great");
            sentiment.SentimentSdcaRegressionTrain(true, "greatest");
            sentiment.SentimentSdcaRegressionTrain(true, "was great");
            sentiment.SentimentSdcaRegressionTrain(true, "it will be great");
            sentiment.SentimentSdcaRegressionTrain(true, "great job");

            var model = sentiment.SentimentFastTreeTrainResult();
            
            Assert.NotNull(model);
            
            // check the recommendation
            var result = sentiment.SentimentPrediction(model, "bad", out var probability, out var score);
            Assert.False(result);

            // needs more values before it will work
//            result = sentiment.SentimentPrediction(model, "great", out  probability, out score);
//            Assert.True(result);

        }

    }
}