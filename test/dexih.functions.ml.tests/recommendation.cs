using System;
using Xunit;

namespace dexih.functions.ml.tests
{
    public class recommendation
    {
        /// <summary>
        /// Unit test of the cluster k-means functions
        /// </summary>
        [Fact]
        public void ClusterkMeans()
        {
            var recommendation = new RecommendationAnalysis();

            // create data that easily clusters into two areas.
            var values = new[]
            {
                new [] {"1", "2"},
                new [] {"2", "3"},
                new [] {"3", "4"},
                new [] {"5", "6"}
            };
            
            var hotEncode = new[] {EEncoding.None, EEncoding.None};
            
            foreach(var value in values)
            {
                recommendation.RecommendationTrain(value[0], value[1]);
            }

            var model = recommendation.RecommendationTrainResult();
            
            Assert.NotNull(model);
            
            // check the recommendation
            var result = recommendation.RecommendationPredict(model, "1", "3");
            Assert.True(result > 0 && result < 0.2);

            result = recommendation.RecommendationPredict(model, "1", "5");
            Assert.True(Single.IsNaN(result));

        }
    }
}