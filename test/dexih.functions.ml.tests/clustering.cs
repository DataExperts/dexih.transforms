using System;
using System.Threading;
using Xunit;

namespace dexih.functions.ml.tests
{
    public class clustering
    {
        /// <summary>
        /// Unit test of the cluster k-means functions
        /// </summary>
        [Fact]
        public void ClusterkMeans()
        {
            var cluster = new Clustering();

            // create data that easily clusters into two areas.
            var labels = new[] {"x", "y"};
            var values = new[]
            {
                new object[] {1, 1}, 
                new object[] {2, 2}, 
                new Object[] {100, 100},
                new Object[] {101, 101}
            };
            var hotEncode = new[] {EEncoding.None, EEncoding.None};
            
            foreach(var value in values)
            {
                cluster.ClusteringKMeansTrain(labels, value, hotEncode);    
            }

            var model = cluster.ClusteringKMeansTrainResult(2);
            
            Assert.NotNull(model);
            
            // check the clusters
            var prediction1 = cluster.ClusteringKMeansPredict(model, labels, values[0], hotEncode);
            var prediction2 = cluster.ClusteringKMeansPredict(model, labels, values[1], hotEncode);
            Assert.Equal(prediction1.PredictedLabel, prediction2.PredictedLabel);

            var prediction3 = cluster.ClusteringKMeansPredict(model, labels, values[0], hotEncode);
            var prediction4 = cluster.ClusteringKMeansPredict(model, labels, values[1], hotEncode);
            Assert.Equal(prediction3.PredictedLabel, prediction4.PredictedLabel);

            cluster.Reset();

            foreach (var value in values)
            {
                cluster.ClusteringKMeansEvaluate(labels, value, hotEncode);
            }

            var evaluateResult = cluster.ClusteringKMeansEvaluateResult(model);

            Assert.Equal(0.5, evaluateResult.AverageDistance);
            Assert.Equal(0, evaluateResult.DaviesBouldinIndex);

        }
        
        /// <summary>
        /// Unit test of the cluster sdca functions
        /// </summary>
        [Fact]
        public void ClusterSdca()
        {
            var cluster = new Clustering();

            // create two obvious clusters
            var labels = new[] {"x", "y", "z"};
            var values = new[]
            {
                ("a", new Object[] {1, 1, 1000}), 
                ("a", new Object[] {1.1, 1, 1000}), 
                ("a", new Object[] {1.2, 1, 1000}), 
                ("a", new Object[] {0.9, 1, 1000}), 
                ("a", new Object[] {1.1, 1.1, 1000}), 
                ("b", new Object[] {100000, 2001, 1}),
                ("b", new Object[] {100001, 2001, 1}),
                ("b", new Object[] {100002, 2000, 1}),
                ("b", new Object[] {100001.5, 2000, 1}),
                ("b", new Object[] {100001.1, 2000, 1}),
                ("b", new Object[] {100001.1, 2000, 1})
            };
            var hotEncode = new[] {EEncoding.None, EEncoding.None, EEncoding.None};
            
            foreach(var value in values)
            {
                cluster.ClusteringSdcaTrain(value.Item1, labels, value.Item2, hotEncode);    
            }

            var model = cluster.ClusteringSdcaTrainResult();
            
            Assert.NotNull(model);

            foreach (var value in values)
            {
                var prediction = cluster.ClusteringSdcaPredict(model, labels, value.Item2, hotEncode);
                Assert.Equal(value.Item1, prediction.Prediction);
            }
            
            cluster.Reset();

            foreach (var value in values)
            {
                cluster.ClusteringSdcaEvaluate(labels, value.Item2, hotEncode);
            }

            var evaluateResult = cluster.ClusteringKMeansEvaluateResult(model);

            Assert.True(evaluateResult.AverageDistance > 0 && evaluateResult.AverageDistance < 0.1);
            Assert.Equal(0, evaluateResult.DaviesBouldinIndex);

        }
    }
}