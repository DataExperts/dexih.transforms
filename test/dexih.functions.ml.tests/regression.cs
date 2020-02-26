using System;
using Xunit;
using Xunit.Abstractions;

namespace dexih.functions.ml.tests
{
    public class regression
    {
        private readonly ITestOutputHelper _output;

        public regression(ITestOutputHelper output)
        {
            _output = output;
        }
        
        [Fact]
        public void RegressionLbfgsPoisson()
        {
            var regression = new RegressionAnalysis();

            // create data that easily clusters into two areas.
            var labels = new[] {"x", "y"};
            var hotEncode = new[] {EEncoding.None, EEncoding.None};

            // predict a simple linear line
            for (var i = 1; i < 10; i++)
            {
                regression.RegressionLbfgsPoissonTrain(i, labels, new object[] {i, 2 * i}, hotEncode);
            }

            var model = regression.RegressionLbfgsPoissonTrainResult();
            
            Assert.NotNull(model);
            
            // check the clusters
            var prediction1 = regression.RegressionPredict(model, labels, new object[] {5, 10});
            _output.WriteLine($"Prediction: {prediction1}");
            Assert.True(prediction1 > 4 && prediction1 < 6);

        }
        
        [Fact]
        public void RegressionGam()
        {
            var regression = new RegressionAnalysis();

            // create data that easily clusters into two areas.
            var labels = new[] {"x", "y"};
            var hotEncode = new[] {EEncoding.None, EEncoding.None};

            // predict a simple linear line
            for (var i = 1; i < 10; i++)
            {
                regression.RegressionGamTrain(i, labels, new object[] {i, 2 * i}, hotEncode);
            }

            var model = regression.RegressionGamTrainResult();
            
            Assert.NotNull(model);
            
            // check the clusters
            var prediction1 = regression.RegressionPredict(model, labels, new object[] {5, 10});

            _output.WriteLine($"Prediction: {prediction1}");
            // Assert.True(prediction1 > 4 && prediction1 < 6);

        }
        
        //TODO Missing Library Issue.
//        [Fact]
//        public void RegressionOls()
//        {
//            var regression = new RegressionAnalysis();
//
//            // create data that easily clusters into two areas.
//            var labels = new[] {"x", "y"};
//            var hotEncode = new[] {EEncoding.None, EEncoding.None};
//
//            // predict a simple linear line
//            for (var i = 1; i < 10; i++)
//            {
//                regression.RegressionOlsTrain(i, labels, new object[] {i, 2 * i}, hotEncode);
//            }
//
//            var model = regression.RegressionOlsTrainResult();
//            
//            Assert.NotNull(model);
//            
//            // check the clusters
//            var prediction1 = regression.RegressionPredict(model, labels, new object[] {5, 10});
//
//            _output.WriteLine($"Prediction: {prediction1}");
//            Assert.True(prediction1 > 4 && prediction1 < 6);
//
//        }
        
        [Fact]
        public void RegressionFastForest()
        {
            var regression = new RegressionAnalysis();

            // create data that easily clusters into two areas.
            var labels = new[] {"x", "y"};
            var hotEncode = new[] {EEncoding.None, EEncoding.None};

            // predict a simple linear line
            for (var i = 1; i < 10; i++)
            {
                regression.RegressionFastForestTrain(i, labels, new object[] {i, 2 * i}, hotEncode);
            }

            var model = regression.RegressionFastForestTrainResult();
            
            Assert.NotNull(model);
            
            // check the clusters
            var prediction1 = regression.RegressionPredict(model, labels, new object[] {5, 10});

            _output.WriteLine($"Prediction: {prediction1}");
            // Assert.True(prediction1 > 4 && prediction1 < 6);

        }
        
        [Fact]
        public void RegressionSdca()
        {
            var regression = new RegressionAnalysis();

            // create data that easily clusters into two areas.
            var labels = new[] {"x", "y"};
            var hotEncode = new[] {EEncoding.None, EEncoding.None};

            // predict a simple linear line
            for (var i = 1; i < 10; i++)
            {
                regression.RegressionSdcaTrain(i, labels, new object[] {i, 2 * i}, hotEncode);
            }

            var model = regression.RegressionSdcaTrainResult(maximumNumberOfIterations:5);
            
            Assert.NotNull(model);
            
            // check the clusters
            var prediction1 = regression.RegressionPredict(model, labels, new object[] {5, 10});

            _output.WriteLine($"Prediction: {prediction1}");
            Assert.True(prediction1 > 0);

        }
        
        [Fact]
        public void RegressionFastTree()
        {
            var regression = new RegressionAnalysis();

            // create data that easily clusters into two areas.
            var labels = new[] {"x", "y"};
            var hotEncode = new[] {EEncoding.None, EEncoding.None};

            // predict a simple linear line
            for (var i = 1; i < 10; i++)
            {
                regression.RegressionFastTreeTrain(i, labels, new object[] {i, 2 * i}, hotEncode);
            }

            var model = regression.RegressionFastTreeTrainResult();
            
            Assert.NotNull(model);
            
            // check the clusters
            var prediction1 = regression.RegressionPredict(model, labels, new object[] {5, 10});

            _output.WriteLine($"Prediction: {prediction1}");
            // Assert.True(prediction1 > 4 && prediction1 < 6);

        }
        
        [Fact]
        public void RegressionOnlineGradientDescent()
        {
            var regression = new RegressionAnalysis();

            // create data that easily clusters into two areas.
            var labels = new[] {"x", "y"};
            var hotEncode = new[] {EEncoding.None, EEncoding.None};

            // predict a simple linear line
            for (var i = 1; i < 10; i++)
            {
                regression.RegressionOnlineGradientDescentTrain(i, labels, new object[] {i, 2 * i}, hotEncode);
            }

            var model = regression.RegressionOnlineGradientDescentTrainResult();
            
            Assert.NotNull(model);
            
            // check the clusters
            var prediction1 = regression.RegressionPredict(model, labels, new object[] {5, 10});

            _output.WriteLine($"Prediction: {prediction1}");
            // Assert.True(prediction1 > 4 && prediction1 < 6);

        }
        
        [Fact]
        public void RegressionFastTreeTweedie()
        {
            var regression = new RegressionAnalysis();

            // create data that easily clusters into two areas.
            var labels = new[] {"x", "y"};
            var hotEncode = new[] {EEncoding.None, EEncoding.None};

            // predict a simple linear line
            for (var i = 1; i < 10; i++)
            {
                regression.RegressionFastTreeTweedieTrain(i, labels, new object[] {i, 2 * i}, hotEncode);
            }

            var model = regression.RegressionFastTreeTweedieTrainResult();
            
            Assert.NotNull(model);
            
            // check the clusters
            var prediction1 = regression.RegressionPredict(model, labels, new object[] {5, 10});

            _output.WriteLine($"Prediction: {prediction1}");
            // Assert.True(prediction1 > 4 && prediction1 < 6);

        }
        
        [Fact]
        public void RegressionBest()
        {
            var regression = new RegressionAnalysis();

            // create data that easily clusters into two areas.
            var labels = new[] {"x", "y"};
            var hotEncode = new[] {EEncoding.None, EEncoding.None};

            // predict a simple linear line
            for (var i = 1; i < 50; i++)
            {
                regression.RegressionExperimentBest(i, labels, new object[] {i, 2 * i}, hotEncode);
            }

            var result = regression.RegressionExperimentBestResult(60);
            
            Assert.NotNull(result.Model);
            
            // check the clusters
            var prediction1 = regression.RegressionPredict(result.Model, labels, new object[] {5, 10});

            _output.WriteLine($"Prediction: {prediction1}");
            // Assert.True(prediction1 > 4 && prediction1 < 6);
        }
    }
}