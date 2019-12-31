using dexih.functions.Query;
using Xunit;

namespace dexih.functions.ml.tests
{
    public class anomalyprediction
    {
        [Fact]
        public void SpikeDetection()
        {
            var anomaly = new AnomalyDetection();


            // create dataset with two spikes
            for (var i = 0; i < 100; i++)
            {
                switch (i)
                {
                    case 20:
                        anomaly.SpikeDetection(i, 200, EAggregate.Average);
                        break;
                    case 60:
                        anomaly.SpikeDetection(i, 0, EAggregate.Average);
                        break;
                    default:
                        anomaly.SpikeDetection(i, i, EAggregate.Average);
                        break;
                }
            }

            // create dataset with two spikes
            for (var i = 0; i < 100; i++)
            {
                var prediction = anomaly.SpikeDetectionResult(i, pvalueHistoryLength: 10);

                switch (i)
                {
                    case 20:
                        Assert.Equal(1, prediction.Alert);
                        break;
                    case 60:
                        Assert.Equal(1, prediction.Alert);
                        break;
                    default:
                        Assert.Equal(0, prediction.Alert);
                        break;
                }
            }

        }
        
//        [Fact]
//        public void SpikeDetectionModel()
//        {
//            var anomaly = new AnomalyDetection();
//
//
//            // create dataset with two spikes
//            for (var i = 0; i < 100; i++)
//            {
//                switch (i)
//                {
//                    case 20:
//                        anomaly.SpikeDetectionModel(i, 200, EAggregate.Average);
//                        break;
//                    case 60:
//                        anomaly.SpikeDetectionModel(i, 0, EAggregate.Average);
//                        break;
//                    default:
//                        anomaly.SpikeDetectionModel(i, i, EAggregate.Average);
//                        break;
//                }
//            }
//
//            var model = anomaly.SpikeDetectionModelResult(pvalueHistoryLength: 10);
//
//            // create dataset with two spikes
//            for (var i = 0; i < 100; i++)
//            {
//                var prediction = anomaly.SpikePredict(model, i);
//
//                switch (i)
//                {
//                    case 20:
//                        Assert.Equal(1, prediction.Alert);
//                        break;
//                    case 60:
//                        Assert.Equal(1, prediction.Alert);
//                        break;
//                    default:
//                        Assert.Equal(0, prediction.Alert);
//                        break;
//                }
//            }
//
//        }
    }
}