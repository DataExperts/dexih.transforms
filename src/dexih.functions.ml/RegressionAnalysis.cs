using System.Collections.Generic;
using System.Linq;
using System.Threading;
using dexih.functions.Exceptions;
using Dexih.Utils.CopyProperties;
using Microsoft.ML;
using Microsoft.ML.AutoML;
using Microsoft.ML.Data;
using Microsoft.ML.Trainers;
using Microsoft.ML.Trainers.FastTree;

namespace dexih.functions.ml
{
   
    public class RegressionAnalysis
    {
        private DynamicList _dynamicList;
        private RegressionPredictor<RegressionPrediction> _predictor;

        private ITransformer _model;


        public class RegressionPrediction
        {
            [ColumnName("Score")]
            public float Score { get; set; }
            
            [ColumnName("PredictedLabel")]
            
            public float Label { get; set; }
        }

        public class RegressionExperiment : RegressionMetrics
        {
            public byte[] Model { get; set; }
            public string TrainerName { get; set; }
            public double RunTimeInSeconds { get; set; }
            public int NumberTested { get; set; }
        }
        
        public class RegressionExperiment2 : RegressionExperiment
        {
            public bool IsBest { get; set; }
            
        }
        
        public void Reset()
        {
            _dynamicList = null;
            _predictor = null;
        }

        public string[] ImportModelLabels(byte[] model) => Helpers.ImportModelLabels(model);

        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression Experiment - Best", Description = "Gets the best regression model based on a series of experiments (use a series transform for all results).", ResultMethod = nameof(RegressionExperimentBestResult), ResetMethod = nameof(Reset))]
        public void RegressionExperimentBest(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
        {
            _dynamicList = _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
        }
        
        public RegressionExperiment RegressionExperimentBestResult(
            [TransformFunctionParameter(Description = "The maximum time in seconds the experiment is allowed to run.")] uint maxExperimentTimeInSeconds = 600,
            [TransformFunctionParameter(Description = "The optimizing metric")] RegressionMetric optimizingMetric = RegressionMetric.MeanSquaredError,
            // [TransformFunctionParameter(Description = "Include Ols")] bool includeOls = true,
            [TransformFunctionParameter(Description = "Include Fast-Forest")] bool includeFastForest = true,
            [TransformFunctionParameter(Description = "Include Fast-Tree")] bool includeFastTree = true,
            [TransformFunctionParameter(Description = "Include Fast-Tree-Tweedie")] bool includeFastTreeTweedie = true,
            [TransformFunctionParameter(Description = "Include Online-Gradient-Descent")] bool includeOnlineGradientDescent = true,
            [TransformFunctionParameter(Description = "Include Lbfgs-Poisson Regression")] bool includeLbfgsPoissonRegression = true,
            [TransformFunctionParameter(Description = "Include Stochastic-Dual-Coordinate-Ascent")] bool includeSdca = true,
            [TransformFunctionParameter(Description = "Include Light Gbm")] bool includeLightGbm = true,
           
            CancellationToken cancellationToken = default
            )
        {
            var mlContext = new MLContext();

            if (_dynamicList == null)
            {
                return null;
            }

            var trainData = _dynamicList.GetDataView(mlContext);

            var featuresColumnName = "Features";
            var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);
            
            var options = new RegressionExperimentSettings()
            {
                MaxExperimentTimeInSeconds = maxExperimentTimeInSeconds,
                CancellationToken = cancellationToken,
                OptimizingMetric = optimizingMetric,
            };
            
            options.Trainers.Clear();
            // if(includeOls) { options.Trainers.Add(RegressionTrainer.Ols);}
            if(includeFastForest) { options.Trainers.Add(RegressionTrainer.FastForest);}
            if(includeFastTree) { options.Trainers.Add(RegressionTrainer.FastTree);}
            if(includeFastTreeTweedie) { options.Trainers.Add(RegressionTrainer.FastTreeTweedie);}
            if(includeOnlineGradientDescent) { options.Trainers.Add(RegressionTrainer.OnlineGradientDescent);}
            if(includeLbfgsPoissonRegression) { options.Trainers.Add(RegressionTrainer.LbfgsPoissonRegression);}
            if(includeSdca) { options.Trainers.Add(RegressionTrainer.StochasticDualCoordinateAscent);}
            if(includeLightGbm) { options.Trainers.Add(RegressionTrainer.LightGbm);}

            var experiment = mlContext.Auto().CreateRegressionExperiment(options);
            var experimentResult = experiment.Execute(trainData, labelColumnName: Helpers.PredictedLabel, preFeaturizer: pipeline);

            var trainedModel = experimentResult.BestRun.Model;
            var modelBytes = Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);

            var metrics = experimentResult.BestRun.ValidationMetrics;

            var regressionResult = new RegressionExperiment
            {
                Model = modelBytes,
                NumberTested = experimentResult.RunDetails.Count(),
                MeanAbsoluteError = metrics.MeanAbsoluteError,
                LossFunction = metrics.LossFunction,
                MeanSquaredError = metrics.MeanSquaredError,
                RootMeanSquaredError = metrics.RootMeanSquaredError,
                RSquared = metrics.RSquared,
                RunTimeInSeconds = experimentResult.BestRun.RuntimeInSeconds,
                TrainerName = experimentResult.BestRun.TrainerName
            };

            return regressionResult;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, GeneratesRows = true, Category = "Machine Learning", Name = "Regression Experiment - Get All", Description = "Gets the a series of regression models based on experiments.", ResultMethod = nameof(RegressionExperimentSeriesResult), ResetMethod = nameof(Reset))]
        public void RegressionExperimentSeries(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
        {
            _dynamicList = _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
        }

        private ExperimentResult<Microsoft.ML.Data.RegressionMetrics> _experimentResult;
        private IEnumerator<RunDetail<Microsoft.ML.Data.RegressionMetrics>> _experimentEnumerator;
        private DataViewSchema _experimentSchema;
        
        public RegressionExperiment2 RegressionExperimentSeriesResult(
            [TransformFunctionParameter(Description = "The maximum time in seconds the experiment is allowed to run.")] uint maxExperimentTimeInSeconds = 600,
            [TransformFunctionParameter(Description = "The optimizing metric")] RegressionMetric optimizingMetric = RegressionMetric.MeanSquaredError,
            // [TransformFunctionParameter(Description = "Include Ols")] bool includeOls = true,
            [TransformFunctionParameter(Description = "Include Fast-Forest")] bool includeFastForest = true,
            [TransformFunctionParameter(Description = "Include Fast-Tree")] bool includeFastTree = true,
            [TransformFunctionParameter(Description = "Include Fast-Tree-Tweedie")] bool includeFastTreeTweedie = true,
            [TransformFunctionParameter(Description = "Include Online-Gradient-Descent")] bool includeOnlineGradientDescent = true,
            [TransformFunctionParameter(Description = "Include Lbfgs-Poisson Regression")] bool includeLbfgsPoissonRegression = true,
            [TransformFunctionParameter(Description = "Include Stochastic-Dual-Coordinate-Ascent")] bool includeSdca = true,
            [TransformFunctionParameter(Description = "Include Light Gbm")] bool includeLightGbm = true,
           
            CancellationToken cancellationToken = default
            )
        {
            var mlContext = new MLContext();

            if (_experimentEnumerator == null)
            {
                if (_dynamicList == null)
                {
                    return null;
                }

                
                var trainData = _dynamicList.GetDataView(mlContext);
                _experimentSchema = trainData.Schema;

                var featuresColumnName = "Features";
                var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);

                var options = new RegressionExperimentSettings()
                {
                    MaxExperimentTimeInSeconds = maxExperimentTimeInSeconds,
                    CancellationToken = cancellationToken,
                    OptimizingMetric = optimizingMetric,
                };

                options.Trainers.Clear();
//                if (includeOls)
//                {
//                    options.Trainers.Add(RegressionTrainer.Ols);
//                }

                if (includeFastForest)
                {
                    options.Trainers.Add(RegressionTrainer.FastForest);
                }

                if (includeFastTree)
                {
                    options.Trainers.Add(RegressionTrainer.FastTree);
                }

                if (includeFastTreeTweedie)
                {
                    options.Trainers.Add(RegressionTrainer.FastTreeTweedie);
                }

                if (includeOnlineGradientDescent)
                {
                    options.Trainers.Add(RegressionTrainer.OnlineGradientDescent);
                }

                if (includeLbfgsPoissonRegression)
                {
                    options.Trainers.Add(RegressionTrainer.LbfgsPoissonRegression);
                }

                if (includeSdca)
                {
                    options.Trainers.Add(RegressionTrainer.StochasticDualCoordinateAscent);
                }

                if (includeLightGbm)
                {
                    options.Trainers.Add(RegressionTrainer.LightGbm);
                }

                var experiment = mlContext.Auto().CreateRegressionExperiment(options);
                _experimentResult = experiment.Execute(trainData, labelColumnName:Helpers.PredictedLabel, preFeaturizer: pipeline);
                _experimentEnumerator = _experimentResult.RunDetails.GetEnumerator();
            }

            if (_experimentEnumerator.MoveNext())
            {
                var current = _experimentEnumerator.Current;
                
                if (current == null)
                {
                    throw new FunctionException($"The regression trainer failed to produce any result.");
                }
                
                if (current.Exception != null)
                {
                    throw new FunctionException($"The regression trainer {current.TrainerName} failed due to {current.Exception.Message}.", current.Exception);
                }
                
                var trainedModel = current.Model;
                var modelBytes = Helpers.SaveModel(mlContext, _experimentSchema, trainedModel);

                var metrics = current.ValidationMetrics;

                var regressionResult = new RegressionExperiment2
                {
                    Model = modelBytes,
                    MeanAbsoluteError = metrics.MeanAbsoluteError,
                    LossFunction = metrics.LossFunction,
                    MeanSquaredError = metrics.MeanSquaredError,
                    RootMeanSquaredError = metrics.RootMeanSquaredError,
                    RSquared = metrics.RSquared,
                    RunTimeInSeconds = current.RuntimeInSeconds,
                    TrainerName = current.TrainerName,
                    NumberTested = _experimentResult.RunDetails.Count(),
                    IsBest = current == _experimentResult.BestRun,
                };

                return regressionResult;
            }

            return null;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression Fast-Tree Analysis - Train", Description = "Builds a model using fast-tree regression trainer.", ResultMethod = nameof(RegressionFastTreeTrainResult), ResetMethod = nameof(Reset))]
        public void RegressionFastTreeTrain(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
        {
            _dynamicList = _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
        }
        
        public byte[] RegressionFastTreeTrainResult(
            [TransformFunctionParameter(Description = "The maximum number of leaves per tree.")] int numberOfLeaves = 20,
            [TransformFunctionParameter(Description = "The minimal number of examples allowed in a leaf of a regression tree, out of the subsampled data.", ListOfValues = new [] {"1", "10", "50"})] int minimumExampleCountPerLeaf = 10,
            [TransformFunctionParameter(Description = "The learning rate")] double learningRate = 0.2
            )
        {
            var mlContext = new MLContext();
            
            if (_dynamicList == null)
            {
                return null;
            }
            var trainData = _dynamicList.GetDataView(mlContext);

            var featuresColumnName = "Features";
            var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);
            
            var options = new FastTreeRegressionTrainer.Options()
            {
                FeatureColumnName = featuresColumnName,
                NumberOfLeaves = numberOfLeaves,
                MinimumExampleCountPerLeaf = minimumExampleCountPerLeaf,
                LearningRate = learningRate
            };

            var pipeline2 = pipeline.Append(mlContext.Regression.Trainers.FastTree(options));
            var trainedModel = pipeline2.Fit(trainData);
            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression Gam Analysis - Train", Description = "Builds a model using the Gam regression trainer.", ResultMethod = nameof(RegressionGamTrainResult), ResetMethod = nameof(Reset))]
        public void RegressionGamTrain(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
        {
            _dynamicList = _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
        }
        
        public byte[] RegressionGamTrainResult(
            [TransformFunctionParameter(Description = "The number of iterations to use in learning the features.")] int numberOfIterations = 9500,
            [TransformFunctionParameter(Description = "The maximum number of bins to use to approximate features.")] int maximumBinCountPerFeature = 255,
            [TransformFunctionParameter(Description = "The learning rate")] double learningRate = 0.002
        )
        {
            var mlContext = new MLContext();
            if (_dynamicList == null)
            {
                return null;
            }
            var trainData = _dynamicList.GetDataView(mlContext);

            var featuresColumnName = "Features";
            var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);
            
            var options = new  GamRegressionTrainer.Options()
            {
                FeatureColumnName = featuresColumnName,
                NumberOfIterations = numberOfIterations,
                MaximumBinCountPerFeature = maximumBinCountPerFeature,
                LearningRate = learningRate
            };

            var pipeline2 = pipeline.Append(mlContext.Regression.Trainers.Gam(options));
            var trainedModel = pipeline2.Fit(trainData);
            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression Stochastic-Dual-Coordinate-Ascent Analysis - Train", Description = "Builds a model using the Stochastic-Dual-Coordinate-Ascent regression trainer.", ResultMethod = nameof(RegressionSdcaTrainResult), ResetMethod = nameof(Reset))]
        public void RegressionSdcaTrain(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
        {
            _dynamicList = _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
        }
        
        public byte[] RegressionSdcaTrainResult(
            [TransformFunctionParameter(Description = "The L2 weight for [regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics))")] int? l1Regularization = null,
            [TransformFunctionParameter(Description = "The L1 weight for [regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics))")] int? l2Regularization = null,
            [TransformFunctionParameter(Description = "The maximum number of passes to perform over the data.")] int? maximumNumberOfIterations = null
        )
        {
            var mlContext = new MLContext();
            if (_dynamicList == null)
            {
                return null;
            }
            var trainData = _dynamicList.GetDataView(mlContext);

            var featuresColumnName = "Features";
            var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);
            
            var options = new  SdcaRegressionTrainer.Options()
            {
                FeatureColumnName = featuresColumnName,
                L1Regularization = l1Regularization,
                L2Regularization = l2Regularization,
                MaximumNumberOfIterations = maximumNumberOfIterations
            };

            var pipeline2 = pipeline.Append(mlContext.Regression.Trainers.Sdca(options));
            var trainedModel = pipeline2.Fit(trainData);
            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression Fast-Forest Analysis - Train", Description = "Builds a model using the Fast-Forest regression trainer.", ResultMethod = nameof(RegressionFastForestTrainResult), ResetMethod = nameof(Reset))]
        public void RegressionFastForestTrain(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
        {
            _dynamicList =  _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
        }
        
        public byte[] RegressionFastForestTrainResult(
            [TransformFunctionParameter(Description = "The maximum number of leaves per decision tree.")] int numberOfLeaves = 20,
            [TransformFunctionParameter(Description = "Total number of decision trees to create in the ensemble.")] int numberOfTrees = 100,
            [TransformFunctionParameter(Description = "The minimal number of data points required to form a new tree leaf.")] int minimumExampleCountPerLeaf = 10
        )
        {
            var mlContext = new MLContext();
            if (_dynamicList == null)
            {
                return null;
            }
            var trainData = _dynamicList.GetDataView(mlContext);

            var featuresColumnName = "Features";
            var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);
            
            var options = new  FastForestRegressionTrainer.Options()
            {
                FeatureColumnName = featuresColumnName,
                NumberOfLeaves = numberOfLeaves,
                NumberOfTrees = numberOfTrees,
                MinimumExampleCountPerLeaf = minimumExampleCountPerLeaf
            };

            var pipeline2 = pipeline.Append(mlContext.Regression.Trainers.FastForest(options));
            var trainedModel = pipeline2.Fit(trainData);
            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
        }

        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression Fast-Tree (Tweedie) Analysis - Train", Description = "Builds a model using the Fast-Tree [Tweedie](https://en.wikipedia.org/wiki/Tweedie_distribution) regression trainer.", ResultMethod = nameof(RegressionFastTreeTweedieTrainResult), ResetMethod = nameof(Reset))]
        public void RegressionFastTreeTweedieTrain(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
        {
            _dynamicList = _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
        }
        
        public byte[] RegressionFastTreeTweedieTrainResult(
            [TransformFunctionParameter(Description = "The maximum number of leaves per decision tree.")] int numberOfLeaves = 20,
            [TransformFunctionParameter(Description = "Total number of decision trees to create in the ensemble.")] int numberOfTrees = 100,
            [TransformFunctionParameter(Description = "The minimal number of data points required to form a new tree leaf.")] int minimumExampleCountPerLeaf = 10,
            [TransformFunctionParameter(Description = "The learning rate.")] double learningRate = 0.2
        )
        {
            var mlContext = new MLContext();
            if (_dynamicList == null)
            {
                return null;
            }
            var trainData = _dynamicList.GetDataView(mlContext);

            var featuresColumnName = "Features";
            var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);
            
            var options = new  FastTreeTweedieTrainer.Options()
            {
                FeatureColumnName = featuresColumnName,
                NumberOfLeaves = numberOfLeaves,
                NumberOfTrees = numberOfTrees,
                MinimumExampleCountPerLeaf = minimumExampleCountPerLeaf,
                LearningRate = learningRate
            };

            var pipeline2 = pipeline.Append(mlContext.Regression.Trainers.FastTreeTweedie(options));
            var trainedModel = pipeline2.Fit(trainData);
            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression LbfgsPoisson Analysis - Train", Description = "Builds a model using the LbfgsPoisson regression trainer.", ResultMethod = nameof(RegressionLbfgsPoissonTrainResult), ResetMethod = nameof(Reset))]
        public void RegressionLbfgsPoissonTrain(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
        {
            _dynamicList = _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
        }
        
        public byte[] RegressionLbfgsPoissonTrainResult(
            [TransformFunctionParameter(Description = "The L2 weight for [regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics))")] float l2Regularization = 1f,
            [TransformFunctionParameter(Description = "The L1 [regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics)) hyperparameter. Higher values will tend to lead to more sparse model.")] float l1Regularization = 1f,
            [TransformFunctionParameter(Description = "Number of previous iterations to remember for estimating the Hessian. Lower values mean faster but less accurate estimates.")] int historySize = 20,
            [TransformFunctionParameter(Description = "Enforce non-negative weights.")] bool enforceNonNegativity = false
        )
        {
            var mlContext = new MLContext();
            if (_dynamicList == null)
            {
                return null;
            }
            var trainData = _dynamicList.GetDataView(mlContext);

            var featuresColumnName = "Features";
            var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);
            
            var options = new  LbfgsPoissonRegressionTrainer.Options()
            {
                FeatureColumnName = featuresColumnName,
                L1Regularization = l1Regularization,
                L2Regularization = l2Regularization,
                HistorySize = historySize,
                EnforceNonNegativity = enforceNonNegativity
            };

            var pipeline2 = pipeline.Append(mlContext.Regression.Trainers.LbfgsPoissonRegression(options));
            var trainedModel = pipeline2.Fit(trainData);
            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression Online Gradient Descent Analysis - Train", Description = "Builds a model using the Online Gradient Descent regression trainer.", ResultMethod = nameof(RegressionOnlineGradientDescentTrainResult), ResetMethod = nameof(Reset))]
        public void RegressionOnlineGradientDescentTrain(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
        {
            _dynamicList = _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
        }
        
        public byte[] RegressionOnlineGradientDescentTrainResult(
            [TransformFunctionParameter(Description = "The learning rate.")] float learningRate = 0.1f,
            [TransformFunctionParameter(Description = "Decrease learning rate as iterations progress.")] bool decreaseLearningRate = true,
            [TransformFunctionParameter(Description = "The L2 weight for [regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics))")] float l2Regularization = 0f,
            [TransformFunctionParameter(Description = "TThe number of passes through the training dataset.")] int numberOfIterations = 1
        )
        {
            var mlContext = new MLContext();
            if (_dynamicList == null)
            {
                return null;
            }
            var trainData = _dynamicList.GetDataView(mlContext);

            var featuresColumnName = "Features";
            var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);
            
            var options = new  OnlineGradientDescentTrainer.Options()
            {
                FeatureColumnName = featuresColumnName,
                DecreaseLearningRate = decreaseLearningRate,
                L2Regularization = l2Regularization,
                LearningRate = learningRate,
                NumberOfIterations = numberOfIterations
            };

            var pipeline2 = pipeline.Append(mlContext.Regression.Trainers.OnlineGradientDescent(options));
            var trainedModel = pipeline2.Fit(trainData);
            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
        }
        
        //TODO Issue with Ols trainer, library does not exist.
//        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression Ols Analysis - Train", Description = "Builds a model using the Ols regression trainer.", ResultMethod = nameof(RegressionOlsTrainResult), ResetMethod = nameof(Reset))]
//        public void RegressionOlsTrain(
//            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
//            [TransformFunctionLinkedParameter("Data Fields"), ParameterLabel] string[] label,
//            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
//            [TransformFunctionLinkedParameter("Data Fields"), ParameterDefault("None")] EEncoding[] encoding)
//        {
//            _dynamicList = _dynamicList.AddData(label, value, encoding, predictor, EEncoding.None);
//        }
//        
//        public byte[] RegressionOlsTrainResult(
//            [TransformFunctionParameter(Description = "The L2 weight for [regularization](https://en.wikipedia.org/wiki/Regularization_(mathematics))")] float l2Regularization = 0f
//        )
//        {
//            var mlContext = new MLContext();
//            if (_dynamicList == null)
//            {
//                return null;
//            }
//            var trainData = _dynamicList.GetDataView(mlContext);
//
//            var featuresColumnName = "Features";
//            var pipeline = Helpers.CreatePipeline(mlContext, _dynamicList.Fields, featuresColumnName);
//            
//            var options = new  OlsTrainer.Options()
//            {
//                FeatureColumnName = featuresColumnName,
//                L2Regularization = l2Regularization,
//            };
//
//            var pipeline2 = pipeline.Append(mlContext.Regression.Trainers.Ols(options));
//            var trainedModel = pipeline2.Fit(trainData);
//            return Helpers.SaveModel(mlContext, trainData.Schema, trainedModel);
//        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Machine Learning", Name = "Regression Analysis - Evaluate", Description = "Evaluates the accuracy of a model using on of the regression trainers.", ResultMethod = nameof(RegressionEvaluateResult), ResetMethod = nameof(Reset), ImportMethod = nameof(ImportModelLabels))]
        public void RegressionEvaluate(
            [TransformFunctionParameter(Description = "The predictor column.")] float predictor,
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label,
            [TransformFunctionLinkedParameter("Data Fields")] object[] value,
            byte[] model)
        {
            if (_model == null)
            {
                var mlContext = new MLContext();
                _model = Helpers.LoadModel(mlContext, model, out var schema);
                
                var fields = label.Select(l =>
                {
                    var column = schema.GetColumnOrNull(l);

                    if (column is null)
                    {
                        throw new FunctionException(
                            $"The input label {label} does not have a matching field in the model.  Ensure the field names are one of the following: {string.Join(", ", schema.Select(c => c.Name))}");
                    }
                
                    return new DynamicTypeProperty(column.Value.Name, column.Value.Type.RawType);
                }).Append(new DynamicTypeProperty(Helpers.PredictedLabel, typeof(float))).Append(new DynamicTypeProperty("Score", typeof(float))).ToArray();
            
                _dynamicList = new DynamicList(fields);
                
            }
            _dynamicList.Add(value.Select(c=> c).Append(predictor).ToArray());
        }
        
        public RegressionMetrics RegressionEvaluateResult()
        {
            var mlContext = new MLContext();
            if (_dynamicList == null)
            {
                return null;
            }

            // run the evaluation
            var predictions = _model.Transform(_dynamicList.GetDataView(mlContext));
            var metrics = mlContext.Regression.Evaluate(predictions, Helpers.PredictedLabel);

            var cloned = metrics.CloneProperties<RegressionMetrics>();
            return cloned;
        }

        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Machine Learning", Name = "Regression Analysis - Predict", Description = "Predicts a value using regression model.", ResetMethod = nameof(Reset), ImportMethod = nameof(ImportModelLabels))]
        public float RegressionPredict(
            byte[] model, 
            [TransformFunctionLinkedParameter("Data Fields"), TransformParameterLabel] string[] label, 
            [TransformFunctionLinkedParameter("Data Fields")] object[] value)
        {
            if (_predictor == null)
            {
                _predictor = new RegressionPredictor<RegressionPrediction>(model, label);
            }

            var result = _predictor.Run(value);

            return result?.Score ?? float.NaN;
        }
        
     
    }
}