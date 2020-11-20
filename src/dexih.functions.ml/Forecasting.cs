using System.Collections.Generic;
using Microsoft.ML;
using Microsoft.ML.Transforms.TimeSeries;

namespace dexih.functions.ml
{
    // for some operating systems extra components required.
    // https://docs.microsoft.com/en-us/dotnet/machine-learning/how-to-guides/install-extra-dependencies
    // 
    public class Forecasting
    {
        private List<TimeSeriesData> _data;
        private ForecastResults _predictions;
        private int _horizon;
        private bool _firstResult = true;

        public class ForecastResults
        {
            public float[] Forecast { get; set; }
            
            public float[] ConfidenceLowerBound { get; set; }

            public float[] ConfidenceUpperBound { get; set; }
        }
        
        public class ForecastResult
        {
            [TransformFunctionParameter(Description = "Forecast Value.")]
            public float Forecast { get; set; }
            
            [TransformFunctionParameter(Description = "Confidence Lower Bound.")]
            public float ConfidenceLowerBound { get; set; }
            
            [TransformFunctionParameter(Description = "Confidence Upper Bound")]
            public float ConfidenceUpperBound { get; set; }
        }
        
        public class TimeSeriesData
        {
            public float Value;

            public TimeSeriesData(float value)
            {
                Value = value;
            }
        }
        
        public void Reset()
        {
            _data?.Clear();
            _horizon = 0;
            _firstResult = true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Machine Learning", 
            Name = "Forecast Time Series", 
            Description = "Execute a series forecasting using Singular Spectrum Analysis (SSA) model for univariate time-series forecasting.", 
            ResultMethod = nameof(ForecastTimeSeriesResult), ResetMethod = nameof(Reset))]
        public void ForecastTimeSeries(
            [TransformFunctionVariable(EFunctionVariable.Forecast)] bool isForecast,
            float value
            )
        {
            _data ??= new List<TimeSeriesData>();

            if (isForecast)
            {
                _horizon++;
            } else 
            {
                _data.Add(new TimeSeriesData(value));
            }
        }
        
        public ForecastResult ForecastTimeSeriesResult(
            [TransformFunctionVariable(EFunctionVariable.Index)]int index,
            [TransformFunctionParameter(Name = "Window Size", Description = "The length of the window on the series for building the trajectory matrix (parameter L)."), ParameterDefault("7")]int windowSize,
            [TransformFunctionParameter(Name = "Series Length", Description = "The length of series that is kept in buffer for modeling (parameter N)."), ParameterDefault("30")]int seriesLength,
            [TransformFunctionParameter(Name = "Confidence Level", Description = "The confidence level (between 0-1)."), ParameterDefault("0.95")]float confidenceLevel
            )
        {

            if (_firstResult)
            {
                if (_data == null)
                {
                    return null;
                }
                
                _firstResult = false;
                
                // Create a new context for ML.NET operations. It can be used for exception tracking and logging,
                // as a catalog of available operations and as the source of randomness.
                var mlContext = new MLContext();

                var trainData = mlContext.Data.LoadFromEnumerable(_data);
                
                var model = mlContext.Forecasting.ForecastBySsa(
                    inputColumnName: nameof(TimeSeriesData.Value),
                    outputColumnName: nameof(ForecastResults.Forecast),
                    windowSize: windowSize,
                    seriesLength: seriesLength,
                    trainSize: _data.Count,
                    horizon: _horizon,
                    confidenceLevel: confidenceLevel,
                    confidenceLowerBoundColumn: nameof(ForecastResults.ConfidenceLowerBound),
                    confidenceUpperBoundColumn: nameof(ForecastResults.ConfidenceUpperBound)
                    );

                var trainedModel = model.Fit(trainData);
                
                // Forecast next five values.
                var forecastEngine = trainedModel.CreateTimeSeriesEngine<TimeSeriesData, ForecastResults>(mlContext);
                _predictions = forecastEngine.Predict();
            }

            if (index < _data.Count)
            {
                return null;
            }

            return new ForecastResult()
            {
                Forecast = _predictions.Forecast[index - _data.Count],
                ConfidenceUpperBound = _predictions.ConfidenceUpperBound[index - _data.Count],
                ConfidenceLowerBound = _predictions.ConfidenceLowerBound[index - _data.Count]
            };
        }
    }
}