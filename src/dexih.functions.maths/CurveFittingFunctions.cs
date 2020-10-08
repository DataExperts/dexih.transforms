using System;
using System.Collections.Specialized;
using System.Linq;
using MathNet.Numerics;
using MathNet.Numerics.LinearRegression;

namespace dexih.functions.maths
{
    /// <summary>
    /// Uses functions provided by https://numerics.mathdotnet.com/Regression.html
    /// </summary>
    public class CurveFittingFunctions
    {
        //The cache parameters are used by the functions to maintain a state during a transform process.
        private OrderedDictionary _cacheSeries;

        private bool _firstResult = true;
        private double _yIntercept;
        private double _slope;
        private double _exponentialA;
        private double _exponentialR;
        private double _logarithmA;
        private double _logarithmB;
        private double[] _pValues;
        
        public bool Reset()
        {
            _cacheSeries?.Clear();
            _firstResult = true;
            return true;
        }

        private void AddSeries(object series, double value, EAggregate duplicateAggregate)
        {
            if (_cacheSeries == null)
            {
                _cacheSeries = new OrderedDictionary();
            }

            if (_cacheSeries.Contains(series))
            {
                var current = (SeriesValue<double>) _cacheSeries[series];
                current.AddValue(value);
            }
            else
            {
                _cacheSeries.Add(series, new SeriesValue<double>(series, value, duplicateAggregate));
            }
        }

        private double[] XValues(int? count = -1)
        {
            if (count == null || count < 0)
            {
                return Enumerable.Range(0, _cacheSeries.Count).Select(Convert.ToDouble).ToArray();
            }
            return Enumerable.Range(_cacheSeries.Count - count.Value + 1, count.Value).Select(Convert.ToDouble).ToArray();  
        }

        private double[] YValues(int? count = -1)
        {
            if (count == null || count < 0)
            {
                return _cacheSeries.Values.OfType<SeriesValue<double>>().Select(c => c.Value).ToArray();
            }
            
            return _cacheSeries.Values.OfType<SeriesValue<double>>()
                .Where((c, i) => i >= _cacheSeries.Count - count.Value)
                .Select(c => c.Value)
                .ToArray();
        }

        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Curve Fit", Name = "Linear Regression", Description = "Least-Squares fitting the points (x,y) to a line y : x -> a+b*x", ResultMethod = nameof(LinearRegressionResult), ResetMethod = nameof(Reset))]
        public void LinearRegression([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, [TransformFunctionVariable(EFunctionVariable.Forecast)] bool isForecast, double value, EAggregate duplicateAggregate = EAggregate.Sum)
        {
            if (!isForecast)
            {
                AddSeries(series, value, duplicateAggregate);
            }
        }
        
        [TransformFunctionParameter(Name = "Fitted Value", Description = "The calculated series value based on the fitted line.")] 
        public double? LinearRegressionResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, 
            [TransformFunctionParameter(Name = "Fit Count", Description = "The maximum number of series values (from the last) to use for the fit (empty for all).")] int? count, 
            [TransformFunctionParameter(Name = "y-Intercept", Description = "The interception with the y-axis")] out double yIntercept, 
            [TransformFunctionParameter(Name = "Slope", Description = "The slope of the line")] out double slope)
        {
            if (_firstResult)
            {
                var p = Fit.Line(XValues(count), YValues(count));
                _yIntercept = p.Item1;
                _slope = p.Item2;
                _firstResult = false;
            }

            yIntercept = _yIntercept;
            slope = _slope;

            if (count != null && count > 0 && _cacheSeries.Count - count.Value > index)
            {
                return null;
            }

            return yIntercept + slope * index;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Curve Fit", Name = "Polynomial Regression", Description = "Least-Squares fitting the points (x,y) to a k-order polynomial y : x -> p0 + p1*x + p2*x^2 + ... + pk*x^k", ResultMethod = nameof(PolynomialRegressionResult), ResetMethod = nameof(Reset))]
        public void PolynomialRegression([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, [TransformFunctionVariable(EFunctionVariable.Forecast)] bool isForecast, double value, EAggregate duplicateAggregate = EAggregate.Sum)
        {
            if (!isForecast)
            {
                AddSeries(series, value, duplicateAggregate);
            }
        }

        [TransformFunctionParameter(Name = "Fitted Value", Description = "The calculated series value based on the polynomial.")]
        public double? PolynomialRegressionResult(
            [TransformFunctionVariable(EFunctionVariable.Index)]int index, 
            [TransformFunctionParameter(Name = "Fit Count", Description = "The maximum number of series values (from the last) to use for the fit (empty for all).")] int? count, 
            [TransformFunctionParameter(Name = "k-Order", Description = "The polynomial order/degree.")] int kOrder, 
            out double[] coefficients)
        {
            if (_firstResult)
            {
                _pValues = Fit.Polynomial(XValues(count), YValues(count), kOrder);
                _firstResult = false;
            }

            coefficients = _pValues;
            
            if (count != null && count > 0 && _cacheSeries.Count - count.Value > index)
            {
                return null;
            }
            
            double value = 0;

            for (var power = 0; power < _pValues.Length; power++)
            {
                value = value + _pValues[power] * Math.Pow(index, power);
            }

            
            return value;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Curve Fit", Name = "Exponential Regression", 
            Description = "Least-Squares fitting the points (x,y) to an exponential y : x -> a*exp(r*x), returning its best fitting parameters as (a, r) tuple.", ResultMethod = nameof(ExponentialRegressionResult), ResetMethod = nameof(Reset))]
        public void ExponentialRegression([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, [TransformFunctionVariable(EFunctionVariable.Forecast)] bool isForecast, double value, EAggregate duplicateAggregate = EAggregate.Sum)
        {
            if (!isForecast)
            {
                AddSeries(series, value, duplicateAggregate);
            }
        }

        [TransformFunctionParameter(Name = "Fitted Value", Description = "The calculated series value based on the exponential regression.")]
        public double? ExponentialRegressionResult(
            [TransformFunctionVariable(EFunctionVariable.Index)]int index, 
            [TransformFunctionParameter(Name = "Regression Method"), ParameterDefault("QR")] DirectRegressionMethod regressionMethod, 
            [TransformFunctionParameter(Name = "Fit Count", Description = "The maximum number of series values (from the last) to use for the fit (empty for all).")] int? count,
            out double a, 
            out double r)
        {
            if (_firstResult)
            {
                var exponential = Fit.Exponential(XValues(count), YValues(count), regressionMethod);
                _exponentialA = exponential.Item1;
                _exponentialR = exponential.Item2;
            }

            a = _exponentialA;
            r = _exponentialR;

            if (count != null && count > 0 && _cacheSeries.Count - count.Value > index)
            {
                return null;
            }

            var value = _exponentialA * Math.Exp(_exponentialR * index);
            
            return value;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Curve Fit", Name = "Logarithm Regression", 
            Description = "Least-Squares fitting the points (x,y) to a logarithm y : x -> a + b*ln(x),", ResultMethod = nameof(LogarithmRegressionResult), ResetMethod = nameof(Reset))]
        public void LogarithmRegression(
            [TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, 
            [TransformFunctionVariable(EFunctionVariable.Forecast)] bool isForecast, 
            double value, 
            EAggregate duplicateAggregate = EAggregate.Sum)
        {
            if (!isForecast)
            {
                AddSeries(series, value, duplicateAggregate);
            }
        }

        [TransformFunctionParameter(Name = "Fitted Value", Description = "The calculated series value based on the logarithmic regression.")]
        public double? LogarithmRegressionResult(
            [TransformFunctionVariable(EFunctionVariable.Index)]int index, 
            [TransformFunctionParameter(Name = "Regression Method"), ParameterDefault("QR")] DirectRegressionMethod regressionMethod, 
            [TransformFunctionParameter(Name = "Fit Count", Description = "The maximum number of series values (from the last) to use for the fit (empty for all).")] int? count,
            out double a, 
            out double b)
        {
            if (_firstResult)
            {
                var (item1, item2) = Fit.Logarithm(XValues(count), YValues(count), regressionMethod);
                _logarithmA = item1;
                _logarithmB = item2;
            }

            a = _logarithmA;
            b = _logarithmB;

            if (count != null && count > 0 && _cacheSeries.Count - count.Value > index)
            {
                return null;
            }
            
            var value = _logarithmA + _logarithmB * Math.Log(index);

            return value;
        }
    }
}