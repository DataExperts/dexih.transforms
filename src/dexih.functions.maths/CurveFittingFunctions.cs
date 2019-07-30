using System;
using System.Collections.Specialized;
using System.Linq;
using dexih.functions.Query;
using MathNet.Numerics;

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
        private double[] _pValues;
        
        public bool Reset()
        {
            _cacheSeries?.Clear();
            _firstResult = true;
            return true;
        }

        private void AddSeries(object series, double value, SelectColumn.EAggregate duplicateAggregate)
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
        
        private double[] XValues() => Enumerable.Range(0, _cacheSeries.Count).Select(Convert.ToDouble).ToArray();
        private double[] YValues() => _cacheSeries.Values.OfType<SeriesValue<double>>().Select(c => c.Value).ToArray();
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Curve Fit", Name = "Linear Regression", Description = "Least-Squares fitting the points (x,y) to a line y : x -> a+b*x", ResultMethod = nameof(LinearRegressionResult), ResetMethod = nameof(Reset))]
        public void LinearRegression([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, [TransformFunctionVariable(EFunctionVariable.Forecast)] bool isForecast, double value, SelectColumn.EAggregate duplicateAggregate = SelectColumn.EAggregate.Sum)
        {
            if (!isForecast)
            {
                AddSeries(series, value, duplicateAggregate);
            }
        }
        
        public double LinearRegressionResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, out double yIntercept, out double slope)
        {
            if (_firstResult)
            {
                var p = Fit.Line(XValues(), YValues());
                _yIntercept = p.Item1;
                _slope = p.Item2;
                _firstResult = false;
            }

            yIntercept = _yIntercept;
            slope = _slope;

            return yIntercept + (slope * index);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Curve Fit", Name = "Polynomial Regression", Description = "Least-Squares fitting the points (x,y) to a k-order polynomial y : x -> p0 + p1*x + p2*x^2 + ... + pk*x^k", ResultMethod = nameof(PolynomialRegressionResult), ResetMethod = nameof(Reset))]
        public void PolynomialRegression([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, double value, SelectColumn.EAggregate duplicateAggregate = SelectColumn.EAggregate.Sum)
        {
            AddSeries(series, value, duplicateAggregate);
        }

        public double PolynomialRegressionResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, int kOrder, out double[] coefficients)
        {
            if (_firstResult)
            {
                _pValues = Fit.Polynomial(XValues(), YValues(), kOrder);
                _firstResult = false;
            }

            double value = 0;

            for (var power = 0; power < _pValues.Length; power++)
            {
                value = value + _pValues[power] * Math.Pow(index, power);
            }

            coefficients = _pValues;
            return value;
        }
    }
}