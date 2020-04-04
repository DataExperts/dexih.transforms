﻿using System;
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
        
        private double[] XValues() => Enumerable.Range(0, _cacheSeries.Count).Select(Convert.ToDouble).ToArray();
        private double[] YValues() => _cacheSeries.Values.OfType<SeriesValue<double>>().Select(c => c.Value).ToArray();
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Curve Fit", Name = "Linear Regression", Description = "Least-Squares fitting the points (x,y) to a line y : x -> a+b*x", ResultMethod = nameof(LinearRegressionResult), ResetMethod = nameof(Reset))]
        public void LinearRegression([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, [TransformFunctionVariable(EFunctionVariable.Forecast)] bool isForecast, double value, EAggregate duplicateAggregate = EAggregate.Sum)
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
        public void PolynomialRegression([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, double value, EAggregate duplicateAggregate = EAggregate.Sum)
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
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Curve Fit", Name = "Exponential Regression", 
            Description = "Least-Squares fitting the points (x,y) to an exponential y : x -> a*exp(r*x), returning its best fitting parameters as (a, r) tuple.", ResultMethod = nameof(ExponentialRegressionResult), ResetMethod = nameof(Reset))]
        public void ExponentialRegression([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, double value, EAggregate duplicateAggregate = EAggregate.Sum)
        {
            AddSeries(series, value, duplicateAggregate);
        }

        public double ExponentialRegressionResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, DirectRegressionMethod regressionMethod, out double a, out double r)
        {
            if (_firstResult)
            {
                var exponential = Fit.Exponential(XValues(), YValues(), regressionMethod);
                _exponentialA = exponential.Item1;
                _exponentialR = exponential.Item2;
            }


            var value = _exponentialA * Math.Exp(_exponentialR * index);

            a = _exponentialA;
            r = _exponentialR;
            
            return value;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Series, Category = "Curve Fit", Name = "Logarithm Regression", 
            Description = "Least-Squares fitting the points (x,y) to a logarithm y : x -> a + b*ln(x),", ResultMethod = nameof(LogarithmRegressionResult), ResetMethod = nameof(Reset))]
        public void LogarithmRegression([TransformFunctionVariable(EFunctionVariable.SeriesValue)]object series, double value, EAggregate duplicateAggregate = EAggregate.Sum)
        {
            AddSeries(series, value, duplicateAggregate);
        }

        public double LogarithmRegressionResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, DirectRegressionMethod regressionMethod, out double a, out double b)
        {
            if (_firstResult)
            {
                var exponential = Fit.Logarithm(XValues(), YValues(), regressionMethod);
                _logarithmA = exponential.Item1;
                _logarithmB = exponential.Item2;
            }

            var value = _logarithmA + _logarithmB * Math.Log(index);

            a = _logarithmA;
            b = _logarithmB;
            return value;
        }
    }
}