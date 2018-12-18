using System;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;

namespace dexih.functions.maths
{
    public class ProbabilityDistributionFunctions
    {
        private List<double> _data = new List<double>();
       
        public enum  EContinuousDistributionType
        {
            PDF, CDF, InvCDF, PDFLn
        }
        
        public enum  EDiscreteDistributionType
        {
            PMF, CDF, InvCDF, PMFLn
        }
        
        public bool Reset()
        {
            _data.Clear();
            return true;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Probability Distribution", Name = "Normal Distribution Estimate", Description = "Estimates the normal distribution parameters from the input sample data", ResultMethod = nameof(NormalDistributionEstimateResult), ResetMethod = nameof(Reset))]
        public void NormalDistributionEstimate(double value)
        {
            _data.Add(value);
        }

        public void NormalDistributionEstimateResult(
            out double entropy,
            out double maximum,
            out double mean,
            out double median,
            out double minimum,
            out double mode,
            out double skewness,
            out double variance
            )
        {
            var normal = Normal.Estimate(_data);
            entropy = normal.Entropy;
            maximum = normal.Maximum;
            mean = normal.Mean;
            median = normal.Median;
            minimum = normal.Minimum;
            mode = normal.Mode;
            skewness = normal.Skewness;
            variance = normal.Variance;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Normal Distribution",
            Description = "Calculates the normal distribution of the mean and standard deviation.", ResetMethod = nameof(Reset))]
        public double NormalDistribution(EContinuousDistributionType continuousDistributionType, double mean, double stddev, double x)
        {
            switch (continuousDistributionType)
            {
                    case EContinuousDistributionType.PDF:
                        return Normal.PDF(mean, stddev, x);
                    case EContinuousDistributionType.CDF:
                        return Normal.CDF(mean, stddev, x);
                    case EContinuousDistributionType.InvCDF:
                        return Normal.InvCDF(mean, stddev, x);
                    case EContinuousDistributionType.PDFLn:
                        return Normal.PDFLn(mean, stddev, x);
            }

            return 0;
        }

        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Probability Distribution", Name = "Log-Normal Distribution Estimate", Description = "Estimates the log-normal distribution parameters from the input sample data", ResultMethod = nameof(LogNormalDistributionEstimateResult), ResetMethod = nameof(Reset))]
        public void LogNormalDistributionEstimate(double value)
        {
            _data.Add(value);
        }

        public void LogNormalDistributionEstimateResult(
            out double entropy,
            out double maximum,
            out double mean,
            out double median,
            out double minimum,
            out double mode,
            out double skewness,
            out double variance,
            out double mu,
            out double sigma
        )
        {
            var logNormal = LogNormal.Estimate(_data);
            entropy = logNormal.Entropy;
            maximum = logNormal.Maximum;
            mean = logNormal.Mean;
            median = logNormal.Median;
            minimum = logNormal.Minimum;
            mode = logNormal.Mode;
            skewness = logNormal.Skewness;
            variance = logNormal.Variance;
            mu = logNormal.Mu;
            sigma = logNormal.Sigma;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Log Normal Distribution",
            Description = "Calculates the log-normal distribution.", ResetMethod = nameof(Reset))]
        public double LogNormalDistribution(EContinuousDistributionType continuousDistributionType, double mu, double sigma, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return LogNormal.PDF(mu, sigma, x);
                case EContinuousDistributionType.CDF:
                    return LogNormal.CDF(mu, sigma, x);
                case EContinuousDistributionType.InvCDF:
                    return LogNormal.InvCDF(mu, sigma, x);
                case EContinuousDistributionType.PDFLn:
                    return LogNormal.PDFLn(mu, sigma, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Continuous Uniform Distribution",
            Description = "Calculates the continuous uniform distribution.", ResetMethod = nameof(Reset))]
        public double ContinuousUniformDistribution(EContinuousDistributionType continuousDistributionType, double lower, double upper, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.CDF:
                    return ContinuousUniform.PDF(lower, upper, x);
                case EContinuousDistributionType.PDF:
                    return ContinuousUniform.CDF(lower, upper, x);
                case EContinuousDistributionType.InvCDF:
                    return ContinuousUniform.InvCDF(lower, upper, x);
                case EContinuousDistributionType.PDFLn:
                    return ContinuousUniform.PDFLn(lower, upper, x);
            }
            
            return 0;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Beta Distribution",
            Description = "Calculates the Continuous Univariate Beta distribution.", ResetMethod = nameof(Reset))]
        public double BetaDistribution(EContinuousDistributionType continuousDistributionType, double a, double b, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Beta.PDF(a, b, x);
                case EContinuousDistributionType.CDF:
                    return Beta.CDF(a, b, x);
                case EContinuousDistributionType.InvCDF:
                    return Beta.InvCDF(a, b, x);
                case EContinuousDistributionType.PDFLn:
                    return Beta.PDFLn(a, b, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Cauchy Distribution",
            Description = "Calculates the Continuous Univariate Cauchy distribution of the location and scale.", ResetMethod = nameof(Reset))]
        public double CauchyDistribution(EContinuousDistributionType continuousDistributionType, double location, double scale, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Cauchy.PDF(location, scale, x);
                case EContinuousDistributionType.CDF:
                    return Cauchy.CDF(location, scale, x);
                case EContinuousDistributionType.InvCDF:
                    return Cauchy.InvCDF(location, scale, x);
                case EContinuousDistributionType.PDFLn:
                    return Cauchy.PDFLn(location, scale, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Chi Distribution",
            Description = "Calculates the Continuous Univariate Chi distribution.", ResetMethod = nameof(Reset))]
        public double ChiDistribution(EContinuousDistributionType continuousDistributionType, double freedom, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Chi.PDF(freedom, x);
                case EContinuousDistributionType.CDF:
                    return Chi.CDF(freedom, x);
                case EContinuousDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {continuousDistributionType} is not supported for the Chi Distribution");
                case EContinuousDistributionType.PDFLn:
                    return Chi.PDFLn(freedom, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Chi Squared Distribution",
            Description = "Calculates the Continuous Univariate Chi Squared distribution.", ResetMethod = nameof(Reset))]
        public double ChiSquaredDistribution(EContinuousDistributionType continuousDistributionType, double freedom, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return ChiSquared.PDF(freedom, x);
                case EContinuousDistributionType.CDF:
                    return ChiSquared.CDF(freedom, x);
                case EContinuousDistributionType.InvCDF:
                    return ChiSquared.InvCDF(freedom, x);
                case EContinuousDistributionType.PDFLn:
                    return ChiSquared.PDFLn(freedom, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Erlang Distribution",
            Description = "Calculates the Continuous Univariate Erlang distribution.", ResetMethod = nameof(Reset))]
        public double ErlangDistribution(EContinuousDistributionType continuousDistributionType, int shape, double rate, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Erlang.PDF(shape, rate, x);
                case EContinuousDistributionType.CDF:
                    return Erlang.CDF(shape, rate, x);
                case EContinuousDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {continuousDistributionType} is not supported for the Erlang Distribution");
                case EContinuousDistributionType.PDFLn:
                    return Erlang.PDFLn(shape, rate, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Exponential Distribution",
            Description = "Calculates the Continuous Univariate Exponential distribution.", ResetMethod = nameof(Reset))]
        public double ExponentialDistribution(EContinuousDistributionType continuousDistributionType, double rate, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Exponential.PDF(rate, x);
                case EContinuousDistributionType.CDF:
                    return Exponential.CDF(rate, x);
                case EContinuousDistributionType.InvCDF:
                    return Exponential.InvCDF(rate, x);
                case EContinuousDistributionType.PDFLn:
                    return Exponential.PDFLn(rate, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "FDistribution Distribution",
            Description = "Calculates the Continuous Univariate Fisher-Snedecor (F-Distribution) distribution.", ResetMethod = nameof(Reset))]
        public double FisherSnedecorDistribution(EContinuousDistributionType continuousDistributionType, double d1, double d2, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return FisherSnedecor.PDF(d1, d2, x);
                case EContinuousDistributionType.CDF:
                    return FisherSnedecor.CDF(d1, d2, x);
                case EContinuousDistributionType.InvCDF:
                    return FisherSnedecor.InvCDF(d1, d2, x);
                case EContinuousDistributionType.PDFLn:
                    return FisherSnedecor.PDFLn(d1, d2, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Gamma Distribution",
            Description = "Calculates the Continuous Univariate Gamma distribution.", ResetMethod = nameof(Reset))]
        public double GammaDistribution(EContinuousDistributionType continuousDistributionType, int shape, double rate, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Gamma.PDF(shape, rate, x);
                case EContinuousDistributionType.CDF:
                    return Gamma.CDF(shape, rate, x);
                case EContinuousDistributionType.InvCDF:
                    return Gamma.InvCDF(shape, rate, x);
                case EContinuousDistributionType.PDFLn:
                    return Gamma.PDFLn(shape, rate, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Inverse Gamma Distribution",
            Description = "Calculates the Continuous Univariate Inverse Gamma distribution.", ResetMethod = nameof(Reset))]
        public double InverseGammaDistribution(EContinuousDistributionType continuousDistributionType, int shape, double scale, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return InverseGamma.PDF(shape, scale, x);
                case EContinuousDistributionType.CDF:
                    return InverseGamma.CDF(shape, scale, x);
                case EContinuousDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {continuousDistributionType} is not supported for the Inverse Gamma Distribution");
                case EContinuousDistributionType.PDFLn:
                    return InverseGamma.PDFLn(shape, scale, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Laplace Distribution",
            Description = "Calculates the Continuous Univariate Laplace distribution.", ResetMethod = nameof(Reset))]
        public double LaplaceDistribution(EContinuousDistributionType continuousDistributionType, int location, double scale, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Laplace.PDF(location, scale, x);
                case EContinuousDistributionType.CDF:
                    return Laplace.CDF(location, scale, x);
                case EContinuousDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {continuousDistributionType} is not supported for the Laplace Distribution");
                case EContinuousDistributionType.PDFLn:
                    return Laplace.PDFLn(location, scale, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Pareto Distribution",
            Description = "Calculates the Continuous Univariate Pareto distribution.", ResetMethod = nameof(Reset))]
        public double ParetoDistribution(EContinuousDistributionType continuousDistributionType, int scale, double shape, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Pareto.PDF(scale, shape, x);
                case EContinuousDistributionType.CDF:
                    return Pareto.CDF(scale, shape, x);
                case EContinuousDistributionType.InvCDF:
                    return Pareto.InvCDF(scale, shape, x);
                case EContinuousDistributionType.PDFLn:
                    return Pareto.PDFLn(scale, shape, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Rayleigh Distribution",
            Description = "Calculates the Continuous Univariate Rayleigh distribution.", ResetMethod = nameof(Reset))]
        public double RayleighDistribution(EContinuousDistributionType continuousDistributionType, int scale, double rate, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Rayleigh.PDF(scale, x);
                case EContinuousDistributionType.CDF:
                    return Rayleigh.CDF(scale, x);
                case EContinuousDistributionType.InvCDF:
                    return Rayleigh.InvCDF(scale, x);
                case EContinuousDistributionType.PDFLn:
                    return Rayleigh.PDFLn(scale, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Stable Distribution",
            Description = "Calculates the Continuous Univariate Stable distribution.", ResetMethod = nameof(Reset))]
        public double StableDistribution(EContinuousDistributionType continuousDistributionType, double alpha, double beta, double scale, double location, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Stable.PDF(alpha, beta, scale, location, x);
                case EContinuousDistributionType.CDF:
                    return Stable.CDF(alpha, beta, scale, location, x);
                case EContinuousDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {continuousDistributionType} is not supported for the Stable Distribution");
                case EContinuousDistributionType.PDFLn:
                    return Stable.PDFLn(alpha, beta, scale, location, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "StudentT Distribution",
            Description = "Calculates the Continuous Univariate StudentT distribution.", ResetMethod = nameof(Reset))]
        public double StudentTDistribution(EContinuousDistributionType continuousDistributionType, int location, double scale, double freedom, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return StudentT.PDF(location, scale, freedom, x);
                case EContinuousDistributionType.CDF:
                    return StudentT.CDF(location, scale, freedom, x);
                case EContinuousDistributionType.InvCDF:
                    return StudentT.InvCDF(location, scale, freedom, x);
                case EContinuousDistributionType.PDFLn:
                    return StudentT.PDFLn(location, scale, freedom, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Probability Distribution", Name = "Weibull Distribution Estimate", Description = "Estimates the Weibull distribution parameters from the input sample data", ResultMethod = nameof(WeibullDistributionEstimateResult), ResetMethod = nameof(Reset))]
        public void WeibullDistributionEstimate(double value)
        {
            _data.Add(value);
        }

        public void WeibullDistributionEstimateResult(
            out double shape,
            out double scale,
            out double entropy,
            out double maximum,
            out double mean,
            out double median,
            out double minimum,
            out double mode,
            out double skewness,
            out double variance
        )
        {
            var estimate = Weibull.Estimate(_data);
            entropy = estimate.Entropy;
            maximum = estimate.Maximum;
            mean = estimate.Mean;
            median = estimate.Median;
            minimum = estimate.Minimum;
            mode = estimate.Mode;
            skewness = estimate.Skewness;
            variance = estimate.Variance;
            shape = estimate.Shape;
            scale = estimate.Scale;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Weibull Distribution",
            Description = "Calculates the Continuous Univariate Weibull distribution.", ResetMethod = nameof(Reset))]
        public double WeibullDistribution(EContinuousDistributionType continuousDistributionType, int shape, double scale, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Weibull.PDF(shape, scale, x);
                case EContinuousDistributionType.CDF:
                    return Weibull.CDF(shape, scale, x);
                case EContinuousDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {continuousDistributionType} is not supported for the Weibull Distribution");
                case EContinuousDistributionType.PDFLn:
                    return Weibull.PDFLn(shape, scale, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Triangular Distribution",
            Description = "Calculates the Continuous Univariate Triangular distribution.", ResetMethod = nameof(Reset))]
        public double TriangularDistribution(EContinuousDistributionType continuousDistributionType, int lower, double upper, double mode, double x)
        {
            switch (continuousDistributionType)
            {
                case EContinuousDistributionType.PDF:
                    return Triangular.PDF(lower, upper, mode, x);
                case EContinuousDistributionType.CDF:
                    return Triangular.CDF(lower, upper, mode, x);
                case EContinuousDistributionType.InvCDF:
                    return Triangular.InvCDF(lower, upper, mode, x);
                case EContinuousDistributionType.PDFLn:
                    return Triangular.PDFLn(lower, upper, mode, x);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Discrete Uniform Distribution",
            Description = "Calculates the Discrete Univariate Discrete Uniform distribution.")]
        public double DiscreteUniformDistribution(EDiscreteDistributionType discreteDistributionType, int lower, int upper, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return DiscreteUniform.PMF(lower, upper, k);
                case EDiscreteDistributionType.CDF:
                    return DiscreteUniform.CDF(lower, upper, k);
                case EDiscreteDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {discreteDistributionType} is not supported for the Discrete Uniform Distribution");
                case EDiscreteDistributionType.PMFLn:
                    return DiscreteUniform.PMFLn(lower, upper, k);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Bernoulli Distribution",
            Description = "Calculates the Discrete Univariate Bernoulli distribution.")]
        public double BernoulliUniformDistribution(EDiscreteDistributionType discreteDistributionType, double p, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return Bernoulli.PMF(p, k);
                case EDiscreteDistributionType.CDF:
                    return Bernoulli.CDF(p, k);
                case EDiscreteDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {discreteDistributionType} is not supported for the Bernoulli Distribution");
                case EDiscreteDistributionType.PMFLn:
                    return Bernoulli.PMFLn(p, k);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Binomial Distribution",
            Description = "Calculates the Discrete Univariate Binomial distribution.")]
        public double BinomialUniformDistribution(EDiscreteDistributionType discreteDistributionType, double p, int n, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return Binomial.PMF(p, n, k);
                case EDiscreteDistributionType.CDF:
                    return Binomial.CDF(p, n, k);
                case EDiscreteDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {discreteDistributionType} is not supported for the Binomial Distribution");
                case EDiscreteDistributionType.PMFLn:
                    return Binomial.PMFLn(p, n, k);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "NegativeBinomial Distribution",
            Description = "Calculates the Discrete Univariate NegativeBinomial distribution.")]
        public double NegativeBinomialUniformDistribution(EDiscreteDistributionType discreteDistributionType, double r, double p, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return NegativeBinomial.PMF(r, p, k);
                case EDiscreteDistributionType.CDF:
                    return NegativeBinomial.CDF(r, p, k);
                case EDiscreteDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {discreteDistributionType} is not supported for the NegativeBinomial Distribution");
                case EDiscreteDistributionType.PMFLn:
                    return NegativeBinomial.PMFLn(r, p, k);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Geometric Distribution",
            Description = "Calculates the Discrete Univariate Geometric distribution.")]
        public double GeometricUniformDistribution(EDiscreteDistributionType discreteDistributionType, double p, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return Geometric.PMF(p, k);
                case EDiscreteDistributionType.CDF:
                    return Geometric.CDF(p, k);
                case EDiscreteDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {discreteDistributionType} is not supported for the Geometric Distribution");
                case EDiscreteDistributionType.PMFLn:
                    return Geometric.PMFLn(p, k);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Hyper-Geometric Distribution",
            Description = "Calculates the Discrete Univariate HyperGeometric distribution.")]
        public double HyperGeometricUniformDistribution(EDiscreteDistributionType discreteDistributionType, int population, int success, int draws, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return Hypergeometric.PMF(population, success, draws, k);
                case EDiscreteDistributionType.CDF:
                    return Hypergeometric.CDF(population, success, draws, k);
                case EDiscreteDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {discreteDistributionType} is not supported for the Hyper-Geometric Distribution");
                case EDiscreteDistributionType.PMFLn:
                    return Hypergeometric.PMFLn(population, success, draws, k);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Poisson Distribution",
            Description = "Calculates the Discrete Univariate Poisson distribution.")]
        public double PoissonUniformDistribution(EDiscreteDistributionType discreteDistributionType, double lambda, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return Poisson.PMF(lambda, k);
                case EDiscreteDistributionType.CDF:
                    return Poisson.CDF(lambda, k);
                case EDiscreteDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {discreteDistributionType} is not supported for the Poisson Distribution");
                case EDiscreteDistributionType.PMFLn:
                    return Poisson.PMFLn(lambda, k);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Categorical Distribution",
            Description = "Calculates the Discrete Univariate Categorical distribution.")]
        public double CategoricalUniformDistribution(EDiscreteDistributionType discreteDistributionType, double[] probabilityMass, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return Categorical.PMF(probabilityMass, k);
                case EDiscreteDistributionType.CDF:
                    return Categorical.CDF(probabilityMass, k);
                case EDiscreteDistributionType.InvCDF:
                    return Categorical.InvCDF(probabilityMass, k);
                case EDiscreteDistributionType.PMFLn:
                    return Categorical.PMFLn(probabilityMass, k);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Conway-Maxwell-Poisson Distribution",
            Description = "Calculates the Discrete Univariate ConwayMaxwellPoisson distribution.")]
        public double ConwayMaxwellPoissonUniformDistribution(EDiscreteDistributionType discreteDistributionType, double lambda, double nu, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return ConwayMaxwellPoisson.PMF(lambda, nu, k);
                case EDiscreteDistributionType.CDF:
                    return ConwayMaxwellPoisson.CDF(lambda, nu, k);
                case EDiscreteDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {discreteDistributionType} is not supported for the Conway-Maxwell-Poisson Distribution");
                case EDiscreteDistributionType.PMFLn:
                    return ConwayMaxwellPoisson.PMFLn(lambda, nu, k);
            }

            return 0;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Probability Distribution", Name = "Zipf Distribution",
            Description = "Calculates the Discrete Univariate Zipf distribution.")]
        public double ZipfUniformDistribution(EDiscreteDistributionType discreteDistributionType, double s, int n, int k)
        {
            switch (discreteDistributionType)
            {
                case EDiscreteDistributionType.PMF:
                    return Zipf.PMF(s, n, k);
                case EDiscreteDistributionType.CDF:
                    return Zipf.CDF(s, n, k);
                case EDiscreteDistributionType.InvCDF:
                    throw new Exception($"Distribution type: {discreteDistributionType} is not supported for the Zipf Distribution");
                case EDiscreteDistributionType.PMFLn:
                    return Zipf.PMFLn(s, n, k);
            }

            return 0;
        }
    }
}