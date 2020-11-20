namespace dexih.functions.ml
{
    /// <summary>
    /// Copy of the regression metrics class which includes comments.
    /// </summary>
    public class RegressionMetrics
    {
        /// <summary>Gets the absolute loss of the model.</summary>
        /// <remarks>
        /// The absolute loss is defined as
        /// L1 = (1/m) * sum( abs( yi - y'i))
        /// where m is the number of instances in the test set.
        /// y'i are the predicted labels for each instance.
        /// yi are the correct labels of each instance.
        /// </remarks>
        [TransformFunctionParameter(Description = "The absolute loss of the model.")]
        public double MeanAbsoluteError { get; set; }

        /// <summary>Gets the squared loss of the model.</summary>
        /// <remarks>
        /// The squared loss is defined as
        /// L2 = (1/m) * sum(( yi - y'i)^2)
        /// where m is the number of instances in the test set.
        /// y'i are the predicted labels for each instance.
        /// yi are the correct labels of each instance.
        /// </remarks>
        [TransformFunctionParameter(Description = "The squared loss of the model.")]
        public double MeanSquaredError { get; set; }

        /// <summary>
        /// Gets the root mean square loss (or RMS) which is the square root of the L2 loss.
        /// </summary>
        [TransformFunctionParameter(Description =
            "The root mean square loss (or RMS) which is the square root of the L2 loss.")]
        public double RootMeanSquaredError { get; set; }

        /// <summary>Gets the result of user defined loss function.</summary>
        /// <remarks>
        /// This is the average of a loss function defined by the user,
        /// computed over all the instances in the test set.
        /// </remarks>
        [TransformFunctionParameter(Description = "The result of user defined loss function.")]
        public double LossFunction { get; set; }

        /// <summary>
        /// Gets the R squared value of the model, which is also known as
        /// the coefficient of determination​.
        /// </summary>
        [TransformFunctionParameter(Description =
            "The R squared value of the model, which is also known as the coefficient of determination​.")]
        public double RSquared { get; set; }

    }
}