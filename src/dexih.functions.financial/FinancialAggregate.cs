using System;
using System.Collections.Generic;
using Excel.FinancialFunctions;

namespace dexih.functions.financial
{
    public class FinancialAggregate
    {
        //The cache parameters are used by the functions to maintain a state during a transform process.
        private readonly List<double> _cacheData = new List<double>();
        private readonly List<DateTime> _cacheDates = new List<DateTime>();

        public bool Reset()
        {
            _cacheData.Clear();
            _cacheDates.Clear();
            return true;
        }

        /// <summary>
        ///  The future value of an initial principal after applying a series of compound interest rates ([learn more](http://office.microsoft.com/en-us/excel/HP052091001033.aspx))
        /// </summary>
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Financial", Name = "Future Value Schedule",
            Description = "The future value of an initial principal after applying a series of compound interest rates", ResetMethod = nameof(Reset), ResultMethod = nameof(FvScheduleResult))]
        public void FvSchedule(double schedule)
        {
            _cacheData.Add(schedule);
        }
        
        public double FvScheduleResult(double principal)
        {
            return Financial.FvSchedule(principal, _cacheData);
        }
        
        /// <summary>
        ///  The internal rate of return for a series of cash flows ([learn more](http://office.microsoft.com/en-us/excel/HP052091461033.aspx))
        /// </summary>
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Financial", Name = "Internal Rate of Return (with guess)",
            Description = "The internal rate of return for a series of cash flows", ResetMethod = nameof(Reset), ResultMethod = nameof(IrrGuessResult))]
        public void IrrGuess(double value)
        {
            _cacheData.Add(value);
        }
        
        public double IrrGuessResult(double guess)
        {
            return Financial.Irr(_cacheData, guess);
        }

        /// <summary>
        ///  The internal rate of return for a series of cash flows ([learn more](http://office.microsoft.com/en-us/excel/HP052091461033.aspx))
        /// </summary>
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Financial", Name = "Internal Rate of Return",
            Description = "The internal rate of return for a series of cash flows", ResetMethod = nameof(Reset), ResultMethod = nameof(IrrResult))]
        public void Irr(double value)
        {
            _cacheData.Add(value);
        }
        
        public double IrrResult()
        {
            return Financial.Irr(_cacheData, 0.1);
        }
        
        /// <summary>
        ///  The internal rate of return where positive and negative cash flows are financed at different rates ([learn more](http://office.microsoft.com/en-us/excel/HP052091801033.aspx))
        /// </summary>
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Financial", Name = "Internal Rate of Return",
            Description =
                "The internal rate of return where positive and negative cash flows are financed at different rates", ResetMethod = nameof(Reset), ResultMethod = nameof(MirrResult))]
        public void Mirr(double value)
        {
            _cacheData.Add(value);
        }
        
        public double MirrResult(double financeRate, double reinvestRate)
        {
            return Financial.Mirr(_cacheData, financeRate, reinvestRate);
        }

        
        /// <summary>
        ///  The net present value of an investment based on a series of periodic cash flows and a discount rate ([learn more](http://office.microsoft.com/en-us/excel/HP052091991033.aspx))
        /// </summary>
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Financial", Name = "Net Present Value",
            Description =
                "The net present value of an investment based on a series of periodic cash flows and a discount rate", ResetMethod = nameof(Reset), ResultMethod = nameof(NpvResult))]
        public void Npv(double value)
        {
            _cacheData.Add(value);
        }
        
        public double NpvResult(double rate)
        {
            if (rate == -1.0)
            {
                throw new Exception("R cannot be -100%");
            }

            return Financial.Npv(rate, _cacheData);
        }
        
        /// <summary>
        ///  The internal rate of return for a schedule of cash flows that is not necessarily periodic ([learn more](http://office.microsoft.com/en-us/excel/HP052093411033.aspx))
        /// </summary>
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Financial", Name = "Internal rate of return (with guess)",
            Description = "The internal rate of return for a schedule of cash flows that is not necessarily periodic", ResetMethod = nameof(Reset), ResultMethod = nameof(XIrrGuessResult))]
        public void XIrrGuess(double value, DateTime date)
        {
            _cacheData.Add(value);
            _cacheDates.Add(date);
        }
        
        public double XIrrGuessResult(double guess)
        {
            return Financial.XIrr(_cacheData, _cacheDates, guess);
        }

        /// <summary>
        ///  The internal rate of return for a schedule of cash flows that is not necessarily periodic ([learn more](http://office.microsoft.com/en-us/excel/HP052093411033.aspx))
        /// </summary>
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Financial", Name = "Internal rate of return",
            Description = "The internal rate of return for a schedule of cash flows that is not necessarily periodic", ResetMethod = nameof(Reset), ResultMethod = nameof(XIrrResult))]
        public void XIrr(double value, DateTime date)
        {
            _cacheData.Add(value);
            _cacheDates.Add(date);
        }
        
        public double XIrrResult()
        {
            return Financial.XIrr(_cacheData, _cacheDates, 0.1);
        }

        /// <summary>
        ///  The net present value for a schedule of cash flows that is not necessarily periodic ([learn more](http://office.microsoft.com/en-us/excel/HP052093421033.aspx))
        /// </summary>
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Financial", Name = "Net present value",
            Description = "The net present value for a schedule of cash flows that is not necessarily periodic", ResetMethod = nameof(Reset), ResultMethod = nameof(XNpvResult))]
        public void XNpv(double value, DateTime date)
        {
            _cacheData.Add(value);
            _cacheDates.Add(date);
        }
        
        public double XNpvResult(double rate, IEnumerable<double> values, IEnumerable<DateTime> dates)
        {
            return Financial.XNpv(rate, values, dates);
        }
    }
}