using System;
using System.Collections.Specialized;
using dexih.functions.Query;
using Excel.FinancialFunctions;

namespace dexih.functions.financial
{
  public class FinancialFunctions
  {
    //The cache parameters are used by the functions to maintain a state during a transform process.
    private OrderedDictionary _cacheSeries;

    public bool Reset()
    {
      _cacheSeries?.Clear();
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

    /// <summary>
    ///  The accrued interest for a security that pays periodic interest ([learn more](http://office.microsoft.com/en-us/excel/HP052089791033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Accrued Interest (Periodic)",
      Description = "The accrued interest for a security that pays periodic interest.")]
    public double AccrInt(DateTime issue, DateTime firstInterest, DateTime settlement, double rate, double par,
      Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365, AccrIntCalcMethod calcMethod = AccrIntCalcMethod.FromFirstToSettlement)
    {
      return Financial.AccrInt(issue, firstInterest, settlement, rate, par, frequency, basis, calcMethod);
    }

    /// <summary>
    ///  The accrued interest for a security that pays interest at maturity ([learn more](http://office.microsoft.com/en-us/excel/HP052089801033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Accrued Interest (Maturity)",
      Description = "The accrued interest for a security that pays interest at maturity.")]
    public double AccrIntM(DateTime issue, DateTime settlement, double rate, double par, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.AccrIntM(issue, settlement, rate, par, basis);
    }

    /// <summary>
    ///  The depreciation for each accounting period by using a depreciation coefficient ([learn more](http://office.microsoft.com/en-us/excel/HP052089841033.aspx))
    ///  ExcelCompliant is used because Excel stores 13 digits. AmorDegrc algorithm rounds numbers
    ///  and returns different results unless the numbers get rounded to 13 digits before rounding them.
    ///  I.E. 22.49999999999999 is considered 22.5 by Excel, but 22.4 by the .NET framework
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Prorated Linear Depreciation (AmorDegrc)",
      Description = "The prorated linear depreciation of an asset for each accounting period.")]
    public double AmorDegrc(double cost, DateTime datePurchased, DateTime firstPeriod, double salvage, double period,
      double rate, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.AmorDegrc(cost, datePurchased, firstPeriod, salvage, period, rate, basis, false);
    }

    /// <summary>
    ///  The depreciation for each accounting period ([learn more](http://office.microsoft.com/en-us/excel/HP052089851033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Prorated Linear Depreciation (AmorLinc)",
      Description = "The prorated linear depreciation of an asset for each accounting period.")]
    public double AmorLinc(double cost, DateTime datePurchased, DateTime firstPeriod, double salvage, double period,
      double rate, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.AmorLinc(cost, datePurchased, firstPeriod, salvage, period, rate, basis);
    }

    /// <summary>
    ///  The number of days from the beginning of the coupon period to the settlement date ([learn more](http://office.microsoft.com/en-us/excel/HP052090301033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Coupon Days (Beginning to Settlement)",
      Description = "The number of days from the beginning of the coupon period to the settlement date.")]
    public double CoupDaysBS(DateTime settlement, DateTime maturity, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.CoupDaysBS(settlement, maturity, frequency, basis);
    }

    /// <summary>
    ///  The number of days in the coupon period that contains the settlement date ([learn more](http://office.microsoft.com/en-us/excel/HP052090311033.aspx))
    ///  The Excel algorithm seems wrong in that it doesn't respect `coupDays = coupDaysBS + coupDaysNC`
    ///  This equality should stand. The differs from Excel by +/- one or two days when the date spans a leap year.
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Coupon Days",
      Description = " The number of days in the coupon period that contains the settlement date")]
    public double CoupDays(DateTime settlement, DateTime maturity, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.CoupDays(settlement, maturity, frequency, basis);
    }

    /// <summary>
    ///  The number of days from the settlement date to the next coupon date ([learn more](http://office.microsoft.com/en-us/excel/HP052090321033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Coupon Days (Settlement to Next)",
      Description = "The number of days from the settlement date to the next coupon date")]
    public double CoupDaysNC(DateTime settlement, DateTime maturity, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.CoupDaysNC(settlement, maturity, frequency, basis);
    }

    /// <summary>
    ///  The next coupon date after the settlement date ([learn more](http://office.microsoft.com/en-us/excel/HP052090331033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Next Coupon Date",
      Description = "The next coupon date after the settlement date")]
    public DateTime CoupNCD(DateTime settlement, DateTime maturity, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.CoupNCD(settlement, maturity, frequency, basis);
    }

    /// <summary>
    ///  The number of coupons payable between the settlement date and maturity date ([learn more](http://office.microsoft.com/en-us/excel/HP052090341033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Number of Coupons",
      Description = "The number of coupons payable between the settlement date and maturity date")]
    public double CoupNum(DateTime settlement, DateTime maturity, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.CoupNum(settlement, maturity, frequency, basis);
    }

    /// <summary>
    ///  The previous coupon date before the settlement date ([learn more](http://office.microsoft.com/en-us/excel/HP052090351033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Previous Coupon",
      Description = "The previous coupon date before the settlement date")]
    public DateTime CoupPCD(DateTime settlement, DateTime maturity, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.CoupPCD(settlement, maturity, frequency, basis);
    }

    /// <summary>
    ///  The cumulative interest paid between two periods ([learn more](http://office.microsoft.com/en-us/excel/HP052090381033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Cumulative Interest",
      Description = "The cumulative interest paid between two periods")]
    public double CumIPmt(double rate, double nper, double pv, double startPeriod, double endPeriod, PaymentDue typ)
    {
      return Financial.CumIPmt(rate, nper, pv, startPeriod, endPeriod, typ);
    }

    /// <summary>
    ///  The cumulative principal paid on a loan between two periods ([learn more](http://office.microsoft.com/en-us/excel/HP052090391033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Cumulative Principal",
      Description = "The cumulative principal paid on a loan between two periods")]
    public double CumPrinc(double rate, double nper, double pv, double startPeriod, double endPeriod, PaymentDue typ = PaymentDue.BeginningOfPeriod)
    {
      return Financial.CumPrinc(rate, nper, pv, startPeriod, endPeriod, typ);
    }

    /// <summary>
    ///  The depreciation of an asset for a specified period by using the fixed-declining balance method
    ///  ([learn more](http://office.microsoft.com/en-us/excel/HP052090481033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Depreciation (Db)",
      Description = "The depreciation of an asset for a specified period by using the fixed-declining balance method")]
    public double Db(double cost, double salvage, double life, double period, double month = 12.0)
    {
      return Financial.Db(cost, salvage, life, period, month);
    }

    /// <summary>
    ///  The depreciation of an asset for a specified period by using the double-declining balance method or some other method that you specify ([learn more](http://office.microsoft.com/en-us/excel/HP052090511033.aspx))
    ///  Excel Ddb has two interesting characteristics:
    ///  1. It special cases ddb for fractional periods between 0 and 1 by considering them to be 1
    ///  2. It is inconsistent with VDB(..., True) for fractional periods, even if VDB(..., True) is defined to be the same as ddb. The algorithm for VDB is theoretically correct.
    ///  This function makes the same 1. adjustment.
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Depreciation (Ddb)",
      Description = "The depreciation of an asset for a specified period by using the double-declining balance method")]
    public double Ddb(double cost, double salvage, double life, double period, double factor = 2.0)
    {
      return Financial.Ddb(cost, salvage, life, period, factor);
    }

    /// <summary>
    ///  The discount rate for a security ([learn more](http://office.microsoft.com/en-us/excel/HP052090601033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Discount Rate",
      Description = "The discount rate for a security")]
    public double Disc(DateTime settlement, DateTime maturity, double pr, double redemption, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.Disc(settlement, maturity, pr, redemption, basis);
    }

    /// <summary>
    ///  Converts a dollar price, expressed as a fraction, into a dollar price, expressed as a decimal number ([learn more](http://office.microsoft.com/en-us/excel/HP052090641033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Dollar to Decimal",
      Description =
        "Converts a dollar price, expressed as a fraction, into a dollar price, expressed as a decimal number")]
    public double DollarDe(double fractionalDollar, double fraction)
    {
      return Financial.DollarDe(fractionalDollar, fraction);
    }

    /// <summary>
    ///  Converts a dollar price, expressed as a decimal number, into a dollar price, expressed as a fraction ([learn more](http://office.microsoft.com/en-us/excel/HP052090651033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Dollar to Fraction",
      Description =
        "Converts a dollar price, expressed as a decimal number, into a dollar price, expressed as a fraction")]
    public double DollarFr(double decimalDollar, double fraction)
    {
      return Financial.DollarFr(decimalDollar, fraction);
    }

    /// <summary>
    ///  The annual duration of a security with periodic interest payments ([learn more](http://office.microsoft.com/en-us/excel/HP052090701033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Duration of Security",
      Description = "The annual duration of a security with periodic interest payments.")]
    public double Duration(DateTime settlement, DateTime maturity, double coupon, double yld, Frequency frequency = Frequency.Annual,
      DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.Duration(settlement, maturity, coupon, yld, frequency, basis);
    }

    /// <summary>
    ///  The effective annual interest rate ([learn more](http://office.microsoft.com/en-us/excel/HP052090741033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Effective Interest Rate",
      Description = "The effective annual interest rate.")]
    public double Effect(double nominalRate, double npery)
    {
      return Financial.Effect(nominalRate, npery);
    }

    /// <summary>
    ///  The future value of an investment ([learn more](http://office.microsoft.com/en-us/excel/HP052090991033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Future Value",
      Description = "The future value of an investment.")]
    public double Fv(double rate, double nper, double pmt, double pv, PaymentDue typ = PaymentDue.BeginningOfPeriod)
    {
      return Financial.Fv(rate, nper, pmt, pv, typ);
    }



    /// <summary>
    ///  The interest rate for a fully invested security ([learn more](http://office.microsoft.com/en-us/excel/HP052091441033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Interest Rate",
      Description = "The interest rate for a fully invested security")]
    public double IntRate(DateTime settlement, DateTime maturity, double investment, double redemption,
      DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.IntRate(settlement, maturity, investment, redemption, basis);
    }

    /// <summary>
    ///  The interest payment for an investment for a given period ([learn more](http://office.microsoft.com/en-us/excel/HP052091451033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Interest Payment",
      Description = "The interest payment for an investment for a given period")]
    public double IPmt(double rate, double per, double nper, double pv, double fv, PaymentDue typ)
    {
      return Financial.IPmt(rate, per, nper, pv, fv, typ);
    }



    /// <summary>
    ///  Calculates the interest paid during a specific period of an investment ([learn more](http://office.microsoft.com/en-us/excel/HP052508401033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Interest Paid",
      Description = "Calculates the interest paid during a specific period of an investment")]
    public double ISPmt(double rate, double per, double nper, double pv)
    {
      return Financial.ISPmt(rate, per, nper, pv);
    }

    /// <summary>
    ///  The Macauley modified duration for a security with an assumed par value of $100 ([learn more](http://office.microsoft.com/en-us/excel/HP052091731033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Macauley Duration",
      Description = "The Macauley modified duration for a security with an assumed par value of $100")]
    public double MDuration(DateTime settlement, DateTime maturity, double coupon, double yld, Frequency frequency = Frequency.Annual,
      DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.MDuration(settlement, maturity, coupon, yld, frequency, basis);
    }


    /// <summary>
    ///  The annual nominal interest rate ([learn more](http://office.microsoft.com/en-us/excel/HP052091911033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Nominal Interest Rate",
      Description = "The annual nominal interest rate")]
    public double Nominal(double effectRate, double npery)
    {
      return Financial.Nominal(effectRate, npery);
    }

    /// <summary>
    ///  The number of periods for an investment ([learn more](http://office.microsoft.com/en-us/excel/HP052091981033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Periods of Investment",
      Description = "The number of periods for an investment")]
    public double NPer(double rate, double pmt, double pv, double fv, PaymentDue typ = PaymentDue.BeginningOfPeriod)
    {
      return Financial.NPer(rate, pmt, pv, fv, typ);
    }



    /// <summary>
    ///  The price per $100 face value of a security with an odd first period ([learn more](http://office.microsoft.com/en-us/excel/HP052092041033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Odd First Period Price",
      Description = " The price per $100 face value of a security with an odd first period")]
    public double OddFPrice(DateTime settlement, DateTime maturity, DateTime issue, DateTime firstCoupon, double rate,
      double yld, double redemption, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.OddFPrice(settlement, maturity, issue, firstCoupon, rate, yld, redemption, frequency, basis);
    }

    /// <summary>
    ///  The yield of a security with an odd first period ([learn more](http://office.microsoft.com/en-us/excel/HP052092051033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Odd First Period Yield",
      Description = "The yield of a security with an odd first period")]
    public double OddFYield(DateTime settlement, DateTime maturity, DateTime issue, DateTime firstCoupon, double rate,
      double pr, double redemption, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.OddFYield(settlement, maturity, issue, firstCoupon, rate, pr, redemption, frequency, basis);
    }

    /// <summary>
    ///  The price per $100 face value of a security with an odd last period ([learn more](http://office.microsoft.com/en-us/excel/HP052092061033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Odd Last Period Price",
      Description = "The price per $100 face value of a security with an odd last period")]
    public double OddLPrice(DateTime settlement, DateTime maturity, DateTime lastInterest, double rate, double yld,
      double redemption, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.OddLPrice(settlement, maturity, lastInterest, rate, yld, redemption, frequency, basis);
    }

    /// <summary>
    ///  The yield of a security with an odd last period ([learn more](http://office.microsoft.com/en-us/excel/HP052092071033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Odd Last Period Yield",
      Description = " The yield of a security with an odd last period")]
    public double OddLYield(DateTime settlement, DateTime maturity, DateTime lastInterest, double rate, double pr,
      double redemption, Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.OddLYield(settlement, maturity, lastInterest, rate, pr, redemption, frequency, basis);
    }

    /// <summary>
    ///  The periodic payment for an annuity ([learn more](http://office.microsoft.com/en-us/excel/HP052092151033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Periodic Payment for Annuity",
      Description = "The periodic payment for an annuity")]
    public double Pmt(double rate, double nper, double pv, double fv, PaymentDue typ = PaymentDue.BeginningOfPeriod)
    {
      return Financial.Pmt(rate, nper, pv, fv, typ);
    }

    /// <summary>
    ///  The payment on the principal for an investment for a given period ([learn more](http://office.microsoft.com/en-us/excel/HP052092181033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Payment on Principal",
      Description = "The payment on the principal for an investment for a given period")]
    public double PPmt(double rate, double per, double nper, double pv, double fv, PaymentDue typ = PaymentDue.BeginningOfPeriod)
    {
      return Financial.PPmt(rate, per, nper, pv, fv, typ);
    }

    /// <summary>
    ///  The price per $100 face value of a security that pays periodic interest ([learn more](http://office.microsoft.com/en-us/excel/HP052092191033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Price (per $100) of Security (pays periodic interest)",
      Description = "The price per $100 face value of a security that pays periodic interest")]
    public double Price(DateTime settlement, DateTime maturity, double rate, double yld, double redemption,
      Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.Price(settlement, maturity, rate, yld, redemption, frequency, basis);
    }

    /// <summary>
    ///  The price per $100 face value of a discounted security ([learn more](http://office.microsoft.com/en-us/excel/HP052092201033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Price (per $100) of Discounted Security",
      Description = "The price per $100 face value of a discounted security")]
    public double PriceDisc(DateTime settlement, DateTime maturity, double discount, double redemption,
      DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.PriceDisc(settlement, maturity, discount, redemption, basis);
    }

    /// <summary>
    ///  The price per $100 face value of a security that pays interest at maturity ([learn more](http://office.microsoft.com/en-us/excel/HP052092211033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Price (per $100) of security (pays at maturity)",
      Description = "The price per $100 face value of a security that pays interest at maturity")]
    public double PriceMat(DateTime settlement, DateTime maturity, DateTime issue, double rate, double yld,
      DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.PriceMat(settlement, maturity, issue, rate, yld, basis);
    }

    /// <summary>
    ///  The present value of an investment ([learn more](http://office.microsoft.com/en-us/excel/HP052092251033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Present Value",
      Description = "The present value of an investment")]
    public double Pv(double rate, double nper, double pmt, double fv, PaymentDue typ = PaymentDue.BeginningOfPeriod)
    {
      return Financial.Pv(rate, nper, pmt, fv, typ);
    }

    /// <summary>
    ///  The interest rate per period of an annuity ([learn more](http://office.microsoft.com/en-us/excel/HP052092321033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Rate per period of an annuity",
      Description = "The interest rate per period of an annuity")]
    public double Rate(double nper, double pmt, double pv, double fv, PaymentDue typ = PaymentDue.BeginningOfPeriod, double guess = 0.1)
    {
      return Financial.Rate(nper, pmt, pv, fv, typ, guess);
    }

    /// <summary>
    ///  The amount received at maturity for a fully invested security ([learn more](http://office.microsoft.com/en-us/excel/HP052092331033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Amount received at maturity",
      Description = "The amount received at maturity for a fully invested security")]
    public double Received(DateTime settlement, DateTime maturity, double investment, double discount,
      DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.Received(settlement, maturity, investment, discount, basis);
    }

    /// <summary>
    ///  The straight-line depreciation of an asset for one period ([learn more](http://office.microsoft.com/en-us/excel/HP052092631033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Straight-line depreciation of an asset",
      Description = "The straight-line depreciation of an asset for one period")]
    public double Sln(double cost, double salvage, double life)
    {
      return Financial.Sln(cost, salvage, life);
    }

    /// <summary>
    ///  The sum-of-years' digits depreciation of an asset for a specified period ([learn more](http://office.microsoft.com/en-us/excel/HP052093021033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Sum-of-years' digits depreciation of an asset",
      Description = "The sum-of-years' digits depreciation of an asset for a specified period")]
    public double Syd(double cost, double salvage, double life, double per)
    {
      return Financial.Syd(cost, salvage, life, per);
    }

    /// <summary>
    ///  The bond-equivalent yield for a Treasury bill ([learn more](http://office.microsoft.com/en-us/excel/HP052093091033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Bond-equivalent yield for a Treasury bill",
      Description = "The bond-equivalent yield for a Treasury bill")]
    public double TBillEq(DateTime settlement, DateTime maturity, double discount)
    {
      return Financial.TBillEq(settlement, maturity, discount);
    }

    /// <summary>
    ///  The price per $100 face value for a Treasury bill ([learn more](http://office.microsoft.com/en-us/excel/HP052093101033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Price per $100 face value for a Treasury bill",
      Description = "The price per $100 face value for a Treasury bill")]
    public double TBillPrice(DateTime settlement, DateTime maturity, double discount)
    {
      return Financial.TBillPrice(settlement, maturity, discount);
    }

    /// <summary>
    ///  The yield for a Treasury bill ([learn more](http://office.microsoft.com/en-us/excel/HP052093111033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Yield for a Treasury bill",
      Description = "The yield for a Treasury bill")]
    public double TBillYield(DateTime settlement, DateTime maturity, double pr)
    {
      return Financial.TBillYield(settlement, maturity, pr);
    }

    /// <summary>
    ///  The depreciation of an asset for a specified or partial period by using a declining balance method ([learn more](http://office.microsoft.com/en-us/excel/HP052093341033.aspx))
    ///  In the excel version of this algorithm the depreciation in the period (0,1) is not the same as the sum of the depreciations in periods (0,0.5) (0.5,1)
    ///  `VDB(100,10,13,0,0.5,1,0) + VDB(100,10,13,0.5,1,1,0) &lt;&gt; VDB(100,10,13,0,1,1,0)`
    ///  Notice that in Excel by using '1' (no_switch) instead of '0' as the last parameter everything works as expected.
    ///  In truth, the last parameter should have no influence in the calculation given that in the first period there is no switch to sln depreciation.
    ///  Overall, I think my algorithm is correct, even if it disagrees with Excel when startperiod is fractional.
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Depreciation of an asset",
      Description =
        "The depreciation of an asset for a specified or partial period by using a declining balance method")]
    public double Vdb(double cost, double salvage, double life, double startPeriod, double endPeriod, double factor = 2.0,
      VdbSwitch noSwitch = VdbSwitch.SwitchToStraightLine)
    {
      return Financial.Vdb(cost, salvage, life, startPeriod, endPeriod, factor, noSwitch);
    }

    /// <summary>
    ///  The yield on a security that pays periodic interest ([learn more](http://office.microsoft.com/en-us/excel/HP052093451033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial", Name = "Yield on a security",
      Description = "The yield on a security that pays periodic interest")]
    public double Yield(DateTime settlement, DateTime maturity, double rate, double pr, double redemption,
      Frequency frequency = Frequency.Annual, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.Yield(settlement, maturity, rate, pr, redemption, frequency, basis);
    }

    /// <summary>
    ///  The annual yield for a discounted security; for example, a Treasury bill ([learn more](http://office.microsoft.com/en-us/excel/HP052093461033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Yield on a discounted security",
      Description = "The annual yield for a discounted security; for example, a Treasury bill")]
    public double YieldDisc(DateTime settlement, DateTime maturity, double pr, double redemption, DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.YieldDisc(settlement, maturity, pr, redemption, basis);
    }

    /// <summary>
    ///  The annual yield of a security that pays interest at maturity ([learn more](http://office.microsoft.com/en-us/excel/HP052093471033.aspx))
    /// </summary>
    [TransformFunction(FunctionType = EFunctionType.Map, Category = "Financial",
      Name = "Annual yield of a security that pays interest at maturity",
      Description = "The annual yield of a security that pays interest at maturity")]
    public double YieldMat(DateTime settlement, DateTime maturity, DateTime issue, double rate, double pr,
      DayCountBasis basis = DayCountBasis.Actual365)
    {
      return Financial.YieldMat(settlement, maturity, issue, rate, pr, basis);
    }

  }
}