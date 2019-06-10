//using System;
//using System.Collections.Generic;
//using System.Threading;
//
//namespace dexih.functions
//{
//
//    /// <summary>
//    /// Retry is used for connections and other activities which 
//    /// </summary>
//    public static class Retry
//    {
//        public static void Do(
//            Action action,
//            TimeSpan retryInterval,
//            int retryCount = 3)
//        {
//            Do<object>(() =>
//            {
//                action();
//                return null;
//            }, retryInterval, retryCount);
//        }
//
//        public static T Do<T>(
//            Func<T> action,
//            TimeSpan retryInterval,
//            int retryCount = 3)
//        {
//            var exceptions = new List<Exception>();
//
//            for (var retry = 0; retry < retryCount; retry++)
//            {
//                try
//                {
//                    if (retry > 0)
//                        Thread.Sleep(retryInterval);
//                    var returnValue = action();
//                    return returnValue;
//                }
//                catch (Exception ex)
//                {
//                    exceptions.Add(ex);
//                }
//            }
//
//            throw new AggregateException(exceptions);
//        }
//    }
//}
