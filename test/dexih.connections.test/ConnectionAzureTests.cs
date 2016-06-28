//using dexih.connections.test;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;
//using Xunit;

//namespace dexih.connections.test
//{
//    public class ConnectionAzureTests
//    {
//        public ConnectionAzure GetConnection()
//        {
//            return new ConnectionAzure()
//            {
//                //Name = "Test Connection",
//                //ServerName = Convert.ToString(Helpers.AppSettings["Azure:ServerName"]),
//                //UserName = Convert.ToString(Helpers.AppSettings["Azure:UserName"]),
//                //Password = Convert.ToString(Helpers.AppSettings["Azure:Password"]),
//                UseConnectionString = true,
//                ConnectionString = "UseDevelopmentStorage=true"
//            };


//        }

//        [Fact]
//        public void Azure_Basic()
//        {
//            string database = "Test-" + Guid.NewGuid().ToString();

//            new UnitTests().Unit(GetConnection(), database);
//        }

//        [Fact]
//        public void Azure_Transform()
//        {
//            string database = "Test-" + Guid.NewGuid().ToString();

//            new TransformTests().Transform(GetConnection(), database);
//        }

//        [Fact]
//        public void Azure_Performance()
//        {

//            new PerformanceTests().Performance(GetConnection(), "Test-" + Guid.NewGuid().ToString(), 100);
//        }

//    }
//}
