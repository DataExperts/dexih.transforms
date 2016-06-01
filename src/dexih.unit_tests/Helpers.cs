using dexih.core;
using dexih.repository;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace dexih_unit_tests
{
    public static class Helpers
    {
        static IConfigurationRoot Configuration;

        static Helpers()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");

            builder.AddEnvironmentVariables();
            Configuration = builder.Build();

        }

        public static string EncryptionKey()
        {
            return Configuration["EncryptionKey"];
        }

        public static dexih_repositoryContext RepositoryConnection()
        {
            //create a new repository connection
            var optionsBuilder = new DbContextOptionsBuilder();
            optionsBuilder.UseSqlServer(Configuration["Data:DefaultConnection:ConnectionString"]);
            var DbContext = new dexih_repositoryContext(optionsBuilder.Options);

            return DbContext;
        }

        //creates a blank subscription for testing
        public static async Task<int> CreateSubscription(string name)
        {
            Subscriptions Subscriptions = new Subscriptions();

            Subscription subscription = new Subscription();
            subscription.Name = name + "-" + DateTime.Now.ToString("s");
            await subscription.Save(RepositoryConnection(), Subscriptions);
            return subscription.SubscriptionKey;
        }


        public static string TestExportsPath()
        {
            //return "C:\\Users\\Gary\\OneDrive\\Information Hub\\tests";
            //return "Z:\\OneDrive\\Information Hub\\tests";

            return Directory.GetCurrentDirectory() + "\\testfiles";
        }
    }
}
