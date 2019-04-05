using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using MongoAggregationSample.Model;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoAggregationSample
{
    public class Program
    {
        private static IMongoDatabase database;
        static string parentCollection = "customer";
        static string childCollection = "order";

        public static async Task Main(string[] args)
        {
            GetDatabase();
            await CreateCollection();
            var elapsed = await GetDataWithAgrregateLookup();
            Console.WriteLine($"It took {elapsed} seconds to get the data with aggregate lookup");
            Console.WriteLine();
            elapsed = await GetDataWithAgrregateLookupPipeline();
            Console.WriteLine($"It took {elapsed} seconds to get the data with aggregate lookup pipeline");
            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }

        private static async Task<int> GetDataWithAgrregateLookup()
        {
            var collection = GetCollection(parentCollection);
            var result = collection.Aggregate(new AggregateOptions
                    { Collation = new Collation("en", numericOrdering: true) })
                .Match(GetCustomFilter())
                .Sort(GetSortDefinition())
                .Skip(0).Limit(20)
                 .Lookup(childCollection, "_id", "Customer._id", childCollection);

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var bson = await result.ToListAsync();
            stopwatch.Stop();
            var contents = bson.ToJson();
            WriteToFile($"{parentCollection}_lookup", contents);
            return stopwatch.Elapsed.Seconds;
        }

        private static async Task<int> GetDataWithAgrregateLookupPipeline()
        {
            var collection = GetCollection(parentCollection);
            var foreignCollection = GetCollection(childCollection);

            var lookupPipeline = GetLookupPipeline();

            var result = collection.Aggregate(new AggregateOptions
                    { Collation = new Collation("en", numericOrdering: true) })
                .Match(GetCustomFilter())
                .Sort(GetSortDefinition())
                .Skip(0).Limit(20)
                .Lookup(foreignCollection, new BsonDocument { { "parentId", "$_id" } }, lookupPipeline, childCollection);

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var bson = await result.ToListAsync();
            stopwatch.Stop();
            var contents = bson.ToJson();
            WriteToFile($"{parentCollection}_lookup_pipeline", contents);
            return stopwatch.Elapsed.Seconds;
        }

        private static PipelineDefinition<BsonDocument, BsonDocument> GetLookupPipeline()
        {
            var sortBuilder = new SortDefinitionBuilder<BsonDocument>();
            var relationSortDefinition = sortBuilder.Ascending("_id");

            var lookupPipeline = new EmptyPipelineDefinition<BsonDocument>()
                .Match(new BsonDocument("$expr",
                    new BsonDocument("$and", new BsonArray
                    {
                        new BsonDocument("$eq", new BsonArray { "Customer_id", "$$parentId"}),
                        new BsonDocument("$not", new BsonArray
                        {
                            new BsonDocument("$in",
                                new BsonArray
                                {
                                    "Status", new BsonArray("Cancelled,OnHold")
                                })
                        })
                    })))
                .Sort(relationSortDefinition)
                .Project("{_id: 1, Status:1}");
            return lookupPipeline;
        }

        private static FilterDefinition<BsonDocument> GetCustomFilter()
        {
            var filterBuilder = new FilterDefinitionBuilder<BsonDocument>();
            var idFilter = filterBuilder.Eq("Status", "Active");
            var filter = filterBuilder.And(idFilter);
            return filter;
        }

        private static SortDefinition<BsonDocument> GetSortDefinition()
        {
            var sortBuilder = new SortDefinitionBuilder<BsonDocument>();
            var sortDefinition = sortBuilder.Ascending("_id");
            return sortDefinition;
        }

        private static void WriteToFile(string filename, string content)
        {
            var filePath = Directory.GetCurrentDirectory();
            filePath = $"{filePath}\\{filename}.json";
            using (var writer = File.CreateText(filePath))
            {
                writer.WriteLine(content); 
            }
        }

        private static void GetDatabase()
        {
            const string serverConnection = "mongodb://localhost:27017";
            const string dbName = "Sales";
            var client = new MongoClient(serverConnection);
            database = client.GetDatabase(dbName);
        }

        private static IMongoCollection<BsonDocument> GetCollection(string resourceName)
        {
            return database.GetCollection<BsonDocument>(resourceName);
        }

        private static async Task CreateCollection()
        {
            try
            {
            var collection = GetCollection(parentCollection);
            var foreignCollection = GetCollection(childCollection);

            collection.DeleteMany(new BsonDocument());
            foreignCollection.DeleteMany(new BsonDocument());

                for (var i = 1; i <= 6000; i++)
            {
                var customer = new Customer
                {
                    Id = i.ToString(),
                    FirstName= $"FirstName{i}",
                    LastName = $"LastName{i}",
                    Phone = $"020 - 000{i}",
                    Status = "Active",
                    Address = "Primary Address",
                    AlternateAddress = "Primary Address",
                    Age = 30,
                    City = "London",
                    Postcode = "TW3",
                    BirthDay = DateTime.Today,
                    EmailAddress = "abc'xyz.com",
                    LoyaltyDiscount = 5,
                    CreditLimit =  200000,
                    ShoppingMode = "Online",
                    ShoppingFrequency = "Monthly",
                    ShoppingInterests = "Household",
                    RefferedBy = (i+1).ToString(),
                    FirstName1 = $"FirstName{i}",
                    LastName1 = $"LastName{i}",
                    Phone1 = $"020 - 000{i}",
                    Status1 = "Active",
                    Address1 = "Primary Address",
                    AlternateAddress1 = "Primary Address",
                    Age1 = 30,
                    City1 = "London",
                    Postcode1 = "TW3",
                    BirthDay1 = DateTime.Today,
                    EmailAddress1 = "abc'xyz.com",
                    LoyaltyDiscount1 = 5,
                    CreditLimit1 = 200000,
                    ShoppingMode1 = "Online",
                    ShoppingFrequency1 = "Monthly",
                    ShoppingInterests1 = "Household",
                    RefferedBy1 = (i + 1).ToString(),
                    FirstName2 = $"FirstName{i}",
                    LastName2 = $"LastName{i}",
                    Phone2 = $"020 - 000{i}",
                    Status2 = "Active",
                    Address2 = "Primary Address",
                    AlternateAddress2 = "Primary Address",
                    Age2 = 30,
                    City2 = "London",
                    Postcode2 = "TW3",
                    BirthDay2 = DateTime.Today,
                    EmailAddress2 = "abc'xyz.com",
                    LoyaltyDiscount2 = 5,
                    CreditLimit2 = 200000,
                    ShoppingMode2 = "Online",
                    ShoppingFrequency2 = "Monthly",
                    ShoppingInterests2 = "Household",
                    RefferedBy2 = (i + 1).ToString(),
                    FirstName3 = $"FirstName{i}",
                    LastName3 = $"LastName{i}",
                    Phone3 = $"020 - 000{i}",
                    Status3 = "Active",
                    Address3 = "Primary Address",
                    AlternateAddress3 = "Primary Address",
                    Age3 = 30,
                    City3 = "London",
                    Postcode3 = "TW3",
                    BirthDay3 = DateTime.Today,
                    EmailAddress3 = "abc'xyz.com",
                    LoyaltyDiscount3 = 5,
                    CreditLimit3 = 200000,
                    ShoppingMode3 = "Online",
                    ShoppingFrequency3 = "Monthly",
                    ShoppingInterests3 = "Household",
                    RefferedBy3 = (i + 1).ToString()
                };
                if (i % 500 == 0)
                    customer.Status = "InActive";

                var customerBson = customer.ToBsonDocument();
                await collection.InsertOneAsync(customerBson);

                for (var j = 1; j <= 7; j++)
                {
                    var order = new Order
                    {
                        Id = $"{i.ToString()}00{j.ToString()}",
                        ProductQuantity = j,
                        DeliveryNotes = "Deliver on weekend",
                        OrderDate = DateTime.Today,
                        Status = "Shipped",
                        IsFragile = false,
                        IsGift = false,
                        IsRefundable = true,
                        InStock = true,
                        Amount = 500,
                        Discount = 10,
                        PaymentMode = "CreditCard",
                        CardNo = 12345678,
                        Bank = "HSBC",
                        CardType = "VISA",
                        ProductQuantity1 = j,
                        DeliveryNotes1 = "Deliver on weekend",
                        OrderDate1 = DateTime.Today,
                        Status1 = "Shipped",
                        IsFragile1 = false,
                        IsGift1 = false,
                        IsRefundable1 = true,
                        InStock1 = true,
                        Amount1 = 500,
                        Discount1 = 10,
                        PaymentMode1 = "CreditCard",
                        CardNo1 = 12345678,
                        Bank1 = "HSBC",
                        CardType1 = "VISA",
                        ProductQuantity2 = j,
                        DeliveryNotes2 = "Deliver on weekend",
                        OrderDate2 = DateTime.Today,
                        Status2 = "Shipped",
                        IsFragile2 = false,
                        IsGift2 = false,
                        IsRefundable2 = true,
                        InStock2 = true,
                        Amount2 = 500,
                        Discount2 = 10,
                        PaymentMode2 = "CreditCard",
                        CardNo2 = 12345678,
                        Bank2 = "HSBC",
                        CardType2 = "VISA",
                        ProductQuantity3 = j,
                        DeliveryNotes3 = "Deliver on weekend",
                        OrderDate3 = DateTime.Today,
                        Status3 = "Shipped",
                        IsFragile3 = false,
                        IsGift3 = false,
                        IsRefundable3 = true,
                        InStock3 = true,
                        Amount3 = 500,
                        Discount3 = 10,
                        PaymentMode3 = "CreditCard",
                        CardNo3 = 12345678,
                        Bank3 = "HSBC",
                        CardType3 = "VISA",
                        Customer = new Customer {Id = i.ToString() }
                    };
                    if (j % 4 == 0)
                        order.Status = "Processing";
                    if (j % 5 == 0)
                        order.Status = "Cancelled";
                    if (j % 6 == 0)
                        order.Status = "Delivered";
                    if (j % 7 == 0)
                        order.Status = "OnHold";

                    var orderBson = order.ToBsonDocument();
                    await foreignCollection.InsertOneAsync(orderBson);
                }
            }
                var indexFields = new List<string>{"_id", "Status"};
                await CreateIndexes(collection, indexFields);
                indexFields.Add("CustomerId");
                indexFields.Add("Customer._id");
                await CreateIndexes(foreignCollection, indexFields);

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private static async Task CreateIndexes(IMongoCollection<BsonDocument> collection,
            IEnumerable<string> indexFields)
        {
            foreach (var fieldName in indexFields)
            {
                var indexKey = new BsonDocument(fieldName, 1);
                await collection.Indexes.CreateOneAsync(
                    new CreateIndexModel<BsonDocument>(indexKey, new CreateIndexOptions {Name = fieldName}));
            }
        }
    }
}
