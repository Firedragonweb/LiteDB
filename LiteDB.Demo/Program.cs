using LiteDB;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Media;

namespace LiteDB.Demo
{
    public struct TestStruct
    {
        public int A { get; set; }
    }
    public class TestDocument
    {
        [BsonId]
        public Guid Id { get; set; } = Guid.NewGuid();

        public string Name { get; set; }
        
    }

    public class PropertyDocument<T> : PropertyDocument
    {
        public T Data { get; set; }
    }

    public class PropertyDocument
    {
        [BsonId]
        public Guid Id { get; set; }
        public string Name { get; set; }
        public int Version { get; set; }
        public DateTime LastWrite { get; set; }
    }

    public class GlobalSettingsData
    {
        public Color UiColor { get; set; }

        public string FeedbackEmailAdress { get; set; }

        public bool IsEquivalentTo(object other)
        {
            if (ReferenceEquals(this, other)) return true;
            if (!(other is GlobalSettingsData casted)) return false;

            return UiColor.Equals(casted.UiColor) &&
                   string.Equals(FeedbackEmailAdress, casted.FeedbackEmailAdress);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            File.Delete("C:\\Temp\\bla.db");
            //Thread.CurrentThread.CurrentCulture = new CultureInfo("de-DE");
            using (LiteDatabase db = new LiteDatabase("C:\\Temp\\bla.db"))
            {
                LiteCollection<PropertyDocument<GlobalSettingsData>> liteCollection = db.GetCollection<PropertyDocument<GlobalSettingsData>>("PropertyDocuments");
                liteCollection.Upsert(new PropertyDocument<GlobalSettingsData>
                {
                    Name = "GlobalStyleSettings",
                    Version = 0,
                    Data = new GlobalSettingsData() { UiColor = Colors.Yellow, FeedbackEmailAdress = "a@b.d" },
                    LastWrite = DateTime.UtcNow - TimeSpan.FromHours(1)
                });
                liteCollection.Upsert(new PropertyDocument<GlobalSettingsData>
                {
                    Name = "GlobalStyleSettings",
                    Version = 1,
                    Data = new GlobalSettingsData() { UiColor = Colors.Green, FeedbackEmailAdress = "a@b.de" },
                    LastWrite = DateTime.UtcNow
                });

                liteCollection.EnsureIndex(doc => doc.Name);
                liteCollection.EnsureIndex(doc => doc.Version);
                liteCollection.EnsureIndex(doc => doc.LastWrite);
            }

            using (LiteDatabase db = new LiteDatabase("C:\\Temp\\bla.db"))
            {
                LiteCollection<PropertyDocument<GlobalSettingsData>> collection = db.GetCollection<PropertyDocument<GlobalSettingsData>>("PropertyDocuments");
                collection.EnsureIndex("CombiIndex", "$.Name + '_' + $.Version", true);
                collection.EnsureIndex("ModifiedIndex", "[$.Name, $.LastWrite, $.Version]");
                
                //var blub = collection.Find(x => x.Name == "GlobalStyleSettings").ToList();
                //var bla = collection.FindOne(x => x.Name == "GlobalStyleSettings" && x.Version == 1);
                var bla = collection.FindOne(Query.EQ("CombiIndex", new BsonValue("GlobalStyleSettings_1")), shallowMode: true);
                //var bla = collection.Max("ModifiedIndex");
                //bool exists = collection.Exists(Query.GT("ModifiedIndex",
                //    new BsonArray(new[]
                //    {
                //        new BsonValue("GlobalStyleSettings_0"),
                //        new BsonValue(DateTime.UtcNow - TimeSpan.FromMinutes(30))
                //    })));
                //IEnumerable<BsonDocument> find = db.Engine.Find("PropertyDocuments", Query.EQ("CombiIndex", new BsonValue("GlobalStyleSettings_1"))).ToList();
            }

            //Thread.CurrentThread.CurrentCulture = new CultureInfo("cs-CZ");
            //using (LiteDatabase db = new LiteDatabase("C:\\Temp\\Customizing_1.ecdc"))
            //{
            //    LiteCollection<PropertyDocument<GlobalSettingsData>> collection = db.GetCollection<PropertyDocument<GlobalSettingsData>>("PropertyDocuments");
            //    var blub = collection.FindAll().ToList();
            //    var bla = collection.FindOne(x => x.Name == "GlobalSettings" && x.Version == 0);
            //}

            var timer = new Stopwatch();
            ITest test = new LiteDB_Paging();
            //ITest test = new SQLite_Paging();

            Console.WriteLine("Testing: {0}", test.GetType().Name);

            test.Init();

            Console.WriteLine("Populating 100.000 documents...");

            timer.Start();
            test.Populate(ReadDocuments());
            timer.Stop();

            Console.WriteLine("Done in {0}ms", timer.ElapsedMilliseconds);

            timer.Restart();
            var counter = test.Count();
            timer.Stop();
            
            Console.WriteLine("Result query counter: {0} ({1}ms)", counter, timer.ElapsedMilliseconds);
            
            var input = "0";
            
            while (input != "")
            {
                var skip = Convert.ToInt32(input);
                var limit = 10;
            
                timer.Restart();
                var result = test.Fetch(skip, limit);
                timer.Stop();
            
                foreach(var doc in result)
                {
                    Console.WriteLine(
                        doc["_id"].AsString.PadRight(6) + " - " +
                        doc["name"].AsString.PadRight(30) + "  -> " +
                        doc["age"].AsInt32);
                }
            
                Console.Write("\n({0}ms) => Enter skip index: ", timer.ElapsedMilliseconds);
                input = Console.ReadLine();
            }
            
            Console.WriteLine("End");
            Console.ReadKey();
        }

        static IEnumerable<BsonDocument> ReadDocuments()
        {
            using (var s = File.OpenRead(@"datagen.txt"))
            {
                var r = new StreamReader(s);

                while(!r.EndOfStream)
                {
                    var line = r.ReadLine();

                    if (!string.IsNullOrEmpty(line))
                    {
                        var row = line.Split(',');

                        yield return new BsonDocument
                        {
                            ["_id"] = Convert.ToInt32(row[0]),
                            ["name"] = row[1],
                            ["age"] = Convert.ToInt32(row[2])
                        };
                    }
                }
            }
        }
    }
}