using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Umweltdaten
{
    internal static class clsLocalDB
    {

        private static MongoClient MongoDB_Client;
        private static MongoServer MongoDB_Server;
        private static MongoDatabase MongoDB;


        public static bool InitLeitstandMongoDB()
        {
            bool connected = false;
            try
            {
                MongoDB_Client = new MongoClient();
                MongoDB_Server = MongoDB_Client.GetServer();
                MongoDB = MongoDB_Server.GetDatabase("UmweltdatenbotNbg");
                connected = true;
            }
            catch (Exception e)
            {
                connected = false;
            }
            return connected;
        }

        public static ObservableCollection<Standort> LadeAlleStandorteAusDatenbank()
        {
            ObservableCollection<Standort> standorte = new ObservableCollection<Standort>();
            var dbStandorte = MongoDB.GetCollection<Standort>("Standort");
            foreach (Standort standortInDB in dbStandorte.FindAll())
            {
                standorte.Add(standortInDB);
            }
            return standorte;
        }
        
        public static Standort LadeStandortAusDatenbank(string name)
        {
            var builder = new QueryBuilder<Standort>();
            var query = builder.EQ(x => x.Name, name);
            return MongoDB.GetCollection<Standort>("Standort").Find(query).SingleOrDefault();
        }

        internal static Messung LadeMessungAusDatenbank(Standort vStandort, Messwert vMesswert, DateTime vFileTime)
        {
            var builder = new QueryBuilder<Messung>();
            var query = Query.And(                builder.EQ(x => x.Standort.Name, vStandort.Name),                builder.EQ(x => x.Messwert.Bezeichnung, vMesswert.Bezeichnung),                builder.EQ(x => x.Zeitpunkt, vFileTime));
            return MongoDB.GetCollection<Messung>("Messung").Find(query).SingleOrDefault();
        }

        public static bool SpeichereStandort(Standort standort)
        {
            bool rslt = false;

            var collection = MongoDB.GetCollection<Standort>("Standort");
            collection.Save(standort);
            
            return rslt;
        }

        internal static CalculatedResult LadeCalculatedResultAusDatenbank(Standort vStandort, Messwert vMesswert, int vYear, int vMonth = 0, int vDay = 0)
        {
            var builder = new QueryBuilder<CalculatedResult>();
            var query = Query.And(                builder.EQ(x => x.Standort.Name, vStandort.Name),                builder.EQ(x => x.Messwert.Bezeichnung, vMesswert.Bezeichnung),                builder.EQ(x => x.Year, vYear),                builder.EQ(x => x.Month, vMonth),                builder.EQ(x => x.Day, vDay));
            return MongoDB.GetCollection<CalculatedResult>("CalculatedResult").Find(query).SingleOrDefault();
        }

        public static bool SpeichereCalculatedResult(CalculatedResult vCalculatedResult)
        {
            bool rslt = false;

            var collection = MongoDB.GetCollection<CalculatedResult>("CalculatedResult");
            collection.Save(vCalculatedResult);

            return rslt;
        }

        public static ObservableCollection<Messwert> LadeAlleMesswerteAusDatenbank()
        {
            ObservableCollection<Messwert> messwerte = new ObservableCollection<Messwert>();
            var dbMesswerte = MongoDB.GetCollection<Messwert>("Messwert");
            foreach (Messwert messwertInDB in dbMesswerte.FindAll())
            {
                messwerte.Add(messwertInDB);
            }
            return messwerte;
        }

        public static bool SpeichereMesswert(Messwert messwert)
        {
            bool rslt = false;

            var collection = MongoDB.GetCollection<Messwert>("Messwert");
            collection.Save(messwert);

            return rslt;
        }

        public static bool SpeichereMessung(Messung messung)
        {
            bool rslt = false;

            var collection = MongoDB.GetCollection<Messung>("Messung");
            collection.Save(messung);
            
            return rslt;
        }


        internal static void SummarizeValues(ObservableCollection<Standort> alleStandorte, ObservableCollection<Messwert> alleMesswerte, DateTime vVon, DateTime vBis)
        {
            foreach (Standort iStandort in alleStandorte)
            {
                foreach (Messwert iMesswert in alleMesswerte)
                {
                    var iMessungen = GetMessungen(iStandort, iMesswert, vVon, vBis);
                    if(iMessungen.Count > 0)
                        ProduceCustomResults(vVon, vBis, iStandort, iMesswert, iMessungen);
                }
            }
        }
        
        
        private static List<Messung> GetMessungen(Standort vStandort, Messwert vMesswert, DateTime vVon, DateTime vBis)
        {                  
            var builder = new QueryBuilder<Messung>();
            var query = Query.And(
                builder.EQ(x => x.Standort.Name, vStandort.Name),
                builder.EQ(x => x.Messwert.Bezeichnung, vMesswert.Bezeichnung),
                builder.GTE(x => x.Zeitpunkt, vVon),
                builder.LT(x => x.Zeitpunkt, vBis));

            return MongoDB.GetCollection<Messung>("Messung").Find(query).ToList();
        }

        private static void ProduceCustomResults(DateTime vVon, DateTime vBis, Standort vStandort, Messwert vMesswert, List<Messung> vMessungen)
        {
            TimeSpan iDuration = vBis - vVon;
            CalculatedResult iCalculatedResult = new CalculatedResult();
            iCalculatedResult.AnzahlMessungen = vMessungen.Count;
            iCalculatedResult.AnzahlGemesseneTage = vMessungen.GroupBy(x => x.Zeitpunkt.Date).ToList().Count;
            iCalculatedResult.AnzahlUeberschreitungen = vMessungen.Where(x => x.Wert >= vMesswert.Grenze).Count();
            iCalculatedResult.AnzahlUeberschritteneTage = vMessungen.Where(x => x.Wert >= vMesswert.Grenze).GroupBy(x => x.Zeitpunkt.Date).Count();
            iCalculatedResult.Maximalwert = vMessungen.FirstOrDefault(x => x.Wert == vMessungen.Max(y => y.Wert));
            iCalculatedResult.Minimalwert = vMessungen.FirstOrDefault(x => x.Wert == vMessungen.Min(y => y.Wert));
            iCalculatedResult.Messwert = vMesswert;
            iCalculatedResult.Standort = vStandort;
            if (iDuration.TotalHours <= 24)
                iCalculatedResult.Day = vVon.Day;
            if (iDuration.TotalDays < 32)
                iCalculatedResult.Month = vVon.Month;
            iCalculatedResult.Year = vVon.Year;
            SpeichereCalculatedResult(iCalculatedResult);
        }

    }
    
    public class Standort
    {
        [BsonId]
        public string Name { get; set; }
    }

    public class Messwert
    {
        [BsonId]
        public string Bezeichnung { get; set; }
        public double Grenze { get; set; }
    }

    public class Messung
    {
        [BsonId]
        public Guid Id { get; set; }
        public Messwert Messwert { get; set; }
        public Standort Standort { get; set; }
        public double Wert { get; set; }
        public DateTime Zeitpunkt { get; set; }
    }

    public class CalculatedResult
    {
        [BsonId]
        public Guid Id { get; set; }
        public Messwert Messwert { get; set; }
        public Standort Standort { get; set; }
        public Messung Maximalwert { get; set; }
        public Messung Minimalwert { get; set; }

        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public int AnzahlMessungen { get; set; }
        public int AnzahlUeberschreitungen { get; set; }
        public int AnzahlGemesseneTage { get; set; }
        public int AnzahlUeberschritteneTage { get; set; }

    }
}