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


        internal static void SummarizeCustom(ObservableCollection<Standort> alleStandorte, ObservableCollection<Messwert> alleMesswerte, DateTime vVon, DateTime vBis)
        {
            foreach (Standort iStandort in alleStandorte)
            {
                foreach (Messwert iMesswert in alleMesswerte)
                {
                    var iMessungen = GetMessungen(iStandort, iMesswert, Zeitspanne.Custom, vVon, vBis);
                }
            }
        }

        internal static void SummarizeLastYear(ObservableCollection<Standort> alleStandorte, ObservableCollection<Messwert> alleMesswerte)
        {
            foreach (Standort iStandort in alleStandorte)
            {
                foreach (Messwert iMesswert in alleMesswerte)
                {
                    var iMessungen = GetMessungen(iStandort, iMesswert, Zeitspanne.LetztesJahr);
                }
            }
        }

        internal static void SummarizeLastMonth(ObservableCollection<Standort> alleStandorte, ObservableCollection<Messwert> alleMesswerte)
        {
            foreach (Standort iStandort in alleStandorte)
            {
                foreach (Messwert iMesswert in alleMesswerte)
                {
                    var iMessungen = GetMessungen(iStandort, iMesswert, Zeitspanne.LetzterMonat);
                }
            }
        }

        internal static void SummarizeYesterday(ObservableCollection<Standort> alleStandorte, ObservableCollection<Messwert> alleMesswerte)
        {
            foreach(Standort iStandort in alleStandorte)
            {
                foreach(Messwert iMesswert in alleMesswerte)
                {
                    var iMessungen = GetMessungen(iStandort, iMesswert, Zeitspanne.LetzterTag);
                }
            }
        }

        private static List<Messung> GetMessungen(Standort iStandort, Messwert iMesswert, Zeitspanne vZeitspanne, DateTime? vVon = null, DateTime? vBis = null)
        {
            switch (vZeitspanne)
            {
                case Zeitspanne.LetzterTag:
                    vVon = DateTime.Today.Subtract(new TimeSpan(1, 0, 0, 0));
                    vBis = DateTime.Today;
                    break;
                case Zeitspanne.LetzterMonat:
                    vVon = (DateTime.Today.Month > 1) ? new DateTime(DateTime.Today.Year, DateTime.Today.Month - 1, 1) : new DateTime(DateTime.Today.Year - 1, 12, 1);
                    vBis = new DateTime(DateTime.Today.Year, DateTime.Today.Month, 1);
                    break;

                case Zeitspanne.LetztesJahr:
                    vVon = new DateTime(DateTime.Today.Year - 1, 1, 1);
                    vBis = new DateTime(DateTime.Today.Year, 1, 1);
                    break;
            }

            var builder = new QueryBuilder<Messung>();
            var query = Query.And(
                builder.EQ(x => x.Standort.Name, iStandort.Name),
                builder.EQ(x => x.Messwert.Bezeichnung, iMesswert.Bezeichnung),
                builder.GTE(x => x.Zeitpunkt, vVon),
                builder.LT(x => x.Zeitpunkt, vBis));

            return MongoDB.GetCollection<Messung>("Messung").Find(query).ToList();
        }
    }

    enum Zeitspanne
    {
        LetzterTag,
        LetzterMonat,
        LetztesJahr,
        Custom
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

    }
}