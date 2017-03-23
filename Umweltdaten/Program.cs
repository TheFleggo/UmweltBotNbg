using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Umweltdaten
{
    class Program
    {
        private static readonly log4net.ILog logger =
           log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static DateTime cdtmFileTime = DateTime.Now.Subtract(new TimeSpan(0, 1, DateTime.Now.Minute, DateTime.Now.Second, DateTime.Now.Millisecond));
        private static string cstrFlughafenBaseFolder = "C:\\Umweltdaten\\Flughafen\\";
        private static string cstrJakobsplatzBaseFolder = "C:\\Umweltdaten\\Jakobsplatz\\";
        private static string cstrStandortFlughafen = "Flughafen";
        private static string cstrStandortJakobsplatz = "Jakobsplatz";
        private static string cstrMesswertFeinstaub10 = "Feinstaub-10";
        private static string cstrMesswertFeinstaub25 = "Feinstaub-25";
        private static string cstrMesswertStickstoffMonoxid = "StickstoffMonoxid";
        private static string cstrMesswertStickstoffDioxid = "StickstoffDioxid";
        private static string cstrMesswertKohlenstoffMonoxid = "KohlenstoffMonoxid";
        private static string cstrMesswertOzon = "Ozon";
        private static string cstrMesswertBenzol = "Benzol";

        private static ObservableCollection<Standort> AlleStandorte = new ObservableCollection<Standort>();
        private static ObservableCollection<Messwert> AlleMesswerte = new ObservableCollection<Messwert>();

        static void Main(string[] args)
        {
            if (clsLocalDB.InitLeitstandMongoDB())
            {
                InitializeStandorte();
                InitializeMesswerte();
            }
            if (args.Any(x => x.Contains("FillDB")))
            {
                logger.Info("Sonderstart: Komplettimport");
                MesswerteZusammenfassen(cstrStandortFlughafen, cstrMesswertFeinstaub10, cstrFlughafenBaseFolder, "Feinstaub_PM10.csv");
                logger.Info("Feinstaub_PM10: Ok");
                MesswerteZusammenfassen(cstrStandortFlughafen, cstrMesswertFeinstaub25, cstrFlughafenBaseFolder, "Feinstaub_PM25.csv");
                logger.Info("Feinstaub_PM25: Ok");
                MesswerteZusammenfassen(cstrStandortFlughafen, cstrMesswertStickstoffMonoxid, cstrFlughafenBaseFolder, "StickstoffMonoxid.csv");
                logger.Info("StickstoffMonoxid: Ok");
                MesswerteZusammenfassen(cstrStandortFlughafen, cstrMesswertStickstoffDioxid, cstrFlughafenBaseFolder, "StickstoffDioxid.csv");
                logger.Info("StickstoffDioxid: Ok");
                MesswerteZusammenfassen(cstrStandortFlughafen, cstrMesswertKohlenstoffMonoxid, cstrFlughafenBaseFolder, "Kohlenstoffmonoxid.csv");
                logger.Info("Kohlenstoffmonoxid: Ok");
                MesswerteZusammenfassen(cstrStandortFlughafen, cstrMesswertOzon, cstrFlughafenBaseFolder, "Ozon.csv");
                logger.Info("Ozon: Ok");
                MesswerteZusammenfassen(cstrStandortFlughafen, cstrMesswertBenzol, cstrFlughafenBaseFolder, "Benzol.csv");
                logger.Info("Benzol: Ok");
            }
            else if (args.Any(x => x.Contains("SummarizeAll")))
            {
                DateTime iVon = new DateTime(2017, 1, 1);
                DateTime iBis = new DateTime(2017, 1, 2);
                while(iBis<=DateTime.Today)
                {
                    clsLocalDB.SummarizeValues(AlleStandorte, AlleMesswerte, iVon, iBis);
                    iVon = iVon.AddDays(1);
                    iBis = iBis.AddDays(1);
                }
                iVon = new DateTime(2017, 1, 1);
                iBis = new DateTime(2017, 2, 1);
                while (iBis <= DateTime.Today)
                {
                    clsLocalDB.SummarizeValues(AlleStandorte, AlleMesswerte, iVon, iBis);
                    iVon = iVon.AddMonths(1);
                    iBis = iBis.AddMonths(1);
                }
            }
            else
            {
                logger.Info("Application is working");
                Console.WriteLine("Start");
                FeedTheBot();
                Console.WriteLine("Ende");
                logger.Info("Application finished");
                if (DateTime.Now.Hour == 8)
                {
                    DateTime iVon = DateTime.Today.Subtract(new TimeSpan(1, 0, 0, 0));
                    DateTime iBis = DateTime.Today;
                    clsLocalDB.SummarizeValues(AlleStandorte, AlleMesswerte, iVon, iBis);
                    if (DateTime.Now.Day == 1)
                    {
                        iVon = (DateTime.Today.Month > 1) ? new DateTime(DateTime.Today.Year, DateTime.Today.Month - 1, 1) : new DateTime(DateTime.Today.Year - 1, 12, 1);
                        iBis = new DateTime(DateTime.Today.Year, DateTime.Today.Month, 1);
                        clsLocalDB.SummarizeValues(AlleStandorte, AlleMesswerte, iVon, iBis);
                        if (DateTime.Now.Month == 1)
                        {
                            iVon = new DateTime(DateTime.Today.Year - 1, 1, 1);
                            iBis = new DateTime(DateTime.Today.Year, 1, 1);
                            clsLocalDB.SummarizeValues(AlleStandorte, AlleMesswerte, iVon, iBis);
                        }
                    }
                    TweetHeartBeat();
                }
                System.Threading.Thread.Sleep(1000);
            }
        }

        private static void MesswerteZusammenfassen(string vStandort, string vMesswert, string vBaseFolder, string vFileName)
        {
            System.IO.DirectoryInfo iResultBaseDir = new System.IO.DirectoryInfo(vBaseFolder);

            foreach (System.IO.FileInfo iResultFile in iResultBaseDir.GetFiles(vFileName, System.IO.SearchOption.AllDirectories))
            {
                using (System.IO.StreamReader iReader = new System.IO.StreamReader(iResultFile.FullName))
                {
                    while (!iReader.EndOfStream)
                    {
                        string iReadLine = iReader.ReadLine();
                        if (iReadLine.Contains(';'))
                        {
                            DateTime iTimeStamp;
                            double iGemessenerWert;
                            DateTime.TryParse(iReadLine.Split(';')[0], out iTimeStamp);
                            double.TryParse(iReadLine.Split(';')[1], out iGemessenerWert);
                            if(iTimeStamp > new DateTime(2000,1,1))
                                SpeichereMesswertInDB(vMesswert, vStandort, iGemessenerWert, iTimeStamp);
                        }
                    }
                }
            }
        }

        private static void InitializeMesswerte()
        {
            try
            {
                AlleMesswerte = clsLocalDB.LadeAlleMesswerteAusDatenbank();
                if (!AlleMesswerte.Any(x => x.Bezeichnung == cstrMesswertFeinstaub10))
                    clsLocalDB.SpeichereMesswert(new Umweltdaten.Messwert() { Bezeichnung = cstrMesswertFeinstaub10, Grenze = 200 });
                if (!AlleMesswerte.Any(x => x.Bezeichnung == cstrMesswertFeinstaub25))
                    clsLocalDB.SpeichereMesswert(new Umweltdaten.Messwert() { Bezeichnung = cstrMesswertFeinstaub25, Grenze = 200 });
                if (!AlleMesswerte.Any(x => x.Bezeichnung == cstrMesswertStickstoffMonoxid))
                    clsLocalDB.SpeichereMesswert(new Umweltdaten.Messwert() { Bezeichnung = cstrMesswertStickstoffMonoxid, Grenze = 200 });
                if (!AlleMesswerte.Any(x => x.Bezeichnung == cstrMesswertStickstoffDioxid))
                    clsLocalDB.SpeichereMesswert(new Umweltdaten.Messwert() { Bezeichnung = cstrMesswertStickstoffDioxid, Grenze = 200 });
                if (!AlleMesswerte.Any(x => x.Bezeichnung == cstrMesswertKohlenstoffMonoxid))
                    clsLocalDB.SpeichereMesswert(new Umweltdaten.Messwert() { Bezeichnung = cstrMesswertKohlenstoffMonoxid, Grenze = 10 });
                if (!AlleMesswerte.Any(x => x.Bezeichnung == cstrMesswertOzon))
                    clsLocalDB.SpeichereMesswert(new Umweltdaten.Messwert() { Bezeichnung = cstrMesswertOzon, Grenze = 120 });
                if (!AlleMesswerte.Any(x => x.Bezeichnung == cstrMesswertBenzol))
                    clsLocalDB.SpeichereMesswert(new Umweltdaten.Messwert() { Bezeichnung = cstrMesswertBenzol, Grenze = 5 });
                AlleMesswerte = clsLocalDB.LadeAlleMesswerteAusDatenbank();
            }
            catch(Exception e)
            { }
        }

        private static void InitializeStandorte()
        {
            try
            {
                AlleStandorte = clsLocalDB.LadeAlleStandorteAusDatenbank();
                if (!AlleStandorte.Any(x => x.Name == cstrStandortFlughafen))
                    clsLocalDB.SpeichereStandort(new Umweltdaten.Standort() { Name = cstrStandortFlughafen });
                if (!AlleStandorte.Any(x => x.Name == cstrStandortJakobsplatz))
                    clsLocalDB.SpeichereStandort(new Umweltdaten.Standort() { Name = cstrStandortJakobsplatz });
                AlleStandorte = clsLocalDB.LadeAlleStandorteAusDatenbank();

            }
            catch (Exception e)
            { }
        }

        private static void FeedTheBot()
        {
            string istrBaseDirectory = cstrFlughafenBaseFolder + cdtmFileTime.Year.ToString() + "\\" + cdtmFileTime.Month.ToString().PadLeft(2, '0') + "\\" + cdtmFileTime.Day.ToString().PadLeft(2, '0') + "\\" + cdtmFileTime.Hour.ToString().PadLeft(2, '0') + "\\";
            System.IO.Directory.CreateDirectory(istrBaseDirectory);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/staubpartikel-pm10/Tages-Ansicht/export.csv", istrBaseDirectory + "Feinstaub_PM10.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Feinstaub_PM10.csv", 50, cstrMesswertFeinstaub10, cstrStandortFlughafen);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/staubpartikel-pm25/Tages-Ansicht/export.csv", istrBaseDirectory + "Feinstaub_PM25.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Feinstaub_PM25.csv", 50, cstrMesswertFeinstaub25, cstrStandortFlughafen);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/stickstoffmonoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "StickstoffMonoxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "StickstoffMonoxid.csv", 200, cstrMesswertStickstoffMonoxid, cstrStandortFlughafen);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/stickstoffdinoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "StickstoffDioxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "StickstoffDioxid.csv", 200, cstrMesswertStickstoffDioxid, cstrStandortFlughafen);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/kohlenstoffmonoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "Kohlenstoffmonoxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Kohlenstoffmonoxid.csv", 10, cstrMesswertKohlenstoffMonoxid, cstrStandortFlughafen);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/ozon/Tages-Ansicht/export.csv", istrBaseDirectory + "Ozon.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Ozon.csv", 120, cstrMesswertOzon, cstrStandortFlughafen);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/benzol/Tages-Ansicht/export.csv", istrBaseDirectory + "Benzol.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Benzol.csv", 5, cstrMesswertBenzol, cstrStandortFlughafen);

            istrBaseDirectory = cstrJakobsplatzBaseFolder + cdtmFileTime.Year.ToString() + "\\" + cdtmFileTime.Month.ToString().PadLeft(2, '0') + "\\" + cdtmFileTime.Day.ToString().PadLeft(2, '0') + "\\" + cdtmFileTime.Hour.ToString().PadLeft(2, '0') + "\\";
            System.IO.Directory.CreateDirectory(istrBaseDirectory);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/staubpartikel-pm10/Tages-Ansicht/export.csv", istrBaseDirectory + "Feinstaub_PM10.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Feinstaub_PM10.csv", 50, cstrMesswertFeinstaub10, cstrStandortJakobsplatz);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/staubpartikel-pm25/Tages-Ansicht/export.csv", istrBaseDirectory + "Feinstaub_PM25.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Feinstaub_PM25.csv", 50, cstrMesswertFeinstaub25, cstrStandortJakobsplatz);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/stickstoffmonoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "StickstoffMonoxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "StickstoffMonoxid.csv", 200, cstrMesswertStickstoffMonoxid, cstrStandortJakobsplatz);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/stickstoffdinoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "StickstoffDioxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "StickstoffDioxid.csv", 200, cstrMesswertStickstoffDioxid, cstrStandortJakobsplatz);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/kohlenstoffmonoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "Kohlenstoffmonoxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Kohlenstoffmonoxid.csv", 10, cstrMesswertKohlenstoffMonoxid, cstrStandortJakobsplatz);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/ozon/Tages-Ansicht/export.csv", istrBaseDirectory + "Ozon.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Ozon.csv", 120, cstrMesswertOzon, cstrStandortJakobsplatz);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/benzol/Tages-Ansicht/export.csv", istrBaseDirectory + "Benzol.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Benzol.csv", 5, cstrMesswertBenzol, cstrStandortJakobsplatz);
        }

        private static bool AnalyzeFileExceedsError(string strFullFileName, double dblGrenzwert, string strMesswert,  string strMessstation)
        {
            bool ibolShoutOut = false;
            string iLineTimeStampComparer = cdtmFileTime.ToString("dd.MM.yyyy HH:mm");
            List<string> ilstFullFileList = ReadFullFile(strMesswert, strMessstation);
            System.IO.StreamReader reader = new System.IO.StreamReader(strFullFileName);
            while(!reader.EndOfStream)
            {
                string strReadLine = reader.ReadLine();
                if (strReadLine.StartsWith(iLineTimeStampComparer))
                {
                    double idblGemessenerWert = -1;
                    try
                    {
                        string istrMessText = strReadLine.Split(';')[1];
                        if (double.TryParse(istrMessText, out idblGemessenerWert))
                        {
                            SpeichereMesswertInDB(strMesswert, strMessstation, idblGemessenerWert);

                            if (idblGemessenerWert > dblGrenzwert)
                            {
                                logger.Info("Grenzwert überschritten! " + strMesswert + ": " + idblGemessenerWert.ToString() + " statt erlaubten " + dblGrenzwert.ToString());
                                ibolShoutOut = true;
                                HandleGrenzeUeberschritten(idblGemessenerWert, dblGrenzwert, strMesswert, strMessstation);
                            }
                        }
                        else
                            logger.Info("Fehler! " + strFullFileName + Environment.NewLine + "Letzter Messwert: " + istrMessText);

                        if (!ilstFullFileList.Contains(strReadLine))
                            ilstFullFileList.Add(strReadLine);
                    }
                    catch(Exception e)
                    {
                        logger.Info("Fehler! " + strFullFileName + Environment.NewLine + e.Message);
                        Console.WriteLine("Fehler! " + strFullFileName);
                    }
                }
            }
            WriteFullFile(strMesswert, strMessstation, ilstFullFileList);
            return ibolShoutOut;
        }

        private static void SpeichereMesswertInDB(string strMesswert, string strMessstation, double idblGemessenerWert, DateTime? vTimeStamp = null)
        {
            try
            {
                var iStandort = AlleStandorte.Single(x => x.Name == strMessstation);
                var iMesswert = AlleMesswerte.Single(x => x.Bezeichnung == strMesswert);
                DateTime iTimeStamp = cdtmFileTime;
                if (vTimeStamp != null)
                    iTimeStamp = (DateTime)vTimeStamp;
                var iMessung = clsLocalDB.LadeMessungAusDatenbank(iStandort, iMesswert, iTimeStamp);
                if (iMessung == null)
                    iMessung = new Messung() { Id = Guid.NewGuid(), Zeitpunkt = iTimeStamp, Wert = idblGemessenerWert, Standort = iStandort, Messwert = iMesswert };
                clsLocalDB.SpeichereMessung(iMessung);
            }
            catch(Exception e)
            {
            }
            logger.Info("Wert in DB geschrieben! " + strMessstation + " " + strMesswert + " " + idblGemessenerWert);
        }

        private static void WriteFullFile(string strMesswert, string strMessstation, List<string> lstFile)
        {
            string istrFile = "";
            if (strMessstation == cstrStandortFlughafen)
                istrFile = cstrFlughafenBaseFolder + strMesswert + "_Komplett.txt";
            else if (strMessstation == cstrStandortJakobsplatz)
                istrFile = cstrJakobsplatzBaseFolder + strMesswert + "_Komplett.txt";
            using (System.IO.StreamWriter sw = new System.IO.StreamWriter(istrFile, false))
            {
                foreach (string line in lstFile)
                    sw.WriteLine(line);
                sw.Flush();
            }
        }

        private static List<string> ReadFullFile(string strMesswert, string strMessstation)
        {
            List<string> ilstFullFileList = new List<string>();
            string istrFile = "";
            if (strMessstation == cstrStandortFlughafen)
                istrFile = cstrFlughafenBaseFolder + strMesswert + "_Komplett.txt";
            else if(strMessstation == cstrStandortJakobsplatz)
                istrFile = cstrJakobsplatzBaseFolder + strMesswert + "_Komplett.txt";
            if (System.IO.File.Exists(istrFile))
            {
                using (System.IO.StreamReader sr = new System.IO.StreamReader(istrFile))
                {
                    while (!sr.EndOfStream)
                    {
                        ilstFullFileList.Add(sr.ReadLine());
                    }
                }
            }
            return ilstFullFileList;
        }

        private static void HandleGrenzeUeberschritten(double dblGemessenerWert, double dblGrenzwert, string strMesswert, string strMessstation)
        {
            var strTweet = strMessstation + " " + strMesswert + "-Grenzwert überschritten! Messwert: " + dblGemessenerWert.ToString() + "(Grenze: " + dblGrenzwert.ToString() + ") #Umweltdaten";
            //Consumer key, Consumer Secret
            var service = new TweetSharp.TwitterService(clsConnectionInfos.cstrTwitterconsumerKey, clsConnectionInfos.cstrTwitterConsumerSecret);
            //Access Token, AccessTokenSecret
            service.AuthenticateWith(clsConnectionInfos.cstrTwitterAccessToken, clsConnectionInfos.cstrTwitterAccessTokenSecret);

            var options = new TweetSharp.SendTweetOptions();
            logger.Info("Tweet! " + strTweet);
            options.Status = strTweet;
            service.SendTweet(options);
        }

        private static void TweetHeartBeat()
        {
            //Consumer key, Consumer Secret
            var service = new TweetSharp.TwitterService(clsConnectionInfos.cstrTwitterconsumerKey, clsConnectionInfos.cstrTwitterConsumerSecret);
            //Access Token, AccessTokenSecret
            service.AuthenticateWith(clsConnectionInfos.cstrTwitterAccessToken, clsConnectionInfos.cstrTwitterAccessTokenSecret);

            var options = new TweetSharp.SendTweetOptions();
            options.Status = "#HeartBeat - Up and running!";
            service.SendTweet(options);
        }

        private static void DownloadFile(string strUrl, string strFullFileName)
        {
            HttpWebRequest req = (HttpWebRequest)WebRequest.Create(strUrl);
            HttpWebResponse resp = (HttpWebResponse)req.GetResponse();

            System.IO.StreamReader sr = new System.IO.StreamReader(resp.GetResponseStream());
            string results = sr.ReadToEnd();
            sr.Close();

            System.IO.StreamWriter writer = new System.IO.StreamWriter(strFullFileName);
            writer.Write(results);
            writer.Close();
        }
    }
}
