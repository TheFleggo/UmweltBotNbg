using System;
using System.Collections.Generic;
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
        private static DateTime cdtmFileTime = DateTime.Now.Subtract(new TimeSpan(1, 0, 0));

        static void Main(string[] args)
        {
            logger.Info("Application is working");
            Console.WriteLine("Start");
            FeedTheBot();
            Console.WriteLine("Ende");
            logger.Info("Application finished");
            if (DateTime.Now.Hour == 8)
                TweetHeartBeat();
            System.Threading.Thread.Sleep(1000);
        }

        private static void FeedTheBot()
        {
            string istrBaseDirectory = "C:\\Umweltdaten\\Flughafen\\" + cdtmFileTime.Year.ToString() + "\\" + cdtmFileTime.Month.ToString().PadLeft(2, '0') + "\\" + cdtmFileTime.Day.ToString().PadLeft(2, '0') + "\\" + cdtmFileTime.Hour.ToString().PadLeft(2, '0') + "\\";
            System.IO.Directory.CreateDirectory(istrBaseDirectory);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/staubpartikel-pm10/Tages-Ansicht/export.csv", istrBaseDirectory + "Feinstaub_PM10.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Feinstaub_PM10.csv", 50, "Feinstaub", "Flughafen");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/staubpartikel-pm25/Tages-Ansicht/export.csv", istrBaseDirectory + "Feinstaub_PM25.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Feinstaub_PM25.csv", 50, "Feinstaub", "Flughafen");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/stickstoffmonoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "StickstoffMonoxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "StickstoffMonoxid.csv", 200, "StickstoffMonoxid", "Flughafen");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/stickstoffdinoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "StickstoffDioxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "StickstoffDioxid.csv", 200, "StickstoffDioxid", "Flughafen");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/kohlenstoffmonoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "Kohlenstoffmonoxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Kohlenstoffmonoxid.csv", 10, "KohlenstoffMonoxid", "Flughafen");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/ozon/Tages-Ansicht/export.csv", istrBaseDirectory + "Ozon.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Ozon.csv", 120, "Ozon", "Flughafen");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-am-flugfeld/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/benzol/Tages-Ansicht/export.csv", istrBaseDirectory + "Benzol.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Benzol.csv", 5, "Benzol", "Flughafen");

            istrBaseDirectory = "C:\\Umweltdaten\\Jakobsplatz\\" + cdtmFileTime.Year.ToString() + "\\" + cdtmFileTime.Month.ToString().PadLeft(2, '0') + "\\" + cdtmFileTime.Day.ToString().PadLeft(2, '0') + "\\" + cdtmFileTime.Hour.ToString().PadLeft(2, '0') + "\\";
            System.IO.Directory.CreateDirectory(istrBaseDirectory);
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/staubpartikel-pm10/Tages-Ansicht/export.csv", istrBaseDirectory + "Feinstaub_PM10.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Feinstaub_PM10.csv", 50, "Feinstaub", "Jakobsplatz");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/staubpartikel-pm25/Tages-Ansicht/export.csv", istrBaseDirectory + "Feinstaub_PM25.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Feinstaub_PM25.csv", 50, "Feinstaub", "Jakobsplatz");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/stickstoffmonoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "StickstoffMonoxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "StickstoffMonoxid.csv", 200, "StickstoffMonoxid", "Jakobsplatz");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/stickstoffdinoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "StickstoffDioxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "StickstoffDioxid.csv", 200, "StickstoffDioxid", "Jakobsplatz");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/kohlenstoffmonoxid/Tages-Ansicht/export.csv", istrBaseDirectory + "Kohlenstoffmonoxid.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Kohlenstoffmonoxid.csv", 10, "Kohlenstoffmonoxid", "Jakobsplatz");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/ozon/Tages-Ansicht/export.csv", istrBaseDirectory + "Ozon.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Ozon.csv", 120, "Ozon", "Jakobsplatz");
            DownloadFile("http://umweltdaten.nuernberg.de/csv/aussenluft/stadt-nuernberg/messstation-jakobsplatz/feinstaub-pm10/csv-export/SUN/nuernberg-flugfeld/benzol/Tages-Ansicht/export.csv", istrBaseDirectory + "Benzol.csv");
            AnalyzeFileExceedsError(istrBaseDirectory + "Benzol.csv", 5, "Benzol", "Jakobsplatz");
        }

        private static bool AnalyzeFileExceedsError(string strFullFileName, double dblGrenzwert, string strMesswert,  string strMessstation)
        {
            bool ibolShoutOut = false;      
            System.IO.StreamReader reader = new System.IO.StreamReader(strFullFileName);
            while(!reader.EndOfStream)
            {
                string strReadLine = reader.ReadLine();
                if (strReadLine.StartsWith(cdtmFileTime.Day.ToString().PadLeft(2, '0') + "." + cdtmFileTime.Month.ToString().PadLeft(2, '0') + "." + cdtmFileTime.Year.ToString() + " " + cdtmFileTime.Hour.ToString().PadLeft(2, '0') + ":00"))
                {
                    double idblGemessenerWert = -1;
                    try
                    {
                        string istrMessText = strReadLine.Split(';')[1];
                        if (double.TryParse(istrMessText, out idblGemessenerWert))
                        {
                            Convert.ToDouble(istrMessText);
                            if (idblGemessenerWert > dblGrenzwert)
                            {
                                logger.Info("Grenzwert überschritten! " + strMesswert + ": " + idblGemessenerWert.ToString() + " statt erlaubten " + dblGrenzwert.ToString());
                                ibolShoutOut = true;
                                HandleGrenzeUeberschritten(idblGemessenerWert, dblGrenzwert, strMesswert, strMessstation);
                            }
                        }
                        else
                            logger.Info("Fehler! " + strFullFileName + Environment.NewLine + "Letzter Messwert: " + istrMessText);

                    }
                    catch(Exception e)
                    {
                        logger.Info("Fehler! " + strFullFileName + Environment.NewLine + e.Message);
                        Console.WriteLine("Fehler! " + strFullFileName);
                    }
                }
            }
            return ibolShoutOut;
        }

        private static void HandleGrenzeUeberschritten(double dblGemessenerWert, double dblGrenzwert, string strMesswert, string strMessstation)
        {
            var strTweet = strMessstation + " " + strMesswert + "-Grenzwert überschritten! Messwert: " + strMesswert + "(Grenze: " + dblGrenzwert.ToString() + ") #Umweltdaten";
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
