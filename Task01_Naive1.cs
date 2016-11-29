using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Linq;

namespace HomeworkTask01
{
    /// <summary>
    /// Naive solution
    /// </summary>
    public class Task01_Naive1
    {
        private static DateTime epochDate = new DateTime(1970, 1, 1);

        public static string ParseLine(string line)
        {
            if (string.IsNullOrWhiteSpace(line)) return "";
	    StringBuilder sb = new StringBuilder();
            string tag48 = null;
            string tag55 = null;
            string tag779 = null;
            string tag1148 = null;
            string tag1149 = null;
            string tag1150 = null;
	    bool read48 = false;
	    bool read55 = false;
	    bool read779 = false;
	    bool read1148 = false;
	    bool read1149 = false;
	    bool read1150 = false;
            string[] fields = line.Split((char)0x01);
            foreach (string field in fields)
            {
                if (string.IsNullOrWhiteSpace(field)) continue;
                string[] tv = field.Split('=');
                if (tv.Length != 2) continue;
                string tagString = tv[0];
                string valueString = tv[1];
                switch (tagString)
                {
                    case "48": tag48 = valueString; read48 = true; break;
                    case "55": tag55 = valueString; read55 = true; break;
                    case "779": tag779 = valueString; read779 = true; break;
                    case "1148": tag1148 = valueString; read1148 = true; break;
                    case "1149": tag1149 = valueString; read1149 = true; break;
                    case "1150": tag1150 = valueString; read1150 = true; break;
                }
		if (read48 & read55 & read779 & read1148 & read1149 & read1150) break;
            }

	    // calculate the updatetime
	    int year = int.Parse(tag779.Substring(0, 4));
            int month = int.Parse(tag779.Substring(4, 2));
            int date = int.Parse(tag779.Substring(6, 2));
            int hour = int.Parse(tag779.Substring(8, 2));
            int minute = int.Parse(tag779.Substring(10, 2));
            int second = int.Parse(tag779.Substring(12, 2));
            int us = int.Parse(tag779.Substring(14, 6));
            DateTime dt = new DateTime(year, month, date, hour, minute, second);
            string LastUpdateTime = (dt - epochDate).TotalSeconds + us.ToString("000000");

	    // calculate the limitrangestring
	    string LimitPriceRange = "NULL";
	    if (tag1148 != null & tag1149 != null)
	    {
		if (tag1148[tag1148.Length - 8] != '.') throw new Exception("Bug");
            	if (tag1149[tag1149.Length - 8] != '.') throw new Exception("Bug");
            	tag1148 = tag1148.Replace(".", "");
            	tag1149 = tag1149.Replace(".", "");
            	long lLowPrice = long.Parse(tag1148);
            	long lHighPrice = long.Parse(tag1149);
            	long lRange = lHighPrice - lLowPrice;
            	string sRange = lRange.ToString("00000000");
            	LimitPriceRange = sRange.Substring(0, sRange.Length - 7) + "." + sRange.Substring(sRange.Length - 7, 7);
	    }

            sb.Append($"{tag48}:{tag55}\n");
            sb.Append($"\tLastUpdateTime={LastUpdateTime}\n");
            sb.Append($"\tLowLimitPrice={tag1148 ?? "NULL"}\n");
            sb.Append($"\tHighLimitPrice={tag1149 ?? "NULL"}\n");
            sb.Append($"\tLimitPriceRange={LimitPriceRange}\n");
            sb.Append($"\tTradingReferencePrice={tag1150 ?? "NULL"}\n");
	    return sb.ToString();
        }

        public static void Parse(string inputPath, string outputPath)
        {
            //StringBuilder sb = new StringBuilder();
	    var sbs = new ConcurrentBag<string>();	  
            string rawInput = File.ReadAllText(inputPath);
            Parallel.ForEach (rawInput.Split('\n'), line =>
            {
                sbs.Add(ParseLine(line));
            });
	    
            File.WriteAllLines(outputPath, sbs.ToList());
        }

        static void Main(string[] args)
        {
            string inputPath;
            string outputPath;
            if (args.Length == 0)
            {
                inputPath = "secdef.dat";
                outputPath = "secdef_parsed.txt";
            }
            else
            {
                inputPath = args[0];
                outputPath = args[1];
            }

            Stopwatch stopwatch = Stopwatch.StartNew();
            Parse(inputPath, outputPath);
            stopwatch.Stop();
            Console.WriteLine($"Code executed in {stopwatch.ElapsedMilliseconds} ms");
        }
    }
}
