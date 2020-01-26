using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json", true, true)
              .Build();

            var filename = config.GetSection("OutputFileName").Value;
            var filesize = config.GetSection("OutputFileSizeBytes").Get<long>();
            var phrases = config.GetSection("Dictionary").Get<string[]>();

            Console.WriteLine("started");
            var sw = new Stopwatch();

            sw.Start();
            WriteToOneFile(filename, filesize, phrases);
            sw.Stop();

            Console.WriteLine($"stoped {(sw.ElapsedMilliseconds / 1000.0).ToString()}");
        }

        private static void WriteToOneFile(string ilename, long filesize, string[] dictionary)
        {
            var rand = new Random();

            using (var file = new StreamWriter(ilename, false, Encoding.UTF8, 65536))
            {
                while (filesize > 0)
                {
                    var number = rand.Next(15000);
                    var phraseNumber = rand.Next(dictionary.Length);
                    var value = $"{number}. {dictionary[phraseNumber]}";
                    filesize -= (value.Length + 2);
                    file.WriteLine(value);
                }
            }
        }
    }
}
