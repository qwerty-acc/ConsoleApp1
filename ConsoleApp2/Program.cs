using ConsoleApp2.Appender;
using ConsoleApp2.Partioning;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace ConsoleApp2
{
    partial class Program
    {
        static void Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            var inputFileName = config.GetSection("InputFileName").Value;
            var outputFileName = config.GetSection("OutputFileName").Value;
            var tempFolder = config.GetSection("TempFolder").Value;
            var partitionReadThreadNumber = config.GetSection("PartitionReadThreadNumber").Get<int>();
            var partitionMaxSize = config.GetSection("PartitionMaxSize").Get<long>();
            var initialPartitioningDepth = config.GetSection("InitialPartitioningDepth").Get<int>();

            if (!Directory.Exists(tempFolder)){
                Directory.CreateDirectory(tempFolder);
            }

            var sw = new Stopwatch();

            Console.WriteLine("started");
            sw.Start();

            Console.WriteLine("start partitioning");


            var splitter = new PartitionSplitter();

            var partitionsInfo = Partition(partitionMaxSize, inputFileName, tempFolder, initialPartitioningDepth);

            Console.WriteLine($"stop partitioning {(sw.ElapsedMilliseconds / 1000.0).ToString()}");

            var appender = new PartitionAppender();
            appender.Append(outputFileName, partitionReadThreadNumber, partitionsInfo);

            sw.Stop();
            Console.WriteLine($"stoped {(sw.ElapsedMilliseconds / 1000.0).ToString()}");
        }

        private static List<PartitionFileInfo> Partition(long partitionMaxSize, string filePath, string tempFolder, int initialPartitioningDepth)
        {
            var result = new List<PartitionFileInfo>();

            var partsInfoToProcess = new List<PartitionFileInfo>(new[]
            { 
                new PartitionFileInfo
                {
                    FilePath = filePath,
                    PartitionKey = "__UNPART__"
                }
            });

            var depth = initialPartitioningDepth;
            while (partsInfoToProcess.Any())
            {
                var bigPartsInfo = new List<PartitionFileInfo>();

                foreach (var partInfo in partsInfoToProcess)
                {
                    if (new FileInfo(partInfo.FilePath).Length > partitionMaxSize)
                    {
                        var results = new PartitionSplitter().Split(partInfo.FilePath, tempFolder, depth);
                        bigPartsInfo.AddRange(results);
                        continue;
                    }

                    result.Add(partInfo);
                }
                depth++;

                partsInfoToProcess = bigPartsInfo;
            }

            return result;
        }
    }
}
