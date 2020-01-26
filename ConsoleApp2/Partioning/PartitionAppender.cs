using ConsoleApp2.Partioning;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2.Appender
{
    public class PartitionAppender
    {
        private readonly TaskManager _manager = new TaskManager();
        private readonly BlockingCollection<PartSortedData> _dataCollection = new BlockingCollection<PartSortedData>();

        public void Append(string outputFileName, int partitionReadThreadNumber, List<PartitionFileInfo> fileNameInfo)
        {
            using (var resultFile = new StreamWriter(outputFileName, false, Encoding.UTF8, 65536))
            {
                var writeTask = Task.Factory.StartNew(() =>
                {
                    foreach (var data in _dataCollection.GetConsumingEnumerable())
                    {
                        var sw = new Stopwatch();
                        Console.WriteLine($"started append {data.Partition} :: {data.FileName}");
                        sw.Start();

                        WriteToFile(data.Data, resultFile);

                        sw.Stop();
                        Console.WriteLine($"stop append {data.Partition} :: {data.FileName} :: {(sw.ElapsedMilliseconds / 1000.0).ToString()}");
                    }
                });

                foreach (var fileNameInfoItem in fileNameInfo.OrderBy(i => i.PartitionKey))
                {
                    var previousTask = _manager.GetAll().LastOrDefault();
                    var item = CreatePartitionReadTask(previousTask, fileNameInfoItem);
                    _manager.Add(item);
                    previousTask = item;
                }

                var readTasks = _manager.GetAll();

                for (var i = 0; i < partitionReadThreadNumber; i++)
                {
                    _manager.RunNext();
                }

                Task.WaitAll(readTasks);
                _dataCollection.CompleteAdding();
                Task.WaitAll(writeTask);
            }
        }

        private Task CreatePartitionReadTask(Task previousTask, PartitionFileInfo fileNameInfoItem)
        {
            return new Task(() =>
            {
                var sw = new Stopwatch();
                Console.WriteLine($"started read {fileNameInfoItem.PartitionKey} :: {fileNameInfoItem.FilePath}");
                sw.Start();

                var sortedPart = ReadAndSort(fileNameInfoItem.FilePath);

                sw.Stop();
                Console.WriteLine($"stop read {fileNameInfoItem.PartitionKey} :: {fileNameInfoItem.FilePath} :: {(sw.ElapsedMilliseconds / 1000.0).ToString()}");

                if (previousTask != null)
                {
                    Task.WaitAll(previousTask);
                }

                _dataCollection.Add(new PartSortedData
                {
                    FileName = fileNameInfoItem.FilePath,
                    Partition = fileNameInfoItem.PartitionKey,
                    Data = sortedPart
                });

                _manager.RunNext();
            });
        }

        private SortedDictionary<string, SortedDictionary<int, LineCount>> ReadAndSort(string fileName)
        {
            var items = new SortedDictionary<string, SortedDictionary<int, LineCount>>();

            using (var stream = new StreamReader(fileName))
            {
                string line;
                while ((line = stream.ReadLine()) != null)
                {
                    var elements = line.Split(". ", 2);

                    if (!items.ContainsKey(elements[1]))
                    {
                        items.Add(elements[1], new SortedDictionary<int, LineCount>());
                    }
                    var item = items[elements[1]];

                    var ind = int.Parse(elements[0]);

                    if (!item.ContainsKey(ind))
                    {
                        item.Add(ind, new LineCount
                        {
                            Line = line,
                            Count = 1
                        });
                    }
                    else
                    {
                        item[ind].Count++;
                    }
                }
            }

            return items;
        }

        private void WriteToFile(SortedDictionary<string, SortedDictionary<int, LineCount>> sortedData, StreamWriter outputstream)
        {
            using (var enumerator = sortedData.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    using (var enumerator2 = enumerator.Current.Value.GetEnumerator())
                    {
                        while (enumerator2.MoveNext())
                        {
                            for (var i = 0; i < enumerator2.Current.Value.Count; i++)
                            {
                                outputstream.WriteLine(enumerator2.Current.Value.Line);
                            }
                        }
                    }
                }
            }
        }
    }

    class PartSortedData
    {
        public string FileName { get; set; }
        public string Partition { get; set; }
        public SortedDictionary<string, SortedDictionary<int, LineCount>> Data { get; set; }
    }

    class LineCount
    {
        public string Line { get; set; }
        public int Count { get; set; }
    }
}
