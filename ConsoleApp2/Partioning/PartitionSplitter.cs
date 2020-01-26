using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp2.Partioning
{
    public class PartitionSplitter
    {
        private Dictionary<string, Part> _data = new Dictionary<string, Part>();

        public List<PartitionFileInfo> Split(string inputFileName, string tempFolder, int splitCount)
        {
            using (var stream = new StreamReader(inputFileName, Encoding.UTF8))
            {
                string line = null;
                while ((line = stream.ReadLine()) != null)
                {
                    var rows = line.Split(". ", 2);
                    string key = rows[1].Substring(0, splitCount);

                    if (!_data.ContainsKey(key))
                    {
                        var blockingCollection = new BlockingCollection<string>();
                        string fileName = Path.Combine(tempFolder, Guid.NewGuid().ToString());
                        var task = Task.Factory.StartNew(() =>
                        {
                            using (var stream = new StreamWriter(fileName))
                            {
                                foreach (var item in blockingCollection.GetConsumingEnumerable())
                                {
                                    stream.WriteLine(item);
                                }
                            }
                        });

                        _data.Add(key, new Part
                        {
                            FileName = fileName,
                            Queue = blockingCollection,
                            Task = task
                        });
                    }

                    _data[key].Queue.Add(line);
                }
            }

            foreach (var item in _data.Values)
            {
                item.Queue.CompleteAdding();
            }

            var tasks = _data.Values.Select(v => v.Task).ToArray();
            Task.WaitAll(tasks);

            return _data.Select(i => new PartitionFileInfo
            {
                PartitionKey = i.Key,
                FilePath = i.Value.FileName
            }).ToList();
        }
    }

    class Part
    {
        public Task Task { get; set; }
        public BlockingCollection<string> Queue { get; set; }
        public string FileName { get; internal set; }
    }
}
