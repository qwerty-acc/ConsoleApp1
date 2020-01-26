using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsoleApp2.Partioning
{
    public class TaskManager
    {
        Queue<Task> _tasks = new Queue<Task>();

        public void Add(Task task)
        {
            _tasks.Enqueue(task);
        }
            
        public void RunNext()
        {
            if (_tasks.Count() == 0) 
            {
                return;
            }

            var current = _tasks.Dequeue();
            current.Start();
        }

        public Task[] GetAll()
        {
            return _tasks.ToArray();
        }
    }
}
