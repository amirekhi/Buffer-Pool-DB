using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using BufferPool.PageDir;
using BufferPool.BF;
using BufferPool.DiskManagerDir;

namespace BufferPoolDemo
{
    class Program
    {
        static void Main()
        {
            const int pageSize = 4096;
            var dm = new DiskManager(pageSize);
            var bpm = new BufferPoolManager(poolSize: 3, pageSize: pageSize, diskManager: dm);

            // Create new page 0
            var page0 = bpm.NewPage(out int pid0);
            Console.WriteLine($"Allocated new page {pid0}");
            var text = Encoding.UTF8.GetBytes("Hello page 0");
            Array.Copy(text, 0, page0.Data, 0, text.Length);
            bpm.UnpinPage(pid0, isDirty: true);

            // Create new page 1
            var page1 = bpm.NewPage(out int pid1);
            Console.WriteLine($"Allocated new page {pid1}");
            var t1 = Encoding.UTF8.GetBytes("Page 1 content");
            Array.Copy(t1, 0, page1.Data, 0, t1.Length);
            bpm.UnpinPage(pid1, isDirty: true);

            // Fetch page0 again
            var f0 = bpm.FetchPage(pid0);
            Console.WriteLine("Fetched page0 content (as string): " + Encoding.UTF8.GetString(f0.Data).TrimEnd('\0'));
            bpm.UnpinPage(pid0, isDirty: false);

            // Allocate a third page
            var page2 = bpm.NewPage(out int pid2);
            Console.WriteLine($"Allocated new page {pid2}");
            bpm.UnpinPage(pid2, isDirty: true);

            bpm.DebugDump();

            // Now allocate a fourth page -> will cause eviction (pool size 3)
            var page3 = bpm.NewPage(out int pid3);
            if (page3 == null)
            {
                Console.WriteLine("No frame available for new page (unexpected)");
            }
            else
            {
                Console.WriteLine($"Allocated new page {pid3} (this will evict someone)");
                bpm.UnpinPage(pid3, isDirty: true);
            }

            bpm.DebugDump();

            // Fetch page1 to ensure it loads if was evicted
            var re1 = bpm.FetchPage(pid1);
            if (re1 != null)
            {
                Console.WriteLine("Fetched page1: " + Encoding.UTF8.GetString(re1.Data).TrimEnd('\0'));
                bpm.UnpinPage(pid1, isDirty: false);
            }
            else
            {
                Console.WriteLine("Failed to fetch page1 - not on disk?");
            }

            // Flush everything and final dump
            bpm.FlushAll();
            bpm.DebugDump();

            Console.WriteLine("Demo done.");
        }
    }
}
