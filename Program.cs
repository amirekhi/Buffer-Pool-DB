using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace BufferPoolDemo
{
    public class Page
    {
        public int PageId { get; internal set; } = -1;
        public int PinCount { get; internal set; } = 0;
        public bool IsDirty { get; internal set; } = false;
        public byte[] Data { get; internal set; }

        public Page(int pageSize)
        {
            Data = new byte[pageSize];
        }

        public void Clear()
        {
            Array.Clear(Data, 0, Data.Length);
            PageId = -1;
            PinCount = 0;
            IsDirty = false;
        }
    }

    // Simulated disk manager: simple in-memory page store
    public class DiskManager
    {
        private readonly Dictionary<int, byte[]> store = new();
        private int nextPageId = 0;
        private readonly int pageSize;
        private readonly object diskLock = new();

        public DiskManager(int pageSize)
        {
            this.pageSize = pageSize;
        }

        public int AllocatePage()
        {
            lock (diskLock)
            {
                int id = nextPageId++;
                store[id] = new byte[pageSize];
                return id;
            }
        }

        public void DeallocatePage(int pageId)
        {
            lock (diskLock)
            {
                store.Remove(pageId);
            }
        }

        public void WritePage(int pageId, byte[] data)
        {
            if (data.Length != pageSize) throw new ArgumentException("Bad page size");
            lock (diskLock)
            {
                store[pageId] = (byte[])data.Clone();
            }
        }

        public byte[] ReadPage(int pageId)
        {
            lock (diskLock)
            {
                if (!store.ContainsKey(pageId)) throw new InvalidOperationException($"Page {pageId} not on disk.");
                return (byte[])store[pageId].Clone();
            }
        }

        public bool PageExists(int pageId)
        {
            lock (diskLock)
            {
                return store.ContainsKey(pageId);
            }
        }
    }

    // LRU replacer (stores frame indices)
    internal class LruReplacer
    {
        private readonly LinkedList<int> list = new();
        private readonly Dictionary<int, LinkedListNode<int>> map = new();

        public void Access(int frameIndex)
        {
            if (map.TryGetValue(frameIndex, out var node))
            {
                list.Remove(node);
                list.AddFirst(node);
            }
            else
            {
                var n = list.AddFirst(frameIndex);
                map[frameIndex] = n;
            }
        }

        public void Erase(int frameIndex)
        {
            if (map.TryGetValue(frameIndex, out var node))
            {
                list.Remove(node);
                map.Remove(frameIndex);
            }
        }

        // Choose victim: least recently used (last)
        public bool Victim(out int frameIndex)
        {
            if (list.Count == 0)
            {
                frameIndex = -1;
                return false;
            }
            var last = list.Last!;
            frameIndex = last.Value;
            list.RemoveLast();
            map.Remove(frameIndex);
            return true;
        }

        public int Size => list.Count;
    }

    public class BufferPoolManager
    {
        private readonly int poolSize;
        private readonly int pageSize;
        private readonly Page[] frames;
        private readonly Dictionary<int, int> pageTable = new(); // pageId -> frameIndex
        private readonly Queue<int> freeList = new();
        private readonly LruReplacer replacer = new();
        private readonly DiskManager diskManager;
        private readonly object globalLock = new();

        public BufferPoolManager(int poolSize, int pageSize, DiskManager diskManager)
        {
            if (poolSize <= 0) throw new ArgumentException("poolSize must be > 0");
            this.poolSize = poolSize;
            this.pageSize = pageSize;
            this.diskManager = diskManager;
            frames = new Page[poolSize];
            for (int i = 0; i < poolSize; i++)
            {
                frames[i] = new Page(pageSize);
                freeList.Enqueue(i);
            }
        }

        // Fetch page into buffer (pin it). Returns null if page doesn't exist on disk (or you can change to allocate)
        public Page? FetchPage(int pageId)
        {
            lock (globalLock)
            {
                if (pageTable.TryGetValue(pageId, out int frameIndex))
                {
                    var page = frames[frameIndex];
                    page.PinCount++;
                    // frame is now pinned -> remove from replacer
                    replacer.Erase(frameIndex);
                    return page;
                }

                // need to bring page from disk: find victim frame
                if (!FindVictimFrame(out int victim))
                {
                    return null; // no frame available
                }

                // if victim holds a valid page, flush if dirty and evict mapping
                var victimPage = frames[victim];
                if (victimPage.PageId != -1)
                {
                    if (victimPage.IsDirty)
                    {
                        diskManager.WritePage(victimPage.PageId, victimPage.Data);
                        victimPage.IsDirty = false;
                    }
                    pageTable.Remove(victimPage.PageId);
                }

                // read from disk
                if (!diskManager.PageExists(pageId)) return null;
                var raw = diskManager.ReadPage(pageId);
                Array.Copy(raw, 0, victimPage.Data, 0, pageSize);
                victimPage.PageId = pageId;
                victimPage.PinCount = 1;
                victimPage.IsDirty = false;

                pageTable[pageId] = victim;
                // pinned -> not in replacer
                replacer.Erase(victim);
                return victimPage;
            }
        }

        // Unpin a page: mark dirty if needed. If pin becomes zero, page becomes a candidate for replacement.
        public bool UnpinPage(int pageId, bool isDirty)
        {
            lock (globalLock)
            {
                if (!pageTable.TryGetValue(pageId, out var frameIndex)) return false;
                var page = frames[frameIndex];
                if (page.PinCount <= 0) return false;
                page.PinCount--;
                if (isDirty) page.IsDirty = true;
                if (page.PinCount == 0)
                {
                    // add to LRU replacer as evictable
                    replacer.Access(frameIndex);
                }
                return true;
            }
        }

        // Allocate a new page and pin it into buffer (returns page object)
        public Page? NewPage(out int newPageId)
        {
            lock (globalLock)
            {
                newPageId = diskManager.AllocatePage();
                if (!FindVictimFrame(out int victim))
                {
                    // no frame available
                    return null;
                }

                var victimPage = frames[victim];
                if (victimPage.PageId != -1)
                {
                    if (victimPage.IsDirty)
                        diskManager.WritePage(victimPage.PageId, victimPage.Data);
                    pageTable.Remove(victimPage.PageId);
                }

                victimPage.Clear();
                victimPage.PageId = newPageId;
                // For a new page we typically return a zeroed buffer. Caller will modify then Unpin with isDirty=true.
                victimPage.PinCount = 1;
                victimPage.IsDirty = false;

                pageTable[newPageId] = victim;
                replacer.Erase(victim);
                return victimPage;
            }
        }

        // Delete a page from disk and buffer (must not be pinned)
        public bool DeletePage(int pageId)
        {
            lock (globalLock)
            {
                if (pageTable.TryGetValue(pageId, out int frameIndex))
                {
                    var page = frames[frameIndex];
                    if (page.PinCount != 0) return false; // can't delete pinned page
                    // evict
                    pageTable.Remove(pageId);
                    page.Clear();
                    freeList.Enqueue(frameIndex);
                    replacer.Erase(frameIndex);
                }

                // remove from disk
                if (diskManager.PageExists(pageId))
                {
                    diskManager.DeallocatePage(pageId);
                }
                return true;
            }
        }

        // Flush page to disk
        public bool FlushPage(int pageId)
        {
            lock (globalLock)
            {
                if (!pageTable.TryGetValue(pageId, out int frameIndex)) return false;
                var page = frames[frameIndex];
                if (page.IsDirty)
                {
                    diskManager.WritePage(pageId, page.Data);
                    page.IsDirty = false;
                }
                return true;
            }
        }

        public void FlushAll()
        {
            lock (globalLock)
            {
                foreach (var kv in pageTable)
                {
                    var page = frames[kv.Value];
                    if (page.IsDirty)
                    {
                        diskManager.WritePage(page.PageId, page.Data);
                        page.IsDirty = false;
                    }
                }
            }
        }

        // Find a free/victim frame index
        private bool FindVictimFrame(out int frameIndex)
        {
            // Prefer free list
            if (freeList.Count > 0)
            {
                frameIndex = freeList.Dequeue();
                return true;
            }

            // Ask replacer (LRU)
            if (replacer.Victim(out int victim))
            {
                frameIndex = victim;
                // ensure victim frame has pincount == 0
                if (frames[frameIndex].PinCount != 0)
                {
                    // shouldn't happen if replacer used correctly, but be safe
                    frameIndex = -1;
                    return false;
                }
                return true;
            }

            frameIndex = -1;
            return false;
        }

        // For debug / test: get a snapshot of buffer contents
        public void DebugDump()
        {
            lock (globalLock)
            {
                Console.WriteLine("BufferPool Dump:");
                for (int i = 0; i < poolSize; i++)
                {
                    var p = frames[i];
                    Console.WriteLine($"Frame {i}: PageId={p.PageId}, Pin={p.PinCount}, Dirty={p.IsDirty}");
                }
                Console.WriteLine("PageTable:");
                foreach (var kv in pageTable) Console.WriteLine($"  Page {kv.Key} -> Frame {kv.Value}");
                Console.WriteLine($"FreeList count: {freeList.Count}, Replacer size: {replacer.Size}");
                Console.WriteLine();
            }
        }
    }

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
