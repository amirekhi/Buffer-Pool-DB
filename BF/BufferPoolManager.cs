using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using BufferPool.PageDir;
using BufferPool.BF;
using BufferPool.DiskManagerDir;


namespace BufferPool.BF
{
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

}