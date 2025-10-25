using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BufferPool.DiskManagerDir
{
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
}