using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BufferPool.PageDir
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
}