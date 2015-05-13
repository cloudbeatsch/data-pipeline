using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataPipeline
{
    public static class TimeUtil
    {
        private static readonly DateTime _Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static long ToUnixTimestamp(this DateTime cur)
        {
            return (long)(cur - _Epoch).TotalMilliseconds;
        }

        public static DateTime FromUnixTimestamp(long timestamp)
        {
            return _Epoch.AddMilliseconds(timestamp);
        }
    }
}
