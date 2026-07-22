using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace NetworkLib.Diagnostics
{
    public static class SimpleLogger
    {
        // 서버용 로그 (화이트/그레이 - 정돈된 형태) 
        public static void LogServer(string catgory, string message)
        {
            string timeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine($"[{timeStamp}] [{catgory.ToUpper()}] {message}");
            Console.ResetColor();
        }
        // 클라이언트 P2P 용로그 (초록색 - 생동감 있는 출력 )
        public static void LogClientP2P(string message)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[P2P] : {message} ");
            Console.ResetColor();

        }
        public static void LogClientP2P(string catgory, string message)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"[P2P] : {message} ");
            Console.ResetColor();

        }

        // 시스템 경고 로그 (노란색)
        public static void LogWarning(string message)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"[WARN] ⚠️ {message}");
            Console.ResetColor();
        }
    }
}