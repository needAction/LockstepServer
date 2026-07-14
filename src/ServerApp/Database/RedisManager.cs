using System;
using StackExchange.Redis;

namespace ServerApp.Database
{
    public class RedisManager
    {
        private static ConnectionMultiplexer _redis = null!;
        private static IDatabase _db =null!;

        // 레디스 서버 연결하는 함수 
        public static void Initialize(string connectString = "localhost:6379")
        {
            try
            {
                // 내 컴퓨터 (localhost)의 기본 레디스 포트인 6379로 
                _redis = ConnectionMultiplexer.Connect(connectString);
                _db = _redis.GetDatabase();
                Console.WriteLine("[데이터베이스] 레디스 연결!!!");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"[데이터베이스 에러] Redis 연결 실패: {ex.Message}");
            }
        }

        // 다른 클래스에서 레디스 기능을 꺼내 쓸 수 있게 통로를 열어줍니다.
        public static IDatabase Db => _db;
    }
}