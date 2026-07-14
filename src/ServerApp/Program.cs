using System;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Tasks;
using ServerApp.Network;
using ServerApp.Database;// 

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=============================================");
        Console.WriteLine("  락스텝 & 홀펀칭 하이브리드 중계 서버 v1.0  ");
        Console.WriteLine("=============================================");

        //[udp 서버가 실행되기 전에 redis 서버와 연결한다.]
        RedisManager.Initialize("localhost:6379");

        // 포트 설정:TCP(로비/매칭)은 5000번 UDP(홀펀칭/시그널링)은 5001;
        UdpPunchServer udpServer = new UdpPunchServer(5001);

        // 멀티스레드/ 테스크 환경에서 UDP 리스터를 배경에서 상시 가동시킵니다.
        Task udpTask = udpServer.StartAsync();

        Console.WriteLine("[시스템] 모든 네트워크 엔진이 준비되었습니다.");

        // 서버가 꺼지지 않도록 비동기 대기 상태를 유지합니다.
        await Task.WhenAll(udpTask);
    }
}