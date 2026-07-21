using System.Net;
using System.Net.Sockets;
using Google.Protobuf;
using NetworkLib.Packets;

Console.WriteLine("=== [Linstep] Modern Async UDP Client (.NET 8+) ===");

Console.Write("내 플레이어 ID를 입력하세요 (1 또는 2): ");
int playerId = int.Parse(Console.ReadLine() ?? "1");

int myPort = (playerId == 1) ? 6001 : 6002;
int peerPort = (playerId == 1) ? 6002 : 6001;

using var udpClient = new UdpClient(myPort);
var serverEndPoint = new IPEndPoint(IPAddress.Loopback, 5001);
var peerEndPoint = new IPEndPoint(IPAddress.Loopback, peerPort);

using var cts = new CancellationTokenSource();

Console.WriteLine($"[로컬] 내 포트: {myPort} | 상대방 포트: {peerPort} 세팅 완료.");

// C# 12 / Task.Run 최신 가이드: 백그라운드 태스크 기동
_ = Task.Run(() => ReceiveLoopAsync(udpClient, cts.Token));
_ = Task.Run(() => SendP2PLoopAsync(udpClient, playerId, peerEndPoint, cts.Token));
_ = Task.Run(() => SendServerVerificationLoopAsync(udpClient, playerId, serverEndPoint, cts.Token));

Console.WriteLine("통신 루프 가동 중... 종료하려면 Enter를 누르세요.");
Console.ReadLine();

await cts.CancelAsync(); // .NET 8+ 비동기 취소 최신 API
Console.WriteLine("클라이언트를 안전하게 종료합니다.");

// ===================================================================
// 비동기 통신 전담 메서드 (Local Functions / Modern Async)
// ===================================================================

static async Task ReceiveLoopAsync(UdpClient client, CancellationToken token)
{
    while (!token.IsCancellationRequested)
    {
        try
        {
            var result = await client.ReceiveAsync(token);
            ReadOnlyMemory<byte> buffer = result.Buffer; // Zero-copy 슬라이싱 준비

            // 1. P2P 패킷 파싱 시도
            try
            {
                var p2pPacket = P2PInputPacket.Parser.ParseFrom(buffer.Span);
                if (p2pPacket.PlayerId != 0 && !string.IsNullOrEmpty(p2pPacket.Command))
                {
                    Console.WriteLine($"[P2P 수신] 상대방({p2pPacket.PlayerId}) 입력: {p2pPacket.Command} (프레임: {p2pPacket.CurrentFrame})");
                    continue;
                }
            }
            catch { /* 파싱 실패 시 통과 */ }

            // 2. 서버 동기화 패킷 파싱 시도
            try
            {
                var syncPacket = ServerSyncPacket.Parser.ParseFrom(buffer.Span);
                Console.WriteLine($"[서버 수신] 최종 동기화 턴: {syncPacket.TurnNumber} 확정 완료!");
            }
            catch { /* 파싱 실패 시 통과 */ }
        }
        catch (ObjectDisposedException) { break; }
        catch (Exception) { /* 10054 흡수 */ }
    }
}

static async Task SendP2PLoopAsync(UdpClient client, int playerId, IPEndPoint target, CancellationToken token)
{
    long frameCounter = 0;
    string[] testCommands = ["w", "a", "s", "d"]; // C# 12 Collection Expressions [...]
    
    try
    {
        while (!token.IsCancellationRequested)
        {
            string myInput = testCommands[Random.Shared.Next(testCommands.Length)]; // .NET 6+ Thread-safe Random

            P2PInputPacket p2pPacket = new()
            {
                PlayerId = playerId,
                Command = myInput,
                CurrentFrame = frameCounter++
            };

            byte[] sendBytes = p2pPacket.ToByteArray();
            await client.SendAsync(sendBytes, sendBytes.Length, target);

            await Task.Delay(10, token); // 10ms (100Hz)
        }
    }
    catch (OperationCanceledException) { }
}

static async Task SendServerVerificationLoopAsync(UdpClient client, int playerId, IPEndPoint server, CancellationToken token)
{
    long turnCounter = 0;
    try
    {
        while (!token.IsCancellationRequested)
        {
            ServerVerificationPacket packet = new()
            {
                RoomNo = 1,
                PlayerId = playerId,
                TurnNumber = turnCounter++,
                Command = "VerificationReceipt"
            };

            byte[] sendBytes = packet.ToByteArray();
            await client.SendAsync(sendBytes, sendBytes.Length, server);

            await Task.Delay(100, token); // 100ms (10Hz)
        }
    }
    catch (OperationCanceledException) { }
}