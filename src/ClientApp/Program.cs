using System.Net;
using System.Net.Sockets;
using ClientApp.P2P;
using Google.Protobuf;
using NetworkLib.Protocol;
using NetworkLib.Diagnostics;
using NetworkLib.Packets;

Console.WriteLine("=== [Linstep] Modern Async UDP Client (.NET 8+) ===");

Console.Write("내 플레이어 ID를 입력하세요 (1 ~8): ");
int myPlayerId = int.Parse(Console.ReadLine() ?? "1");
// 내 P2P 수신 포트 설정 (예: P1=5002, P2=5003, P3=5004...)
int myP2PPort = 5001 + myPlayerId;


using var p2pSocket = new UdpClient(myP2PPort); // 내 p2p 수신 포트 바인딩
using var serverSocket = new UdpClient();// 서버 통신용

var serverEndPoint = new IPEndPoint(IPAddress.Loopback, 5001);

//1. P2P 세션 관리자 생성
var sessionManager = new P2PSessionManager(myPlayerId, p2pSocket);
//2. 동적 Peer 등록 (실제로는 서버에서 방 참여자 IP/Port 목록을 받아와서 등록함)
//테스트용: 1번~3번 유저가 3인 P2P 방에 들어왔다고 가정
for (int id = 1; id <= 3; id++)
{
    if (id != myPlayerId)
    {
        int peerPort = 5001 + id;
        sessionManager.AddPeer(id, "127.0.0.1", peerPort);
    }
}
using var cts = new CancellationTokenSource();

//3. 비동기 백그라운드 테스크 기동
// (A) 다른 N명의 peer 들로부터 오는 입력 수신
_ = Task.Run(() => ReceivePeerPacketsAsync(p2pSocket, myPlayerId, cts.Token));

//(B) 10ms 매턴 입력 1:N 브로드 캐스트 + 100ms 서버 영수증 발송
_ = Task.Run(() => GameLoopAsync(sessionManager, serverSocket, serverEndPoint, myPlayerId, roomNo: 1, cts.Token));

SimpleLogger.LogClientP2P("SYSTEM", $"Client {myPlayerId} (Port: {myP2PPort}) 가동 중. Enter를 누르면 종료합니다.");
Console.ReadLine();

await cts.CancelAsync();


//============
//[p2p Sender] 10ms 단위 1:N BrodCast 및 100 서버 영수증 전송
//===============

static async Task GameLoopAsync(P2PSessionManager sessionManager, UdpClient serverSocket, IPEndPoint serverEP,
int myPlayerId, int roomNo, CancellationToken token)
{
    long turnNumber = 0;

    while (!token.IsCancellationRequested)
    {
        turnNumber++;
        string currentCommand = $"MOVE_RIGHT_TURN_{turnNumber}";

        // 1. P2P 패킷 생성
        var p2pPacket = new GameInputPacket
        {
            PlayerId = myPlayerId,
            TurnNumber = turnNumber,
            Command = currentCommand
        };
        //2. [1:N P2P BrodCast] 세션 관리자를 통해 N명의 Peer들에게 동시 전송
        await sessionManager.BroadcastAsync(p2pPacket);
        //3. [100ms 서버 영수증] 10 턴에 한번씩 검증서버로 제줄
        // GameLoopAsync 내부 서버 영수증 전송 부분 수정:
        if (turnNumber % 10 == 0)
        {
            var verifyPacket = new ServerVerificationPacket
            {
                RoomNo = roomNo,
                TurnNumber = turnNumber,
                PlayerId = myPlayerId,
                Command = currentCommand
            };

            // PacketId = 1 (서버 검증 영수증) 헤더를 포장하여 바이너리로 변환합니다.
            byte[] verifyData = NetworkLib.Protocol.PacketSerializer.Serialize(packetId: 1, verifyPacket);

            // 검증 서버로 포장된 영수증 패킷을 전송합니다.
            await serverSocket.SendAsync(verifyData, verifyData.Length, serverEP);
        }

    }
    // 락스텝 10ms 로직 타임스텝
    await Task.Delay(10, token);
}


//=========
//[P2P Receiver] 다른 N명의 peer 패킷 수신 루프
// =============

static async Task ReceivePeerPacketsAsync(UdpClient socket, int myPlayerId, CancellationToken token)
{
    while (!token.IsCancellationRequested)
    {
        try
        {
            var result = await socket.ReceiveAsync(token);
            // 수신받은 데이터에서 헤더(PacketId, Body)를 분리
            var (packetId, body) = PacketSerializer.Deserialize(result.Buffer);
            // 2. 패킷 종류에 따른 비동기 분기 핸들링
            switch (packetType: (PacketType)packetId)
            {
                case PacketType.GameInput:
                    var inputPacket = GameInputPacket.Parser.ParseFrom(body.Span);
                    SimpleLogger.LogClientP2P("P2P_RECV", $"[Turn : {inputPacket.TurnNumber}] player {inputPacket.PlayerId} 입력: {inputPacket.Command}");
                    break;
                case PacketType.Ping:
                    var ping = PingPacket.Parser.ParseFrom(body.Span);
                    // ping 패킷 수신 즉시 시간정보를 얹어 pong 패킷으로 응답 (Echo)
                    var pong = new PongPacket
                    {
                        SenderPlayerId = myPlayerId,
                        OriginalSendTimestamp = ping.SendTimestamp,
                        ReceiveTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    };
                    byte[] pongBytes = PacketSerializer.Serialize(PacketType.Pong, pong);
                    await socket.SendAsync(pongBytes, pongBytes.Length, result.RemoteEndPoint);
                    break;
                case PacketType.Pong:
                    var pongRecv = PongPacket.Parser.ParseFrom(body.Span);
                    //rtt 계산: 현재시간 - ping 전송시간
                    long rtt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - pongRecv.OriginalSendTimestamp;
                    SimpleLogger.LogClientP2P("RTT_CHECK", $"Player {pongRecv.SenderPlayerId}와의 RTT: {rtt}ms");
                    break;
                default:
                    SimpleLogger.LogWarning($"알 수 없는 패킷 수신: PacketId={packetId}");
                    break;
            }

        }
        catch (OperationCanceledException) { break; }
        catch (Exception ex)
        {
            SimpleLogger.LogWarning($"P2P 수신 에러: {ex.Message}");
        }
    }
}