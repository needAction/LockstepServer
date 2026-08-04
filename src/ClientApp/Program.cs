using System.Net;
using System.Net.Sockets;
using ClientApp.P2P;
using ClientApp.Utils;
using Google.Protobuf;
using NetworkLib.Protocol;
using NetworkLib.Diagnostics;
using NetworkLib.Packets;



internal class Program
{
    private const string SERVER_IP = "127.0.0.1";
    private const int SERVER_PORT = 5001;
    // 락스텝 세션 설정
    private const int TOTAL_PLAYERS = 8; // 총 플레이어 수
    private const int TURN_TIME_MS = 10; // 각 턴의 시간 제한 (밀리초)
    private const int VERIFY_INTERVAL_TURNS = 10; // 검증 주기 (밀리초)

    private static async Task Main(string[] args)
    {
        int myPlayerId = 1; // 예시로 0번 플레이어로 설정 (실제 게임에서는 서버에서 할당)
        if (args.Length > 0 &&  int.TryParse(args[0], out int parsedId))
        {
            myPlayerId = parsedId;
        }
        int myP2PPort = 6000 + myPlayerId; // 예시로 포트 번호를 플레이어 ID 기반으로 설정
        SimpleLogger.LogClientP2P($"Player {myPlayerId} starting on port {myP2PPort}");
        
        using var p2pSocket = new UdpClient(myP2PPort);
        var sessionManager = new P2PSessionManager(myPlayerId, p2pSocket);

        //P2P 세션 주소록 설정
        for (int i = 1; i <= TOTAL_PLAYERS; i++)
        {
            if (i == myPlayerId) continue; // 자기 자신은 제외
            
            sessionManager.AddPeer(i, "127.0.0.1", 6000 + i);
        }
        // 락스텝 턴 수집 버퍼 생성
        var turnBuffer = new LockstepTurnBuffer(TOTAL_PLAYERS);
        using var cts = new CancellationTokenSource();

        //1. P2P 수신 루프 시작
        var receiveTask = ReceivePeerPacketsAsync(p2pSocket, myPlayerId, turnBuffer, sessionManager, cts.Token);
        //2. 메인 락스텝 시뮬레이션 루프 가동
        var gameLoopTask = LockstepGameLoopAsync(p2pSocket, myPlayerId, sessionManager, turnBuffer, cts.Token);

        SimpleLogger.LogClientP2P($"SYSTEM", "락스텝 엔진 가동 완료. (엔터 키를 누르면 종료합니다)");
        Console.ReadLine();

        cts.Cancel();
        await Task.WhenAll(receiveTask, gameLoopTask);

    }
    /// <summary>
    /// p2p 네트워크 패킷 수신 및 버퍼 채우기 루프
    /// </summary>
    private static async Task ReceivePeerPacketsAsync(UdpClient socket, int myPlayerId, LockstepTurnBuffer turnBuffer, P2PSessionManager sessionManager, CancellationToken token)
    {
        while(!token.IsCancellationRequested)
        {
            try
            {
                var result = await socket.ReceiveAsync(token);
                var (packetType, bodyMemory) = PacketSerializer.Deserialize(result.Buffer);

                switch ((PacketType)packetType)
                {
                    case PacketType.GameInput:
                        var inputPacket = GameInputPacket.Parser.ParseFrom(bodyMemory.Span);
                        
                        // Phase 5: 수신된 P2P 입력을 턴 버퍼에 쌓음
                        turnBuffer.AddInput(inputPacket);
                        break;

                    case PacketType.Ping:
                        var pingPacket = PingPacket.Parser.ParseFrom(bodyMemory.Span);
                        var pongPacket = new PongPacket
                        {
                            SenderPlayerId = myPlayerId,
                            ReceiveTimestamp = pingPacket.SendTimestamp
                        };

                        byte[] pongBytes = PacketSerializer.Serialize(PacketType.Pong, pongPacket);
                        await socket.SendAsync(pongBytes, pongBytes.Length, result.RemoteEndPoint);
                        break;

                    case PacketType.Pong:
                        var pongRes = PongPacket.Parser.ParseFrom(bodyMemory.Span);
                        long currentTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        long rtt = currentTimestamp - pongRes.ReceiveTimestamp;

                        sessionManager.SetRtt(pongRes.SenderPlayerId, rtt);
                        break;
                }
            } catch (OperationCanceledException)
            {
                // 취소 요청 시 루프 종료
                break;
            }
            catch (Exception ex)
            {
                SimpleLogger.LogClientP2P($"ERROR", $"[Receive Error] {ex.Message}");
            }
        }
    }
    /// <summary>
    /// 락스텝 메인 가상 시뮬레이션 루프(turn frame 연산 &  StateHash 검증)
    /// </summary>
    private static async Task LockstepGameLoopAsync(UdpClient socket, int myPlayerId, P2PSessionManager sessionManager, LockstepTurnBuffer turnBuffer, CancellationToken token)
    {
        int currentTurn = 1;
        var serverEndPoint = new IPEndPoint(IPAddress.Parse(SERVER_IP), SERVER_PORT);

        // 가상 캐릭터 위치 상태
        int playerX = 100 + (myPlayerId * 10); // 예시로 플레이어 ID 기반 초기 위치 설정
        int playerY = 200;


        while (!token.IsCancellationRequested)
        {
           try
            {
                // Step 1 : 내 이번턴 (currentTurn) 조작 생성 및 p2p 브로드캐스트
                var myCommand = $"MOVE_RIGHT_TURN_{currentTurn}"; // 예시로 단순 문자열 명령 생성
                var myInput = new GameInputPacket
                {
                    TurnNumber = (int)currentTurn,
                    PlayerId = myPlayerId,
                    Command = myCommand,
                    ClientTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

                };

                // 내가 보낸 입력도 내 버퍼에 먼저 채움
                turnBuffer.AddInput(myInput);
                // 다른 peer들에게 내 입력 Broadcast
                await sessionManager.BroadcastAsync(myInput);

                // Step 2 : 이번 턴에 대한 모든 peer 입력 수집 (Turn Stall/Lock)
                int stallCount = 0;
                while(!turnBuffer.IsTurnReady(currentTurn))
                {
                    await Task.Delay(1, token); // 1ms 대기
                    stallCount++;
                    // 1초 이상 입력이 안들어오면 경고 로그 출력
                    if (stallCount % 1000 == 0) // 1초 이상 대기 시 경고
                    {
                        SimpleLogger.LogClientP2P($"STALL", $"[Turn Stall] Turn {currentTurn} 입력 수집 지연 중...");
                        stallCount = 0; // 경고 후 카운트 초기화
                    }
                }
                // Step 3 : 전체 인원의 입력 수집 완료 -> TurnFrame 소비 침 게임 로직 결정론 연산
                var turnFrame = turnBuffer.ConsumeTurnFrame(currentTurn);
                if(turnFrame != null)
                {
                   // 결정론적 상태 업데이트 연산 (가상)
                   playerX += 1; // 예시로 단순히 x좌표 증가
                }

                // Step 4 : 턴 연산 완료 후 stateHash 생성 
                string stateSummary = $"P{myPlayerId}:X={playerX},Y={playerY}";
                string stateHash = StateHashGenerator.GenerateStateHash(currentTurn, stateSummary);

                // Step 5 : 10턴마다 서버에 stateHash 검증 요청
                if(currentTurn % VERIFY_INTERVAL_TURNS == 0)
                {
                    var verifyPacket = new ServerVerificationPacket
                    {
                        RoomNo = 1,
                        TurnNumber = (int)currentTurn,
                        PlayerId = myPlayerId,
                        Command = myCommand,
                        StateHash = stateHash
                    };
                    byte[] rawPacket = PacketSerializer.Serialize(PacketType.ServerVerification, verifyPacket);
                    await socket.SendAsync(rawPacket, rawPacket.Length, serverEndPoint);

                    SimpleLogger.LogClientP2P("LOCKSTEP", 
                        $"[Turn {currentTurn}] 연산 완료 (Hash: {stateHash}) ──► 서버 영수증 제출");
                }

                // step 6 :  다음턴으로 이동 및 10ms 락스텝 주기 대기
                currentTurn++;
                await Task.Delay(TURN_TIME_MS, token);
            }
            catch (OperationCanceledException)
            {
                // 취소 요청 시 루프 종료
                break;
            }
            catch (Exception ex)
            {
                SimpleLogger.LogClientP2P($"ERROR", $"[GameLoop Error] {ex.Message}");
            }
        }
    }


}