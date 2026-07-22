using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading.Channels;
using Google.Protobuf;
using NetworkLib.Diagnostics;
using NetworkLib.Dispatcher;
using NetworkLib.Packets;
using StackExchange.Redis;

SimpleLogger.LogServer("SYSTEM", "=== [Linstep] 8-Player Lockstep Server with Redis & Dispatcher ===");

const int ServerPort = 5001;
using var udpServer = new UdpClient(ServerPort);

// 1. Redis DB 연결 (localhost:6379)
IDatabase? redisDb = await TryConnectRedisAsync("localhost:6379");
if (redisDb != null)
{
    SimpleLogger.LogServer("REDIS", "Redis 데이터베이스 연결 성공!");
}
else
{
    SimpleLogger.LogWarning("Redis 연결 실패. 메모리 전용 모드로 동작합니다.");
}

// 2. 방별 인원 설정 (1번 방 = 8인 방)
var roomInfoMap = new ConcurrentDictionary<int, HashSet<int>>();
roomInfoMap.TryAdd(1, new HashSet<int>{1,2,3,4,5,6,7,8}); 

// 3. Dispatcher 및 턴 상태 수집 구조
var dispatcher = new PacketDispatcher();

// [턴 상태 저장소]: 방 번호 -> (턴 번호 -> (플레이어별 입력 Dict, 생성시각))
var roomTurnStates = new ConcurrentDictionary<int, ConcurrentDictionary<long, (Dictionary<int, string> Inputs, DateTime CreatedAt)>>();

// 패킷 ID: 1 (ServerVerificationPacket) 핸들러 등록 (Redis 연결 객체 전달)
dispatcher.RegisterHandler(
    packetId: 1, 
    parser: ServerVerificationPacket.Parser, 
    handler: async (packet, _) => await HandleVerificationPacketAsync(packet, roomTurnStates, roomInfoMap, redisDb)
);

// 4. Backpressure 방어 Channel (최대 10,000개 완충)
var channelOptions = new BoundedChannelOptions(10000)
{
    SingleWriter = false,
    SingleReader = true,
    FullMode = BoundedChannelFullMode.DropOldest
};
Channel<(int PacketId, byte[] Data, int PlayerId)> packetChannel = Channel.CreateBounded<(int, byte[], int)>(channelOptions);

using var cts = new CancellationTokenSource();

// 5. 비동기 백그라운드 태스크 가동
_ = Task.Run(() => PacketProducerAsync(udpServer, packetChannel.Writer, cts.Token));
_ = Task.Run(() => PacketConsumerAsync(packetChannel.Reader, dispatcher, cts.Token));
_ = Task.Run(() => TimeoutMonitorAsync(roomTurnStates, roomInfoMap, cts.Token));

SimpleLogger.LogServer("SYSTEM", "Redis 연동 8인 검증 서버 구동 중. Enter를 누르면 종료합니다.");
Console.ReadLine();

await cts.CancelAsync();
udpServer.Close();


// ===================================================================
// [Dispatcher Handler] 8인 영수증 수집 및 Redis 영속화
// ===================================================================
static async Task HandleVerificationPacketAsync(
    ServerVerificationPacket packet,
    ConcurrentDictionary<int, ConcurrentDictionary<long, (Dictionary<int, string> Inputs, DateTime CreatedAt)>> roomTurnStates,
    ConcurrentDictionary<int, HashSet<int>> roomActivePlayer,
    IDatabase? redisDb)
{
    // 방에 참여중인 실제 플레이어 목록(기본 :1~8);
    var activePlayers = roomActivePlayer.GetValueOrDefault(
        packet.RoomNo, 
        new HashSet<int>{1,2,3,4,5,6,7,8}
        ); 

    var turnMap = roomTurnStates.GetOrAdd(packet.RoomNo, _ => new());
    var turnEntry = turnMap.GetOrAdd(packet.TurnNumber, _ => (new Dictionary<int, string>(), DateTime.Now));

    lock (turnEntry.Inputs)
    {
        turnEntry.Inputs[packet.PlayerId] = packet.Command;
    }

    // 8명의 영수증이 모두 모인 경우!
    if (turnEntry.Inputs.Count >= activePlayers.Count)
    {
        // 1. P1=...|P2=...|...|P8=... 동적 데이터 조립
        var commands = new List<string>();

        // 실제 존재하는 플레이어들의 조립
        foreach (int pId  in activePlayers)
        {
            string cmd= turnEntry.Inputs.GetValueOrDefault(pId, "NONE");
            commands.Add($"P{pId}={cmd}");
        }
        string combinedResult = string.Join("|", commands);

        SimpleLogger.LogServer("VERIFY", $"[Room: {packet.RoomNo}] [Turn: {packet.TurnNumber}] {activePlayers.Count} 검증 완료! -> ({combinedResult})");

        // 2. Redis에 최종 턴 상태 기록 (TTL 10분)
        if (redisDb != null)
        {
            string redisKey = $"room:{packet.RoomNo}:turn:{packet.TurnNumber}";
            await redisDb.StringSetAsync(redisKey, combinedResult, TimeSpan.FromMinutes(10));
            SimpleLogger.LogServer("REDIS", $"[Saved] Key: {redisKey} | Val: {combinedResult}");
        }

        // 3. 검증 완료된 메모리 정리
        turnMap.TryRemove(packet.TurnNumber, out _);
    }
}


// ===================================================================
// [Producer] UDP 소켓 패킷 수신 루프
// ===================================================================
static async Task PacketProducerAsync(
    UdpClient server, 
    ChannelWriter<(int PacketId, byte[] Data, int PlayerId)> writer, 
    CancellationToken token)
{
    while (!token.IsCancellationRequested)
    {
        try
        {
            var result = await server.ReceiveAsync(token);
            
            int packetId = 1; // Protobuf ServerVerificationPacket
            int playerId = 1; // 임시 플레이어 ID

            await writer.WriteAsync((packetId, result.Buffer, playerId), token);
        }
        catch (ObjectDisposedException) { break; }
        catch (OperationCanceledException) { break; }
        catch (Exception ex) { SimpleLogger.LogWarning($"수신 오류: {ex.Message}"); }
    }
}


// ===================================================================
// [Consumer] Dispatcher 파이프라인
// ===================================================================
static async Task PacketConsumerAsync(
    ChannelReader<(int PacketId, byte[] Data, int PlayerId)> reader, 
    PacketDispatcher dispatcher, 
    CancellationToken token)
{
    await foreach (var (packetId, data, playerId) in reader.ReadAllAsync(token))
    {
        await dispatcher.DispatchAsync(packetId, data, playerId);
    }
}


// ===================================================================
// [Timeout Monitor] 실제 참여자 기준 유실 패킷 감지
// ===================================================================
static async Task TimeoutMonitorAsync(
    ConcurrentDictionary<int, ConcurrentDictionary<long, (Dictionary<int, string> Inputs, DateTime CreatedAt)>> roomTurnStates,
    ConcurrentDictionary<int, HashSet<int>> roomActivePlayers,
    CancellationToken token)
{
    const int TimeoutMilliseconds = 300;

    while (!token.IsCancellationRequested)
    {
        try
        {
            DateTime now = DateTime.Now;

            foreach (var (roomNo, turnMap) in roomTurnStates)
            {
                var activePlayers = roomActivePlayers.GetValueOrDefault(roomNo, new HashSet<int>{ 1, 2, 3, 4, 5, 6, 7, 8 });

                foreach (var (turnNumber, turnEntry) in turnMap)
                {
                    if ((now - turnEntry.CreatedAt).TotalMilliseconds > TimeoutMilliseconds)
                    {
                        lock (turnEntry.Inputs)
                        {
                            foreach (int pId in activePlayers)
                            {
                                if (!turnEntry.Inputs.ContainsKey(pId))
                                {
                                    SimpleLogger.LogWarning($"[PACKET LOSS] [Room: {roomNo}] [Turn: {turnNumber}] Player {pId}/{activePlayers.Count} 패킷 미도달!");
                                }
                            }
                        }
                        turnMap.TryRemove(turnNumber, out _);
                    }
                }
            }
            await Task.Delay(50, token);
        }
        catch (OperationCanceledException) { break; }
    }
}


// ===================================================================
// [Redis Helper] Redis 서버 연결 시도 함수
// ===================================================================
static async Task<IDatabase?> TryConnectRedisAsync(string connectionString)
{
    try
    {
        var options = ConfigurationOptions.Parse(connectionString);
        options.ConnectTimeout = 1000; // 1초 타임아웃
        options.AbortOnConnectFail = false;

        var redis = await ConnectionMultiplexer.ConnectAsync(options);
        if (redis.IsConnected)
        {
            return redis.GetDatabase();
        }
    }
    catch (Exception ex)
    {
        SimpleLogger.LogWarning($"Redis 연결 오류: {ex.Message}");
    }
    return null;
}