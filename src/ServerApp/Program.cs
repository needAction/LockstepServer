using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Google.Protobuf;
using NetworkLib.Diagnostics;
using NetworkLib.Packets;
using StackExchange.Redis;

SimpleLogger.LogServer("SYSTEM", "=== [Linstep] .NET 8 Verification Server with Redis & Recovery ===");

const int ServerPort = 5001;
using var udpServer = new UdpClient(ServerPort);

// 1. Redis 연결 시도 (없으면 null 로 안전 폴백)
IDatabase? redisDb = await TryConnectRedisAsync("localhost:6379");
// 방별 설정 정보(방번호 -> 최대 인원수, 기본 8명)
var roomInfoMap = new ConcurrentDictionary<int, int>();

// 2. 고성능 Bounded Channel 생성 (최대 10,000개 수용, 백프레셔 방어)
var channelOptions = new BoundedChannelOptions(10000)
{
    SingleWriter = false,
    SingleReader = true,
    FullMode = BoundedChannelFullMode.DropOldest
};
Channel<ServerVerificationPacket> packetChannel = Channel.CreateBounded<ServerVerificationPacket>(channelOptions);

using var cts = new CancellationTokenSource();

// 3. 하이브리드 중첩 데이터 구조
var roomTurnStates = new ConcurrentDictionary<int, ConcurrentDictionary<long, (Dictionary<int, string> Inputs, DateTime CreatedAt)>>();

// 4. 비동기 스레드 파이프라인 가동
_ = Task.Run(() => PacketProducerAsync(udpServer, packetChannel.Writer, cts.Token));
_ = Task.Run(() => PacketConsumerAsync(packetChannel.Reader, roomTurnStates, redisDb, cts.Token));
_ = Task.Run(() => TimeoutMonitorAsync(roomTurnStates, cts.Token));

SimpleLogger.LogServer("SYSTEM", "서버 정상 기동 완료. Enter를 누르면 종료합니다.");
Console.ReadLine();

await cts.CancelAsync();
udpServer.Close();

// ===================================================================
// [Producer] 네트워크 UDP 수신 (Zero-allocation & Channel Push)
// ===================================================================
static async Task PacketProducerAsync(UdpClient server, ChannelWriter<ServerVerificationPacket> writer, CancellationToken token)
{
    while (!token.IsCancellationRequested)
    {
        try
        {
            var result = await server.ReceiveAsync(token);
            var packet = ServerVerificationPacket.Parser.ParseFrom(result.Buffer.AsSpan());
            await writer.WriteAsync(packet, token);
        }
        catch (ObjectDisposedException) { break; }
        catch (OperationCanceledException) { break; }
        catch (Exception ex) { SimpleLogger.LogWarning($"수신 예외: {ex.Message}"); }
    }
}

// ===================================================================
// [Consumer] 턴 검증 및 Redis 비동기 영구 기록
// ===================================================================
static async Task PacketConsumerAsync(
    ChannelReader<ServerVerificationPacket> reader, 
    ConcurrentDictionary<int, ConcurrentDictionary<long, (Dictionary<int, string> Inputs, DateTime CreatedAt)>> roomTurnStates,
    IDatabase? redisDb,
    CancellationToken token)
{
    await foreach (ServerVerificationPacket packet in reader.ReadAllAsync(token))
    {
        var turnMap = roomTurnStates.GetOrAdd(packet.RoomNo, _ => new());
        var turnEntry = turnMap.GetOrAdd(packet.TurnNumber, _ => (new Dictionary<int, string>(), DateTime.Now));

        lock (turnEntry.Inputs)
        {
            turnEntry.Inputs[packet.PlayerId] = packet.Command;
        }

        if (turnEntry.Inputs.Count >= 2)
        {
            string p1Command = turnEntry.Inputs.GetValueOrDefault(1, "NONE");
            string p2Command = turnEntry.Inputs.GetValueOrDefault(2, "NONE");

            SimpleLogger.LogServer("VERIFY", $"[Room: {packet.RoomNo}] [Turn: {packet.TurnNumber}] 검증 성공! (P1: {p1Command}, P2: {p2Command})");

            if (redisDb != null)
            {
                string redisKey = $"room:{packet.RoomNo}:turn:{packet.TurnNumber}";
                string redisValue = $"P1={p1Command}|P2={p2Command}";
                await redisDb.StringSetAsync(redisKey, redisValue, TimeSpan.FromMinutes(10));
            }

            turnMap.TryRemove(packet.TurnNumber, out _);
        }
    }
}

// ===================================================================
// [Timeout Monitor] 타임아웃 감시 및 패킷 유실 대처
// ===================================================================
static async Task TimeoutMonitorAsync(
    ConcurrentDictionary<int, ConcurrentDictionary<long, (Dictionary<int, string> Inputs, DateTime CreatedAt)>> roomTurnStates,
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
                foreach (var (turnNumber, turnEntry) in turnMap)
                {
                    if ((now - turnEntry.CreatedAt).TotalMilliseconds > TimeoutMilliseconds)
                    {
                        lock (turnEntry.Inputs)
                        {
                            for (int playerId = 1; playerId <= 2; playerId++)
                            {
                                if (!turnEntry.Inputs.ContainsKey(playerId))
                                {
                                    SimpleLogger.LogWarning($"[PACKET LOSS] [Room: {roomNo}] [Turn: {turnNumber}] Player {playerId} 패킷 유실 감지!");
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
// Redis 연결 도우미
// ===================================================================
static async Task<IDatabase?> TryConnectRedisAsync(string connectionString)
{
    try
    {
        var options = ConfigurationOptions.Parse(connectionString);
        options.ConnectTimeout = 1000;
        options.AbortOnConnectFail = false;

        var redis = await ConnectionMultiplexer.ConnectAsync(options);
        if (redis.IsConnected)
        {
            SimpleLogger.LogServer("REDIS", "✅ Redis 연결 성공!");
            return redis.GetDatabase();
        }
    }
    catch { SimpleLogger.LogWarning("Redis 연결 실패. (메모리 검증 모드로 작동)"); }

    return null;
}