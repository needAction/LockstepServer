using System.Collections.Concurrent;
using NetworkLib.Diagnostics;
using NetworkLib.Packets;
using StackExchange.Redis;
namespace ServerApp.Verification
{
    public class ServerVerification
    {
        private readonly IDatabase? _redisDb;
        private readonly HashSet<int> _activePlayers = new HashSet<int>() { 1, 2, 3, 4, 5, 6, 7, 8 }; // 1번 방에 참가한 플레이어 ID 집합 (예시)

        //key: (roomNo, turnNumber) -> value: (key : playerId -> value: hash)
        private readonly ConcurrentDictionary<(int roomNo, long turn), ConcurrentDictionary<int, string>> _turnHashReceipts
        = new ConcurrentDictionary<(int roomNo, long turn), ConcurrentDictionary<int, string>>();

        public ServerVerification(IDatabase? redisDb)
        {
            _redisDb = redisDb;
        }

        public async Task ProcessVerificationAsync(ReadOnlyMemory<byte> body, int playerId)
        {
            var packet = ServerVerificationPacket.Parser.ParseFrom(body.Span);
            SimpleLogger.LogServer("VERIFY_RECV",
            $"[Room {packet.RoomNo} | Turn {packet.TurnNumber}] Player {packet.PlayerId} 영수증 수신 (Hash: {packet.StateHash})");

            var key = (packet.RoomNo, (long)packet.TurnNumber);
            var receipts = _turnHashReceipts.GetOrAdd(key, _ => new ConcurrentDictionary<int, string>());
            receipts.TryAdd(packet.PlayerId, packet.StateHash);
            // 방 전체 인원의 영수증이 모두 모인 경우 Desync 검증 실행
            if (receipts.Count >= _activePlayers.Count)
            {
                bool isSynced = ValidateStateHashes(receipts ,out string commonHash);
                if (isSynced)
                {
                    SimpleLogger.LogServer("VERIFY_PASS", $"[Room {packet.RoomNo} | Turn {packet.TurnNumber}] 모든 플레이어 영수증 일치! (Combined Hash: {commonHash})");
                    await SaveReceiptToRedisAsync(packet.RoomNo, packet.TurnNumber, commonHash);
                }
                else
                {
                    SimpleLogger.LogWarning(
                    $"🚨 [DESYNC DETECTED] Room {packet.RoomNo} | Turn {packet.TurnNumber} 클라이언트 간 StateHash 불일치 발생!");
                }
            }
            // 검중 완료된 턴 메모리 해제
            _turnHashReceipts.TryRemove(key, out _);
        }

        /// <summary>
        /// 모든 플레이어의 StateHash를 비교하여 일치 여부를 검증합니다.
        /// </summary>
        private bool ValidateStateHashes(ConcurrentDictionary<int, string> receipts, out string commonHash)
        {

            // 1. 만약 수집된 해시가 없다면 실패 처리
            if (receipts.IsEmpty)
            {
                commonHash = string.Empty;
                return false;
            }

            // 2. 첫 번째 플레이어의 해시값을 기준 해시로 지정
            commonHash = receipts.Values.First();
            // 3. 모든 유저의 해시가 기준 해시와 동일한지 비교
            // (EqualityComparer<string>.Default를 사용하거나 Equals 연산으로 명확히 비교)
            string baseHash = commonHash;
            return receipts.Values.All(hash => string.Equals(hash, baseHash, StringComparison.Ordinal));
        }

        private async Task SaveReceiptToRedisAsync(int roomNo, long turnNumber, string stateHash)
        {
            if (_redisDb == null) return;

            try
            {
                string redisKey = $"room:{roomNo}:turn:{turnNumber}";
                string redisValue = $"VERIFIED|HASH:{stateHash}|TIME:{DateTime.UtcNow:O}";

                // 10분(600초) TTL 설정
                await _redisDb.StringSetAsync(redisKey, redisValue, TimeSpan.FromMinutes(10));
            }
            catch (Exception ex)
            {
                SimpleLogger.LogWarning($"Redis 저장 에러: {ex.Message}");
            }
        }

    }
}