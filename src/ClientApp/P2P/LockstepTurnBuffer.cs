using System;
using System.Collections.Concurrent;
using NetworkLib.Diagnostics;
using NetworkLib.Packets;

namespace ClientApp.P2P
{
    /// <summary>
    /// P2P로 수신된 개별 유저 입력을 턴(Turn) 단위로 모아 통합 턴 프레임(TurnFrame)을 구성하는 락스텝 완충 버퍼입니다.
    /// </summary>
    public class LockstepTurnBuffer
    {
        private readonly int _totalPlayers;
        // Key: TurnNumber -> Value: (Key: PlayerId -> Value: GameInputPacket)
        private readonly ConcurrentDictionary<long, ConcurrentDictionary<int, GameInputPacket>> _turnInputBuffer = new();

        /// <summary>
        /// 락스텝 버퍼상 생성자
        /// </summary>
        /// <param name="totalPlayers"> 세션에 참가한 총 플레이어 수 </param>
        public LockstepTurnBuffer(int totalPlayers)
        {
            _totalPlayers = totalPlayers;
        }

        /// <summary>
        /// P2P 네트워크를 통해 수신된 개별 유저의 입력을 버퍼에 등록합니다.
        /// </summary>
        /// <param name="turnNumber"></param>
        /// <param name="playerId"></param>
        /// <param name="command"></param>
        public void AddInput(GameInputPacket input)
        {
            var playerInputs = _turnInputBuffer.GetOrAdd(input.TurnNumber, _ => new ConcurrentDictionary<int, GameInputPacket>());
            // 동일 플레이어의 해당 턴 입력 중복 처리 방지 (최초 입력 유지 )
            playerInputs.TryAdd(input.PlayerId, input);
        }
        ///
        /// <summary>
        /// 특정 턴에 대해 모든 플레이어의 입력이 수신되었는지 확인합니다.
        /// </summary>   
        public bool IsTurnReady(long targetTurn)
        {
            if (_turnInputBuffer.TryGetValue(targetTurn, out var playerInputs))
            {
                return playerInputs.Count >= _totalPlayers;
            }
            return false;
        }

        public TurnFramePacket? ConsumeTurnFrame(long targetTurn)
        {
            if (!IsTurnReady(targetTurn))
            {
                SimpleLogger.LogClientP2P("LOCKSTEP", $"[TurnFrame 미완성] 턴 {targetTurn}은 아직 모든 플레이어 입력이 수신되지 않았습니다.");
                return null;
            }
            if (_turnInputBuffer.TryRemove(targetTurn, out var playerInputs))
            {
                var framePacket = new TurnFramePacket
                {
                    TurnNumber = (int)targetTurn
                };
                // PlayerId 순서대로 정렬하여 TurnFramePacket에 입력을 추가
                foreach (var kvp in playerInputs.OrderBy(x => x.Key))
                {
                    framePacket.Inputs.Add(kvp.Value);
                }
                SimpleLogger.LogClientP2P("LOCKSTEP", $"[TurnFrame 생성] 턴 {targetTurn}에 대한 통합 입력 프레임이 생성되었습니다.");
                return framePacket;
            }
            return null;
        }
        public List<int> GetMissingPlayers(long targetTurn)
        {

            var missing = new List<int>();

            _turnInputBuffer.TryGetValue(targetTurn, out var playerInputs);

            for (int pId = 1; pId <= _totalPlayers; pId++)
            {
                if (playerInputs == null || !playerInputs.ContainsKey(pId))
                {
                    missing.Add(pId);
                }
            }
            return missing;
        }
    }
}