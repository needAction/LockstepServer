using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;
using NetworkLib.Diagnostics;

namespace NetworkLib.Dispatcher
{
    public class PacketDispatcher
    {
        // 패킷 ID(int) -> 해당 패킷을 처리할 비동기 액션(func) 매핑 테이블
        private readonly ConcurrentDictionary<int, Func<byte[], int , Task>>_handlers = new();
        /// <summary>
        /// 특정 패킷 ID 와 Protobuf 파서를 등록 
        /// <summary>
        public void RegisterHandler<T>(int packetId, MessageParser<T> parser, Func<T, int, Task> handler)  where T : IMessage<T>
        {
            _handlers[packetId] = async (data, playerId) =>
            {
                // Zero-Allocation/ Safe Parsing
                T packet = parser.ParseFrom(data);
                await handler(packet,playerId);
            };
            SimpleLogger.LogServer("DISPATCHER", $"패킷 핸들러 등록 완료 ID ={packetId} Type = {typeof(T).Name}");
        }

        /// <summary>
        /// 수신된 패킷 ID를 보고 적절한 핸들러로 배달(Dispatch)합니다.
        /// </summary>
        /// <returns></returns>
        public async Task DispatchAsync(int packetId, byte[] packetData, int playerId)
        {
            _
        }
    }
}