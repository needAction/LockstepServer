using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace NetworkLib.Dispatcher
{
    public interface IPacketHandler
    {
        // 수신된 패킷 데이터를 처리하는 공통 메서드
        Task HandleAsync(byte[] PacketData, int playerId);
    }
}