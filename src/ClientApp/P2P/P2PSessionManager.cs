using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Google.Protobuf;
using NetworkLib.Packets;
using NetworkLib.Diagnostics;


namespace ClientApp.P2P;

public class P2PSessionManager
{
    private readonly int _myPlayerId;
    private readonly UdpClient _udpClient;
    
    // 현재 세션에 존재하는 나를 제외한 Peer 목록 (PlayerId -> IPEndPoint)
    private readonly ConcurrentDictionary<int, IPEndPoint> _peers = new();

    public P2PSessionManager(int myPlayerId, UdpClient udpClient)
    {
        _myPlayerId = myPlayerId;
        _udpClient = udpClient;
    }

    /// <summary>
    /// 동적으로 Peer를 등록합니다 (예: 방 입장 / 매칭 성공 시).
    /// </summary>
    public void AddPeer(int playerId, string ip, int port)
    {
        if (playerId == _myPlayerId) return; // 나 자신은 피어 목록에 넣지 않음

        var endPoint = new IPEndPoint(IPAddress.Parse(ip), port);
        _peers[playerId] = endPoint;
        SimpleLogger.LogClientP2P("P2P_SESSION", $"[Peer 등록] Player {playerId} ({endPoint})");
    }

    /// <summary>
    /// 피어가 나가거나 튕겼을 때 피어 목록에서 제거합니다.
    /// </summary>
    public void RemovePeer(int playerId)
    {
        if (_peers.TryRemove(playerId, out var endPoint))
        {
            SimpleLogger.LogClientP2P("P2P_SESSION", $"[Peer 제거] Player {playerId} ({endPoint})");
        }
    }

    /// <summary>
    /// [1:N Broadcast] 나를 제외한 모든 활성 Peer들에게 내 입력 패킷을 동시에 전송합니다.
    /// </summary>
    public async Task BroadcastAsync(GameInputPacket inputPacket)
    {
        byte[] data = inputPacket.ToByteArray();

        // 등록된 N명의 Peer 목록을 루프 돌며 비동기 전송
        foreach (var (peerId, endPoint) in _peers)
        {
            try
            {
                await _udpClient.SendAsync(data, data.Length, endPoint);
            }
            catch (Exception ex)
            {
                SimpleLogger.LogWarning($"[P2P Send Error] Player {peerId} ({endPoint}) 전송 실패: {ex.Message}");
            }
        }
    }

    public int ActivePeerCount => _peers.Count;
}