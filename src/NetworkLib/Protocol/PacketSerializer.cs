using Google.Protobuf;
using NetworkLib.Packets;

namespace NetworkLib.Protocol;

/// <summary>
/// 모든 네트워크 패킷의 [Header + Body] 바이너리 포장 및 해제를 담당하는 정적 유틸리티 클래스입니다.
/// </summary>
public static class PacketSerializer
{
    /// <summary>
    /// 헤더의 고정 크기 (Bytes): PacketId(int, 4 Bytes) + BodySize(int, 4 Bytes) = 총 8 Bytes
    /// </summary>
    public const int HeaderSize = 8;

    /// <summary>
    /// Protobuf 메시지(Body)에 8바이트 헤더(Header)를 붙여 완전한 네트워크 패킷 바이너리로 포장(직렬화)합니다.
    /// </summary>
    /// <param name="packetId">패킷 식별자 ID (예: 1 = Verification, 2 = GameInput)</param>
    /// <param name="body">Protobuf로 생성된 패킷 객체</param>
    /// <returns>헤더와 바디가 결합된 완전한 byte[] 배열</returns>
    /// 
    
    public static byte[] Serialize(PacketType packetType, IMessage body)
    {
        // Enum PacketType을 int로 변환하여 serialize 메서드 호출
        return Serialize((int)packetType, body);
    }

    /// <summary>
    /// [기존] 정수형 packetId 기반 직렬화 메서드
    /// </summary>
    public static byte[] Serialize(int packetId, IMessage body)
    {
        // 1. Protobuf 객체를 이진 데이터(byte[])로 변환합니다.
        byte[] bodyBytes = body.ToByteArray();
        
        // 2. 바디 데이터의 실제 길이를 구합니다.
        int bodySize = bodyBytes.Length;

        // 3. 헤더(8바이트) + 바디(bodySize바이트) 크기만큼의 전체 패킷 버퍼를 할당합니다.
        byte[] fullPacket = new byte[HeaderSize + bodySize];

        // 4. [Header 영역 - 0~3바이트] PacketId(int)를 4바이트 16진수 바이너리로 변환하여 패킷 맨 앞에 복사합니다.
        BitConverter.GetBytes(packetId).CopyTo(fullPacket, 0);

        // 5. [Header 영역 - 4~7바이트] BodySize(int)를 4바이트 16진수 바이너리로 변환하여 패킷 4번째 인덱스 위치에 복사합니다.
        BitConverter.GetBytes(bodySize).CopyTo(fullPacket, 4);

        // 6. [Body 영역 - 8바이트부터 끝까지] Protobuf 바디 바이너리 데이터를 헤더 바로 뒤(인덱스 8)에 복사합니다.
        Array.Copy(bodyBytes, 0, fullPacket, HeaderSize, bodySize);

        // 7. 포장이 완료된 완성형 패킷을 반환합니다.
        return fullPacket;
    }

    /// <summary>
    /// 수신한 RAW 바이너리에서 헤더를 해석하여 (PacketType Enum, Body) 형태로 분리합니다.
    /// </summary>
    /// <param name="rawData">소켓에서 수신된 RAW 바이트 Span</param>
    /// <returns>(PacketType Enum, Body 메모리 영역) 튜플</returns>
    public static (PacketType Type, ReadOnlyMemory<byte> Body) DeserializeEnum(ReadOnlySpan<byte> rawData)
    {
        // 정수형 Deserialize를 호출한 뒤 int를 PacketType Enum으로 변환합니다.
        var (packetId, body) = Deserialize(rawData);
        return ((PacketType)packetId, body);
    }
    

    /// <summary>
    /// 소켓으로부터 수신한 RAW 바이너리 데이터에서 헤더를 해석하여 PacketId와 Body 영역을 분리(역직렬화)합니다.
    /// </summary>
    /// <param name="rawData">소켓에서 읽어온 순수 바이트 데이터 (Span 구조로 메모리 복사 최소화)</param>
    /// <returns>분리된 (PacketId, Body 메모리 영역) 튜플</returns>
    public static (int PacketId, ReadOnlyMemory<byte> Body) Deserialize(ReadOnlySpan<byte> rawData)
    {
        // 1. 최소한 헤더 크기(8바이트)보다는 큰 데이터가 들어왔는지 유효성을 검사합니다.
        if (rawData.Length < HeaderSize)
        {
            throw new ArgumentException($"[수신 에러] 패킷 크기가 최소 헤더 크기({HeaderSize} Bytes)보다 작습니다.");
        }

        // 2. [Header 해석 - 0~3바이트] 앞의 4바이트를 정수형(int)으로 읽어 오리지널 PacketId를 복원합니다.
        int packetId = BitConverter.ToInt32(rawData.Slice(0, 4));

        // 3. [Header 해석 - 4~7바이트] 다음 4바이트를 정수형(int)으로 읽어 실제 Body 데이터의 크기를 복원합니다.
        int bodySize = BitConverter.ToInt32(rawData.Slice(4, 4));

        // 4. 수신된 전체 패킷 길이가 (헤더 크기 + 바디 크기)보다 작다면 패킷이 도중에 짤린 것입니다.
        if (rawData.Length < HeaderSize + bodySize)
        {
            throw new InvalidOperationException($"[수신 에러] 패킷 데이터가 짤렸습니다. Expected: {HeaderSize + bodySize}, Received: {rawData.Length}");
        }

        // 5. [Body 추출] 헤더(8바이트) 이후부터 bodySize만큼의 바디 데이터를 메모리 복사 없이 슬라이싱하여 반환합니다.
        ReadOnlyMemory<byte> bodyMemory = rawData.Slice(HeaderSize, bodySize).ToArray();

        return (packetId, bodyMemory);
    }
}