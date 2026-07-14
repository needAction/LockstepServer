using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using StackExchange.Redis;
using ServerApp.Database;
using System.Net.NetworkInformation;
namespace ServerApp.Network
{
    public class UdpPunchServer
    {
        private readonly UdpClient _udpListener;
        private readonly int _port;

        public UdpPunchServer(int port)
        {
            _port = port;
            _udpListener = new UdpClient(_port);
        }

        public async Task StartAsync()
        {
            Console.WriteLine($"[UDP 서버] 포트 {_port}에서 서버 보조형 락스텝 엔진 가동...");

            while (true)
            {
                UdpReceiveResult result = await _udpListener.ReceiveAsync();
                IPEndPoint clientEndPoint = result.RemoteEndPoint;
                string clientMessage = Encoding.UTF8.GetString(result.Buffer);

                // 💡 만약 게임 중에 유저가 보낸 "조작 패킷(JSON)"이라면?
                if (clientMessage.StartsWith("{"))
                {
                    _ = HandleGameInputAsync(clientMessage);
                    continue;
                }

                // 💡 게임 중이 아니라면 기존처럼 "방 입장 요청(숫자)"으로 처리합니다.
                if (int.TryParse(clientMessage, out int roomNo))
                {
                    await HandleRoomJoin(roomNo, clientEndPoint);
                }
            }
        }

        // 1️ 방 입장 및 홀펀칭 주소 교환 로직
        private async Task HandleRoomJoin(int roomNo, IPEndPoint clientEndPoint)
        {
            var db = RedisManager.Db;// 레디스 통로 연결
            string redisKey = $"room:{roomNo}:players";
            string playerAddress = clientEndPoint.ToString();

            // 1. 레디스의 'Set' 구조에 유저 주소를 저장합니다.  (중복 방지도 자동) 
            await db.SetAddAsync(redisKey, playerAddress);

            //2. 현재 방에 몇명이 모였는지 레디스에 물어봅니다.
            long playerCount = await db.SetLengthAsync(redisKey);
            Console.WriteLine($"[입장] 유저 {playerAddress} -> Redis {roomNo}번 방 (현재 {playerCount}명)");
            // 테스트를 위해 2명이 모이면 게임 시작! (나중에 8명으로 확장 가능)
            if (playerCount == 2)
            {
                //3 레디스에서 방에 있는 유저 주소들을 싹다 긁어 옵니다. 
                var member = await db.SetMembersAsync(redisKey);
                string userAStr = member[0].ToString();
                string userBStr = member[1].ToString();

                //4 글자 주서를 진짜 주소(IPEndPoint) 객체로 조립
                IPEndPoint userAEndPoint = IPEndPoint.Parse(userAStr);
                IPEndPoint userBEndPoint = IPEndPoint.Parse(userBStr);

                //5. [복구 완료] 서로에게 교환 해 줄 Json 데이터를 패키징 합니다. 
                var infoForA = new { TargetIP = userBEndPoint.Address.ToString(), TargetPort = userBEndPoint.Port, PlayerId = 0 };
                var infoForB = new { TargetIP = userAEndPoint.Address.ToString(), TargetPort = userAEndPoint.Port, PlayerId = 1 };
                // 이후 주소교환(시그널링) 및 st
                await SendJsonAsync(infoForA, userAEndPoint);
                await SendJsonAsync(infoForB, userBEndPoint);

                Console.WriteLine($"[시그널링] {userAStr} -> {userBStr} 주소 맞교환 완료");

                // 💡 [핵심] 주소 교환이 끝나자마자 서버 보조형 "락스텝 타이머"를 배경에서 출발시킵니다!
                _ = Task.Run(() => StartGameLoop((roomNo), userAEndPoint, userBEndPoint));
            }
        }

        //  (락스텝 게임 루프)
        private async Task StartGameLoop(int roomNo, IPEndPoint userA, IPEndPoint userB)
        {
            var db = RedisManager.Db;
            long currentTurn = 0;

            Console.WriteLine($"[락스텝 시작] {roomNo}번 방 게임 루프 가동");

            while (true)
            {
                // ⏱️ 0.1초(100ms) 명령문 모으는 시간 간격
                await Task.Delay(100);

                currentTurn++; // 턴 증가! (예: 1번째 턴)

                // redis에서 이번 턴에 쌓인 입력 해시맵 데이터째로 긁어오기 
                var inputKey = $"room:{roomNo}:turn:{currentTurn}";
                var redisInputs = await db.HashGetAllAsync(inputKey);

                //C# 딕셔너리로 변환하여 전송데이터 구조화
                var currentInput = new Dictionary<int, string>();
                foreach (var entry in redisInputs)
                {
                    if (int.TryParse(entry.Name.ToString(), out int pId))
                    {
                        currentInput[pId] = entry.Value.ToString();
                    }
                }

                // 만약 유저가 인터넷이 렉이 심해 못보냈다면 서버가 None으로 수정
                if (!currentInput.ContainsKey(0)) currentInput[0] = "None";
                if (!currentInput.ContainsKey(1)) currentInput[1] = "None";

                // 모든 유저에게 "이번 턴 최종 결과판"을 만들어 브로드캐스팅(전체 전송)합니다.
                var syncPacket = new
                {
                    TurnNumber = currentTurn,
                    PlayerInputs = currentInput // { {0, "MoveUp"}, {1, "None"} } 모양
                };

                // 💡 [수정 완료] Undefined 에러가 나던 _rooms 대신, 매개변수로 전달받은 두 유저에게 확실히 전송합니다.
                await SendJsonAsync(syncPacket, userA);
                await SendJsonAsync(syncPacket, userB);

                // 🧹 보관이 끝난 지나간 턴 데이터는 Redis 메모리 관리를 위해 깔끔하게 지워줍니다.
                await db.KeyDeleteAsync(inputKey);
            }
        }

        // 유저가 보낸 패킷을 바구니에 담는 로직
        private async Task HandleGameInputAsync(string jsonMessage)
        {
            try
            {
                using (JsonDocument doc = JsonDocument.Parse(jsonMessage))
                {
                    JsonElement root = doc.RootElement;
                    int roomNo = root.GetProperty("RoomNo").GetInt32();
                    int playerId = root.GetProperty("PlayerId").GetInt32();
                    string? command = root.GetProperty("Command").GetString();

                    // 클라이언트가 현재 플레이 중인 턴 번호도 같이 패킷에 실어 보낸다고 가정합니다.
                    // 만약 패킷에 턴 번호가 없다면, 서버가 현재 흘려보내는 추정 턴이나 공통 관리 키를 써야 합니다.
                    // 안전한 테스트를 위해 임시로 다음 턴을 예측해 저장하거나 단일 키 구조로 쌓습니다.
                    // 여기서는 유저가 보낸 입력 패킷에 'TurnNumber'가 포함되어 있다고 구현하는 것이 락스텝의 정석입니다.
                    long clientTurn = root.TryGetProperty("TurnNumber", out var turnNode) ? turnNode.GetInt64() : 1;

                    var db = RedisManager.Db;
                    string inputKey = $"room:{roomNo}:turn:{clientTurn}";

                    // 💡 [핵심] C# 메모리 대신 Redis의 Hash 구조에 "플레이어번호 - 입력명령"을 저장합니다!
                    await db.HashSetAsync(inputKey, playerId.ToString(), command);
                }
            }
            catch { /* 패킷 에러 무시 네트워크 패킷 유실등... 일단 무시하고 추후 로그만 쌓아보자  */ }
        }

        // JSON 데이터를 편하게 보내기 위한 헬퍼 함수
        private async Task SendJsonAsync(object data, IPEndPoint target)
        {
            string json = JsonSerializer.Serialize(data);
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            await _udpListener.SendAsync(bytes, bytes.Length, target);
        }
    }
}