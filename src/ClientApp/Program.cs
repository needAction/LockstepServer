using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

class ClientProgram
{
    private const string ServerIP = "127.0.0.1";
    private const int ServerPort = 5001;

    // 💡 내 정보들을 기억할 변수들입니다.
    private static int _myRoomNo;
    private static int _myPlayerId;
    private static string _targetIP;
    private static int _targetPort;

    static async Task Main(string[] args)
    {
        Console.WriteLine("=============================================");
        Console.WriteLine("   서버 보조형 락스텝 테스트 클라이언트 v1.2   ");
        Console.WriteLine("=============================================");

        UdpClient udpClient = new UdpClient(0);

        Console.Write("입장할 방 번호를 입력하세요 (숫자): ");
        if (!int.TryParse(Console.ReadLine(), out _myRoomNo)) return;

        try
        {
            // 1. 서버에 방 입장 요청하기
            byte[] sendData = Encoding.UTF8.GetBytes(_myRoomNo.ToString());
            await udpClient.SendAsync(sendData, sendData.Length, ServerIP, ServerPort);
            Console.WriteLine($"\n[입장 요청] {_myRoomNo}번 방 매칭을 기다리는 중...");

            // 2. 서버가 매칭 완료 후 보내준 주소 및 내 ID 정보 받기
            UdpReceiveResult result = await udpClient.ReceiveAsync();
            string jsonResponse = Encoding.UTF8.GetString(result.Buffer);

            using (JsonDocument doc = JsonDocument.Parse(jsonResponse))
            {
                JsonElement root = doc.RootElement;
                _targetIP = root.GetProperty("TargetIP").GetString();
                _targetPort = root.GetProperty("TargetPort").GetInt32();
                _myPlayerId = root.GetProperty("PlayerId").GetInt32();

                Console.WriteLine("\n=============================================");
                Console.WriteLine($"🎉 매칭 완료! 나는 [{_myPlayerId}번 플레이어] 입니다.");
                Console.WriteLine($"📍 P2P 상대방 주소: {_targetIP}:{_targetPort}");
                Console.WriteLine("---------------------------------------------");
                Console.WriteLine(" 조작 명령을 입력하세요 (예: w, a, s, d, attack 등)");
                Console.WriteLine("=============================================");

                // 🎧 [배경 수신 장치 가동] 서버가 주는 0.1초 메트로놈 패킷을 계속 귀 기울여 듣습니다.
                _ = Task.Run(() => StartServerSyncLoop(udpClient));

                // 3. 사용자의 입력을 실시간으로 받아서 서버로 쏘아 올리는 루프
                while (true)
                {
                    string inputCommand = Console.ReadLine();
                    if (string.IsNullOrEmpty(inputCommand)) continue;
                    if (inputCommand.ToLower() == "exit") break;

                    // 서버가 정해둔 하이브리드 규격에 맞게 JSON 패킷을 포장합니다.
                    var inputPacket = new
                    {
                        RoomNo = _myRoomNo,
                        PlayerId = _myPlayerId,
                        Command = inputCommand
                    };

                    string jsonInput = JsonSerializer.Serialize(inputPacket);
                    byte[] inputBytes = Encoding.UTF8.GetBytes(jsonInput);

                    // 💡 중요: 패킷을 상대방이 아니라 '서버(심판)'에게 보냅니다!
                    await udpClient.SendAsync(inputBytes, inputBytes.Length, ServerIP, ServerPort);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[에러 발생] {ex.Message}");
        }
    }

    // ⏱️ 서버가 0.1초마다 방송해 주는 '최종 결과 통보'를 수신하는 함수
    private static async Task StartServerSyncLoop(UdpClient udpClient)
    {
        try
        {
            while (true)
            {
                UdpReceiveResult syncResult = await udpClient.ReceiveAsync();
                string jsonSync = Encoding.UTF8.GetString(syncResult.Buffer);

                // 만약 서버에서 보낸 턴 동기화 데이터가 맞다면 화면에 뿌려줍니다.
                if (jsonSync.Contains("TurnNumber"))
                {
                    using (JsonDocument doc = JsonDocument.Parse(jsonSync))
                    {
                        JsonElement root = doc.RootElement;
                        long turnNo = root.GetProperty("TurnNumber").GetInt64();
                        JsonElement playerInputs = root.GetProperty("PlayerInputs");

                        // 0번 유저와 1번 유저의 조작 내역을 뜯어봅니다.
                        string move0 = playerInputs.GetProperty("0").GetString();
                        string move1 = playerInputs.GetProperty("1").GetString();

                        // 0.1초마다 너무 많이 찍히면 정신없으니, 두 유저 중 한 명이라도 입력을 했을 때만 화면에 출력합니다.
                        if (move0 != "None" || move1 != "None")
                        {
                            Console.WriteLine($"\n[⏱️ 락스텝 턴 {turnNo}] 0번유저: {move0} | 1번유저: {move1}");
                        }
                    }
                }
            }
        }
        catch { /* 종료 시 예외 무시 */ }
    }
}