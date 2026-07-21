using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf; // Protobuf 핵심 라이브러리
using NetworkLib.Diagnostics;
using NetworkLib.Packets; // 자동 생성된 패킷들

namespace ClientApp
{
    class Program
    {
        // 통신을 담당할 UDP 소켓
        private static UdpClient _udpClient = null!;

        // 내 정보 및 연결할 상대방 정보
        private static int _playerId;
        private static int _roomNo = 1;
        private static IPEndPoint _serverEndPoint = null!;
        private static IPEndPoint _peerEndPoint = null!;

        // 멀티스레드 정지용 토큰
        private static CancellationTokenSource _cts = new CancellationTokenSource();

        static async Task Main(string[] args)
        {
            Console.WriteLine("=== [Linstep] 하이브리드 UDP 클라이언트 기동 ===");

            // 1. 초기 세팅 (실제로는 서버에서 홀펀칭 결과를 받아와야 하지만, 테스트를 위해 임시 지정)
            Console.Write("내 플레이어 ID를 입력하세요 (1 또는 2): ");
            _playerId = int.Parse(Console.ReadLine() ?? "1");

            // 내 포트와 상대방 포트를 ID에 따라 다르게 설정 (로컬 한 컴퓨터 테스트용)
            int myPort = (_playerId == 1) ? 6001 : 6002;
            int peerPort = (_playerId == 1) ? 6002 : 6001;

            _udpClient = new UdpClient(myPort);
            _serverEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5001); // 검증 서버 주소
            _peerEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), peerPort); // 상대방 클라 주소

            Console.WriteLine($"[로컬] 내 포트: {myPort} | 상대방 포트: {peerPort} 세팅 완료.");

            // 2. 수신 스레드 가동 (받기 루프)
            _ = Task.Run(() => ReceiveLoopAsync(_cts.Token));

            // 3. 송신 스레드 가동 (P2P 루프 & 서버 검증 루프)
            _ = Task.Run(() => SendP2PLoopAsync(_cts.Token));
            _ = Task.Run(() => SendServerVerificationLoopAsync(_cts.Token));

            Console.WriteLine("통신 루프 가동 중... 종료하려면 Enter를 누르세요.");
            Console.ReadLine();

            // 종료 처리
            _cts.Cancel();
            _udpClient.Close();
            Console.WriteLine("클라이언트를 안전하게 종료합니다.");
        }

        /// <summary>
        /// 1. 패킷 수신 루프 (상대방 P2P 패킷과 서버 동기화 패킷을 동시에 받아 처리)
        /// </summary>
        private static async Task ReceiveLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    //1 수신 대기는 무조건 wile 루프안에서 안전하게 보호한다. 
                    var result = await _udpClient.ReceiveAsync(token);
                    byte[] data = result.Buffer;
                    // 2. p2p 조작 패킷 파싱 시도 
                    try
                    {
                        var p2pPacket = P2PInputPacket.Parser.ParseFrom(data);
                        if (p2pPacket.PlayerId != 0 && !string.IsNullOrEmpty(p2pPacket.Command))
                        {
                            SimpleLogger.LogClientP2P($"[P2P 수신] 상대방({p2pPacket.PlayerId}) 입력: {p2pPacket.Command} (프레임: {p2pPacket.CurrentFrame})");
                            continue; // 성공 시 다음 루프로
                        }
                    }
                    catch {/*P2p 패킷 포맷이 아니면 하단 통과*/ }

                    //3. 서버 동기화 패킷 파싱 시도 
                    try
                    {
                        var syncPacket = ServerSyncPacket.Parser.ParseFrom(data);
                        SimpleLogger.LogClientP2P($"[서버수신] 최종 동기화 턴 : {syncPacket.TurnNumber}");
                    }
                    catch{/*p2p 패킷 포멧이 아니면 하단으로 통과*/}
                }
                catch(ObjectDisposedException)
                {
                    break;// 소켓이 정상 종료된 경우 루프 탈출
                }
                catch(Exception ex)
                {
                    // 💡 핵심: 상대방이 안 켜져서 발생하는 10054 에러 등은 여기서 잡히며,
                    // 'break'나 'return'을 하지 않고 로그만 슬쩍 찍은 뒤 다음 수신을 계속 기다립니다(while 유지).
                    // SimpleLogger.LogClientP2P($"[수신 일시 오류(무시가능)]: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// 2. [P2P] 0.01초(10ms) 주기로 상대방에게 다이렉트 키 입력 전송
        /// </summary>
        private static async Task SendP2PLoopAsync(CancellationToken token)
        {
            long frameCounter = 0;
            string[] testCommands = { "w", "a", "s", "d" };
            Random rand = new Random();

            try
            {
                while (!token.IsCancellationRequested)
                {
                    // 가상의 키 조작 생성
                    string myInput = testCommands[rand.Next(testCommands.Length)];

                    // Protobuf 객체 생성
                    var p2pPacket = new P2PInputPacket
                    {
                        PlayerId = _playerId,
                        Command = myInput,
                        CurrentFrame = frameCounter++
                    };

                    // 직렬화 (Protobuf 규격 바이트로 변환)
                    byte[] sendBytes = p2pPacket.ToByteArray();

                    // 상대방에게 직접 발송 (서버를 거치지 않음!)
                    await _udpClient.SendAsync(sendBytes, sendBytes.Length, _peerEndPoint);

                    // 0.01초 대기
                    await Task.Delay(10, token);
                }
            }
            catch (TaskCanceledException) { }
        }

        /// <summary>
        /// 3. [서버 검증] 0.1초(100ms) 주기로 중앙 검증 서버(Redis 기록용)로 영수증 발송
        /// </summary>
        private static async Task SendServerVerificationLoopAsync(CancellationToken token)
        {
            long turnCounter = 0;
            try
            {
                while (!token.IsCancellationRequested)
                {
                    var verificationPacket = new ServerVerificationPacket
                    {
                        RoomNo = _roomNo,
                        PlayerId = _playerId,
                        TurnNumber = turnCounter++,
                        Command = "VerificationReceipt" // 검증용 메시지
                    };

                    byte[] sendBytes = verificationPacket.ToByteArray();

                    // 중앙 검증 서버로 발송
                    await _udpClient.SendAsync(sendBytes, sendBytes.Length, _serverEndPoint);

                    // 0.1초 대기
                    await Task.Delay(100, token);
                }
            }
            catch (TaskCanceledException) { }
        }
    }
}