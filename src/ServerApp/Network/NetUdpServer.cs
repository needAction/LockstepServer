using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;


namespace ServerApp.Network
{
    public class NetUdpServer
    {
        private UdpClient _udpListener = null!;
        private readonly int _port;

        public NetUdpServer(int port)
        {
            _port = port;

        }

        public async Task StartAsync()
        {
            // udp는 내 컴퓨터의 모든 IP, 지정된 포트로 들어오는 패킷을 대기함
            _udpListener = new UdpClient(_port);
            Console.WriteLine($"[udp 서버] 포트 {_port} 번에서 락스텝/홀펀칭 시그널링 대기 증");

            while (true)
            {
                // ReceiveAsync 는 패킷이 올 때까지 대기하며 송신자의 포트(RemoteEndPoint)를 함께 캡처합니다. 
                // 이 'RemoteEndPoint'를 알아내는 것이 홀펀칭의 핵심입니다!
                UdpReceiveResult result = await _udpListener.ReceiveAsync();

                string message = Encoding.UTF8.GetString(result.Buffer);
                IPEndPoint clientEndPoint = result.RemoteEndPoint;

                Console.WriteLine($"[USP 수신] {clientEndPoint}로 패킷 도착 :{message} ");

                // 예시 : 받은 패키슬 그대로 돌려주는 (Echo) 기능 또는 홀펀칭 주소 반환
                string response = $"YourAddress:{clientEndPoint.Address}:{clientEndPoint.Port}";
                byte[] responseData = Encoding.UTF8.GetBytes(response);

                // UDP는 연결이 없기 때문에 보낼 때마다 목적지(clientEndPoint)를 항상 지정해 줘야 합니다.
                await _udpListener.SendAsync(responseData, responseData.Length, clientEndPoint);
            }
        }
    }
}