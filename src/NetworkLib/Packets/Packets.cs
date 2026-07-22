// using System;
// using System.Collections.Generic;

// namespace NetworkLib
// {
//     // 💡 패킷의 종류가 무엇인지 알려주는 번호판(형태)입니다.
//     public enum PacketType
//     {
//         GameStart = 1,  // 게임 시작 시그널
//         TurnInput = 2,  // 유저가 보낸 입력 패킷
//         TurnSync = 3    // 서버가 모두에게 뿌리는 동기화 패킷
//     }

//     // -------------------------------------------------------------
//     // [1번 패킷] "이제 8명 다 들어왔으니 게임 시작합니다!" 알림
//     // -------------------------------------------------------------
//     public class MsgGameStart
//     {
//         public PacketType Type => PacketType.GameStart;
//         public int TotalPlayers { get; set; } // 총 참여 인원 (최대 8명)
//         public int MyPlayerId { get; set; }   // 내가 몇 번 플레이어인지 (0~7)
//     }

//     // -------------------------------------------------------------
//     // [2번 패킷] 유저가 "나 이번 턴에 이 조작 했어" 하고 보내는 패킷
//     // -------------------------------------------------------------
//     public class MsgTurnInput
//     {
//         public PacketType Type => PacketType.TurnInput;
//         public int PlayerId { get; set; }   // 내 플레이어 번호
//         public long TurnNumber { get; set; } // 현재 몇 번째 턴(프레임)인가? (예: 124번째 턴)
        
//         // 유저가 누른 조작 데이터 (예: "MoveUp", "Skill_1", "Stop" 등)
//         // 나중에 락스텝이 정교해지면 키보드 배열이나 좌표값(X, Y)이 들어갑니다.
//         public string InputCommand { get; set; } = null!; 
//     }

//     // -------------------------------------------------------------
//     // [3번 패킷] 락스텝의 핵심! 서버가 8명의 입력을 모아서 모두에게 뿌리는 패킷
//     // -------------------------------------------------------------
//     public class MsgTurnSync
//     {
//         public PacketType Type => PacketType.TurnSync;
//         public long TurnNumber { get; set; } // 이번에 동기화할 턴 번호

//         //  중요: 이 턴에 "모든 플레이어(최대 8명)가 입력한 조작들"을 한 바구니에 담아서 보냅니다.
//         // Key: 플레이어 번호(int), Value: 그 플레이어의 입력 내용(string)
//         public Dictionary<int, string> PlayerInputs { get; set; } = new Dictionary<int, string>();
//     }

//     // 1.대기실 입장할 때 서버에 던지는 방 번호 패킷
//     public class RooomJoinPacket
//     {
//         public int RoomNo{get; set;}

//     }
//     //2 서버가 홀펀칭 성공시 양쪽 클라이언트에게 주는 주소록 패킷
//     public class PunchResultPacket
//     {
//         public string TargetIP{get; set;} = null!;
//         public int TargetPort {get; set;}
//         public int PlayerId{get; set;}
//     }
//     //3. [p2p 직접 전송용] 클라끼리 직접 초고속(0.01초)으로 주고 받을 조작 패킷
//     public class P2PInputPacket
//     {
//         public int PlayerId{get; set;}
//         public string Command{get; set;} = null!; // w a s d  같은 입력어
//         public long CurrnentFrame {get; set;} // 랜더링 동기화용 프레임 번호
//     }
//     // 4. [서버 검증용] 서버 Redis로 보낼 영수증 패킷
//     public class ServerVerificationPacket
//     {
//         public int RoomNo{get; set;} 
//         public int PlayerId{get; set;}
//         public long TurnNumber{get; set;}
//         public string Command { get; set;} = null!;
//     }
//     //5. 서버가 0.1초마다 모두에게 최종 확정해서 뿌려주는 동기화 패킷
//     public class ServerSyncPacket
//     {
//         public long TurnInput {get; set;} 
//         // 플레이어 ID별 최종 확정 조작 (예: {0:"w", 1:"None"})
//         public Dictionary<int, string> PlayerInputs {get; set;} = null!;
//     }
// }