using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Security.Cryptography;

namespace ClientApp.Utils
{
    /// <summary>
    /// 락스텝  턴 진행 후 게임 월드의 무결성을 검증하기 위한 핑거프린드(해시) 생성기
    /// </summary>
    public class StateHashGenerator
    {
        /// <summary>
        /// 턴 번호, 플레이어어의 위치 /상태 문자열을 기반으로 8자리 Hex CRC32/SHA256 Checksum 해시를 산출합니다.
        /// <param name="turnNumber">현재 연산 완료된 턴 번호</param>
        /// <param name="gameStateSummary">현재 클라이언트의 게임 월드 렌더링/물리 상태 요약 (예: "P1:10,20|P2:30,40")</param>
        /// <returns>동기화 검증용 8자리 해시 문자열 (예: "A1F3C9E2")</returns>
        public static string GenerateStateHash(long turnNumber, string gameStateSummary)
        {
            // 턴 번호 + 상태 데이터를 결합한 원본 문자열
            string rawData = $"TURN:{turnNumber}|STATE:{gameStateSummary}";

            byte[] bytes = Encoding.UTF8.GetBytes(rawData);
            byte[] hashBytes = SHA256.HashData(bytes);
            return Convert.ToHexString(hashBytes,0,4); // 앞 4바이트만 잘라서 8자리 Hex 문자열로 변환(경량화)
        }

    }
}