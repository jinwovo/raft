# raft — 밑바닥부터 구현한 Raft 합의 · 결정론적 검증 · 인터랙티브 시각화

분산 시스템의 핵심인 **합의(consensus)** 를 라이브러리 없이 직접 구현하고, **결정론적 카오스 시뮬레이션**으로 안전성을 증명하고, **브라우저에서 직접 망가뜨릴 수 있는 비주얼라이저**로 보여줍니다.

> 자매 프로젝트 **weave** 가 *최종 일관성*(CRDT) 쪽이라면, raft 는 *강한 일관성*(합의) 쪽입니다.
> realtime-messaging=전달 · weave=수렴 · **raft=합의**.

*(English: [README.md](README.md))*

## 무엇인가

머신이 죽고, 시계가 흔들리고, 네트워크가 메시지를 잃고·재배열하고·중복하고·분할하는 와중에도 N개의 노드가 **하나의 순서화된 로그에 합의**하게 만드는 것. Raft(Ongaro & Ousterhout, 2014)를 from-scratch로 구현했습니다.

## 핵심 (`raft-core` — 순수 자바, 프로덕션 의존성 0)

- **리더 선출 · 로그 복제 · 커밋 안전성** (§5) — current-term 직접커밋 규칙으로 Figure-8 덮어쓰기 버그 방지
- **pre-vote** (§9.6) — 파티션된 노드의 term 인플레이션 방지 → 재합류해도 멀쩡한 리더가 안 깨짐
- **로그 압축 / 스냅샷 + InstallSnapshot** (§7) — 로그 무한증가 방지, 너무 뒤처진 팔로워를 한 방에 따라잡기
- **선형화 읽기 (ReadIndex)** (§6.4) — 파티션된 stale 리더는 읽기를 확정하지 못함
- **동적 멤버십 변경** (§6) — 설정을 로그 엔트리로, 노드 추가/제거를 라이브로

설계의 핵심은 노드가 **I/O·스레드·실시계가 없는 결정론적 상태함수**(주입식 시계 + 아웃바운드 메시지 싱크)라는 점입니다. 덕분에 *같은 코어*가 시뮬레이션·라이브 서버·실네트워크에서 그대로 돕니다 (ADR-0001).

## 증명 (말이 아니라 실행으로)

- **결정론적 카오스 DST** (FoundationDB/TigerBeetle 스타일) — 시드 하나로 전 실행이 재현됩니다. 파티션·노드프리즈·지연·재배열·드롭·중복 속에서 **매 틱 안전성 불변식**(텀당 리더 ≤ 1, 커밋 히스토리 무발산)을 검증하고 힐 후 전 노드 수렴을 단언합니다. **150시드 그린**, 모든 깊은 변경(로그 재인덱싱·동적 설정)에도 회귀 없음.
- **실네트워크 멀티프로세스** — 3개의 별도 JVM이 실제 HTTP로 선출·복제·리더 페일오버 (`RealNetworkConvergenceTest` + `scripts/raft-cluster-test.ps1`).
- raft-core 15 + 앱 6 = **21 테스트**.

## 데모

![raft](docs/demo/raft.gif)

복제 → 네트워크 분할(고립된 소수파는 candidate로만 스핀 = split-brain 없음) → 힐 → 전 로그 재수렴. 비주얼라이저에서 노드를 클릭해 얼리고, 망을 가르고, pre-vote·압축·멤버십을 라이브로 토글할 수 있습니다.

## 빠른 시작

```bash
./gradlew test                        # 합의 스펙 + 카오스 시뮬 + 실HTTP 3노드 테스트
java -jar app/build/libs/app-*.jar    # 백엔드 :8104
cd web && npm install && npm run dev   # 비주얼라이저 http://localhost:3010
pwsh scripts/raft-cluster-test.ps1    # 별도 JVM 3개로 실제 클러스터 (선출→복제→페일오버)
```

## 스택

Java 21 · Spring Boot 4.1 · Next.js 15 · jqwik · Gradle. `raft-core` 는 프로덕션 의존성 0.
설계 결정은 [docs/adr](docs/adr) 참고.
