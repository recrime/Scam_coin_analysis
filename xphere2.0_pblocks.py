# xphere2.0_proof_blocks.py
# proof blocks 데이터 2차 스캔 방식으로 수집
import requests
import pandas as pd
from datetime import datetime
import time

def fetch_proof_blocks_in_batches(existing_proof_ids=None):
    if existing_proof_ids is None:
        existing_proof_ids = set()
        is_second_scan = False
    else:
        is_second_scan = True

    collected_proofs = []
    page = 1
    limit = 100

    while True:
        try:
            params = {'page': page, 'limit': limit}
            response = requests.get(api_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            rows = data.get('rows', []) if 'rows' in data else data.get('proofs', [])

            if not rows:
                scan_type = "2차 (누락분 확인)" if is_second_scan else "1차"
                print(f"페이지 {page}에서 더 이상 데이터가 없어 {scan_type} 스캔을 종료합니다.")
                break

            new_found_count = 0
            for proof in rows:
                proof_id = proof.get('proofId') or proof.get('id')
                if proof_id not in existing_proof_ids:
                    collected_proofs.append(proof)
                    if is_second_scan:
                        existing_proof_ids.add(proof_id)
                    new_found_count += 1

            if is_second_scan:
                print(f"페이지 {page} 완료 (새로 발견된 누락 데이터: {new_found_count}건)")
            else:
                print(f"페이지 {page} 완료 (총 {len(collected_proofs)}건 수집)")

            page += 1
            time.sleep(0.02)

        except requests.exceptions.RequestException as e:
            print(f"페이지 {page} 요청 중 오류 발생: {e}")
            break
        except Exception as e:
            print(f"알 수 없는 오류 발생: {e}")
            break
    return collected_proofs

if __name__ == "__main__":
    api_url = "https://xp.tamsa.io/xphere/api/v1/proof"
    now = datetime.now()
    filename = f"pblocks_{now.strftime('%Y%m%d_%H%M%S')}.csv"

    print("--- 1차 전체 proof blocks 데이터 스캔을 시작합니다. ---")
    initial_proofs = fetch_proof_blocks_in_batches()
    seen_proof_ids = set()
    for proof in initial_proofs:
        proof_id = proof.get('proofId') or proof.get('id')
        seen_proof_ids.add(proof_id)

    if initial_proofs:
        df = pd.DataFrame(initial_proofs)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n✅ 1차 스캔 완료. {len(initial_proofs)}개의 데이터가 '{filename}' 파일에 저장되었습니다.")
    else:
        print("\n⚠️ 1차 스캔에서 수집된 데이터가 없습니다. 프로그램을 종료합니다.")
        exit()

    print(f"\n--- 2차 스캔을 시작합니다. 1차 스캔 동안 추가/변경된 데이터를 확인합니다. ---")
    missing_proofs = fetch_proof_blocks_in_batches(existing_proof_ids=seen_proof_ids)

    if missing_proofs:
        print(f"\n✅ 2차 스캔 완료. {len(missing_proofs)}개의 누락된 데이터를 발견했습니다.")
        new_df = pd.DataFrame(missing_proofs)
        new_df.to_csv(filename, mode='a', header=False, index=False, encoding='utf-8-sig')
        print(f"누락된 데이터가 '{filename}' 파일에 성공적으로 추가되었습니다.")
        total_records = len(initial_proofs) + len(missing_proofs)
        print(f"최종적으로 총 {total_records}개의 데이터가 저장되었습니다.")
    else:
        print("\n✅ 2차 스캔 완료. 추가로 발견된 누락 데이터는 없습니다.")
