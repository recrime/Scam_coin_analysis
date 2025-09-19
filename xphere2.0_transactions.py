# trans_ais3.py

import requests
import pandas as pd
from datetime import datetime
import time

# --- 1. 초기 설정 ---
now = datetime.now()
filename = f"transactions_{now.strftime('%Y%m%d_%H%M%S')}.csv"
url = 'https://xp.tamsa.io/xphere/api/v1/tx'

def fetch_transactions_in_batches(existing_tx_ids=None):
    """
    전체 페이지를 스캔하여 트랜잭션 데이터를 수집하는 함수.
    
    :param existing_tx_ids: (set, optional) 이미 수집된 txId 집합. 
                            이 값이 주어지면, 여기에 없는 txId만 수집합니다.
    :return: 수집된 트랜잭션 데이터 리스트 (list of dicts)
    """
    if existing_tx_ids is None:
        existing_tx_ids = set()
        is_second_scan = False
    else:
        is_second_scan = True

    collected_transactions = []
    page = 1
    limit = 100

    while True:
        try:
            params = {'page': page, 'limit': limit}
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            rows = data.get('rows', [])

            if not rows:
                scan_type = "2차 (누락분 확인)" if is_second_scan else "1차"
                print(f"페이지 {page}에서 더 이상 데이터가 없어 {scan_type} 스캔을 종료합니다.")
                break

            new_found_count = 0
            for tx in rows:
                tx_id = tx.get('txId')
                if tx_id not in existing_tx_ids:
                    collected_transactions.append(tx)
                    if is_second_scan: # 2차 스캔일 때만 새 ID를 추가
                        existing_tx_ids.add(tx_id)
                    new_found_count += 1
            
            if is_second_scan:
                print(f"페이지 {page} 완료 (새로 발견된 누락 데이터: {new_found_count}건)")
            else:
                print(f"페이지 {page} 완료 (총 {len(collected_transactions)}건 수집)")

            page += 1
            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            print(f"페이지 {page} 요청 중 오류 발생: {e}")
            break
        except Exception as e:
            print(f"알 수 없는 오류 발생: {e}")
            break
            
    return collected_transactions


# --- 2. 1차 전체 스캔 실행 ---
print("--- 1차 전체 데이터 스캔을 시작합니다. ---")
initial_transactions = fetch_transactions_in_batches()
seen_tx_ids = {tx['txId'] for tx in initial_transactions} # 2차 스캔을 위한 ID 집합 생성

# --- 3. 1차 수집 데이터를 CSV로 저장 ---
if initial_transactions:
    df = pd.DataFrame(initial_transactions)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"\n✅ 1차 스캔 완료. {len(initial_transactions)}개의 데이터가 '{filename}' 파일에 저장되었습니다.")
else:
    print("\n⚠️ 1차 스캔에서 수집된 데이터가 없습니다. 프로그램을 종료합니다.")
    exit() # 데이터가 없으면 종료

# --- 4. 2차 스캔으로 누락된 데이터 찾기 ---
print(f"\n--- 2차 스캔을 시작합니다. 1차 스캔 동안 추가/변경된 데이터를 확인합니다. ---")
missing_transactions = fetch_transactions_in_batches(existing_tx_ids=seen_tx_ids)

# --- 5. 누락된 데이터를 기존 CSV 파일에 추가 저장 ---
if missing_transactions:
    print(f"\n✅ 2차 스캔 완료. {len(missing_transactions)}개의 누락된 데이터를 발견했습니다.")
    new_df = pd.DataFrame(missing_transactions)
    
    # mode='a' (append)로 파일을 열고, header=False로 헤더 중복 작성을 방지합니다.
    new_df.to_csv(filename, mode='a', header=False, index=False, encoding='utf-8-sig')
    print(f"누락된 데이터가 '{filename}' 파일에 성공적으로 추가되었습니다.")
    total_records = len(initial_transactions) + len(missing_transactions)
    print(f"최종적으로 총 {total_records}개의 데이터가 저장되었습니다.")
else:
    print("\n✅ 2차 스캔 완료. 추가로 발견된 누락 데이터는 없습니다.")