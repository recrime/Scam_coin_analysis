# xphere2.0_tokens_unions.py
# tokens와 unions를 한 번에 2차 스캔 방식으로 수집
import requests
import pandas as pd
from datetime import datetime
import time

def fetch_in_batches(api_url, id_keys, label):
    def get_id(row):
        for k in id_keys:
            if row.get(k):
                return row.get(k)
        return None

    def scan(existing_ids=None):
        if existing_ids is None:
            existing_ids = set()
            is_second_scan = False
        else:
            is_second_scan = True
        collected = []
        page = 1
        count = 100
        while True:
            try:
                params = {'page': page, 'count': count}
                response = requests.get(api_url, params=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                rows = data.get('rows', []) or data.get(label, [])
                if not rows:
                    scan_type = "2차 (누락분 확인)" if is_second_scan else "1차"
                    print(f"{label} 페이지 {page}에서 더 이상 데이터가 없어 {scan_type} 스캔을 종료합니다.")
                    break
                new_found_count = 0
                for row in rows:
                    row_id = get_id(row)
                    if row_id not in existing_ids:
                        collected.append(row)
                        if is_second_scan:
                            existing_ids.add(row_id)
                        new_found_count += 1
                if is_second_scan:
                    print(f"{label} 페이지 {page} 완료 (새로 발견된 누락 데이터: {new_found_count}건)")
                else:
                    print(f"{label} 페이지 {page} 완료 (총 {len(collected)}건 수집)")
                page += 1
                time.sleep(0.1)
            except requests.exceptions.RequestException as e:
                print(f"{label} 페이지 {page} 요청 중 오류 발생: {e}")
                break
            except Exception as e:
                print(f"{label} 알 수 없는 오류 발생: {e}")
                break
        return collected
    return scan


import os

def get_today_filename(prefix):
    today = datetime.now().strftime('%Y%m%d')
    for fname in os.listdir('.'):
        if fname.startswith(prefix + '_' + today) and fname.endswith('.csv'):
            return fname
    return None

def analyze_csv(filename, label):
    print(f"\n--- {label} 데이터 분석 결과 ---")
    df = pd.read_csv(filename)
    print(f"총 {len(df)}개 {label} 데이터")
    print(f"컬럼: {list(df.columns)}")
    # 주요 컬럼별 상위 5개 값 출력 (예시)
    for col in df.columns[:3]:
        print(f"\n[{col}] 상위 5개 분포:")
        print(df[col].value_counts(dropna=False).head())
    print(f"\n{label} 데이터 샘플:")
    print(df.head(3))

if __name__ == "__main__":
    now = datetime.now()
    # tokens
    token_api = "https://xp.tamsa.io/xphere/api/v1/token"
    token_id_keys = ['tokenId', 'id', 'contractAddress']
    token_label = 'tokens'
    token_prefix = 'tokens'
    token_today_file = get_today_filename(token_prefix)
    if token_today_file:
        ans = input(f"오늘 날짜로 저장된 토큰 파일({token_today_file})이 있습니다. 새로 다운로드할까요? (y=다운로드/n=분석만): ").strip().lower()
        if ans == 'y':
            do_token_download = True
        else:
            do_token_download = False
    else:
        do_token_download = True
    if do_token_download:
        token_filename = f"tokens_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        print("--- 1차 전체 토큰 데이터 스캔을 시작합니다. ---")
        token_scan = fetch_in_batches(token_api, token_id_keys, token_label)
        initial_tokens = token_scan()
        seen_token_ids = set()
        for token in initial_tokens:
            for k in token_id_keys:
                if token.get(k):
                    seen_token_ids.add(token.get(k))
                    break
        if initial_tokens:
            pd.DataFrame(initial_tokens).to_csv(token_filename, index=False, encoding='utf-8-sig')
            print(f"\n✅ 1차 스캔 완료. {len(initial_tokens)}개의 데이터가 '{token_filename}' 파일에 저장되었습니다.")
        else:
            print("\n⚠️ 1차 스캔에서 수집된 데이터가 없습니다. 프로그램을 종료합니다.")
            exit()
        print(f"\n--- 2차 스캔을 시작합니다. 1차 스캔 동안 추가/변경된 데이터를 확인합니다. ---")
        missing_tokens = token_scan(existing_ids=seen_token_ids)
        if missing_tokens:
            pd.DataFrame(missing_tokens).to_csv(token_filename, mode='a', header=False, index=False, encoding='utf-8-sig')
            print(f"누락된 데이터가 '{token_filename}' 파일에 성공적으로 추가되었습니다.")
            total_records = len(initial_tokens) + len(missing_tokens)
            print(f"최종적으로 총 {total_records}개의 데이터가 저장되었습니다.")
        else:
            print("\n✅ 2차 스캔 완료. 추가로 발견된 누락 데이터는 없습니다.")
        analyze_csv(token_filename, '토큰')
    else:
        analyze_csv(token_today_file, '토큰')

    # unions
    union_api = "https://xp.tamsa.io/xphere/api/v1/unions"
    union_id_keys = ['unionId', 'id']
    union_label = 'unions'
    union_prefix = 'unions'
    union_today_file = get_today_filename(union_prefix)
    if union_today_file:
        ans = input(f"오늘 날짜로 저장된 유니온 파일({union_today_file})이 있습니다. 새로 다운로드할까요? (y=다운로드/n=분석만): ").strip().lower()
        if ans == 'y':
            do_union_download = True
        else:
            do_union_download = False
    else:
        do_union_download = True
    if do_union_download:
        union_filename = f"unions_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        print("\n--- 1차 전체 유니온 데이터 스캔을 시작합니다. ---")
        union_scan = fetch_in_batches(union_api, union_id_keys, union_label)
        initial_unions = union_scan()
        seen_union_ids = set()
        for union in initial_unions:
            for k in union_id_keys:
                if union.get(k):
                    seen_union_ids.add(union.get(k))
                    break
        if initial_unions:
            pd.DataFrame(initial_unions).to_csv(union_filename, index=False, encoding='utf-8-sig')
            print(f"\n✅ 1차 스캔 완료. {len(initial_unions)}개의 데이터가 '{union_filename}' 파일에 저장되었습니다.")
        else:
            print("\n⚠️ 1차 스캔에서 수집된 데이터가 없습니다. 프로그램을 종료합니다.")
            exit()
        print(f"\n--- 2차 스캔을 시작합니다. 1차 스캔 동안 추가/변경된 데이터를 확인합니다. ---")
        missing_unions = union_scan(existing_ids=seen_union_ids)
        if missing_unions:
            pd.DataFrame(missing_unions).to_csv(union_filename, mode='a', header=False, index=False, encoding='utf-8-sig')
            print(f"누락된 데이터가 '{union_filename}' 파일에 성공적으로 추가되었습니다.")
            total_records = len(initial_unions) + len(missing_unions)
            print(f"최종적으로 총 {total_records}개의 데이터가 저장되었습니다.")
        else:
            print("\n✅ 2차 스캔 완료. 추가로 발견된 누락 데이터는 없습니다.")
        analyze_csv(union_filename, '유니온')
    else:
        analyze_csv(union_today_file, '유니온')
