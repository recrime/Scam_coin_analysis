# xphere2.0_blocks.py
# main blocks 데이터 2차 스캔 방식으로 수집

import requests
import os
import pandas as pd
from datetime import datetime
import time

def fetch_blocks_in_batches(existing_block_ids=None, start_page=1, resume_file=None):
    """
    전체 페이지를 스캔하여 블록 데이터를 수집하는 함수.
    :param existing_block_ids: (set, optional) 이미 수집된 blockId 집합. 여기에 없는 block만 수집.
    :return: 수집된 블록 데이터 리스트 (list of dicts)
    """
    if existing_block_ids is None:
        existing_block_ids = set()
        is_second_scan = False
    else:
        is_second_scan = True

    collected_blocks = []
    page = start_page
    limit = 100
    while True:
        retry_count = 0
        while retry_count < 30:
            try:
                params = {'page': page, 'limit': limit}
                response = requests.get(api_url, params=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                rows = data.get('rows', []) if 'rows' in data else data.get('blocks', [])
                if not rows:
                    scan_type = "2차 (누락분 확인)" if is_second_scan else "1차"
                    print(f"페이지 {page}에서 더 이상 데이터가 없어 {scan_type} 스캔을 종료합니다.")
                    return collected_blocks
                new_found_count = 0
                for block in rows:
                    block_id = block.get('number')
                    if block_id not in existing_block_ids:
                        collected_blocks.append(block)
                        if is_second_scan:
                            existing_block_ids.add(block_id)
                        new_found_count += 1
                if is_second_scan:
                    print(f"페이지 {page} 완료 (새로 발견된 누락 데이터: {new_found_count}건)")
                else:
                    print(f"페이지 {page} 완료 (총 {len(collected_blocks)}건 수집)")
                page += 1
                time.sleep(0.02)
                break  # 정상 조회 시 retry 루프 탈출
            except requests.exceptions.RequestException as e:
                print(f"페이지 {page} 요청 중 오류 발생: {e}")
                retry_count += 1
                if retry_count < 30:
                    print(f"1분 후 재시도... (시도 {retry_count}/30)")
                    if resume_file:
                        with open(resume_file, 'w') as f:
                            f.write(str(page))
                    time.sleep(60)
                else:
                    print(f"페이지 {page}에서 30회 재시도 실패. 중단합니다.")
                    if resume_file:
                        with open(resume_file, 'w') as f:
                            f.write(str(page))
                    return collected_blocks
            except Exception as e:
                print(f"알 수 없는 오류 발생: {e}")
                if resume_file:
                    with open(resume_file, 'w') as f:
                        f.write(str(page))
                return collected_blocks
    return collected_blocks


if __name__ == "__main__":
    # 실제 API 엔드포인트와 저장 파일명 입력
    api_url = "https://xp.tamsa.io/xphere/api/v1/block"  # 실제 블록 API URL
    now = datetime.now()
    resume_file = 'mblocks_resume.txt'
    page = 1
    append_mode = False
    filename = f"mblocks_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    latest_csv = None
    limit = 100
    seen_block_ids = set()
    first_page = True
    total_count = 0
    # 최신 csv 탐색
    csv_files = [f for f in os.listdir('.') if f.startswith('mblocks_') and f.endswith('.csv')]
    if csv_files:
        csv_files.sort(reverse=True)
        latest_csv = csv_files[0]
    if os.path.exists(resume_file):
        with open(resume_file, 'r') as f:
            try:
                page = int(f.read().strip())
                print(f"[이어받기] {page} 페이지부터 재시작합니다.")
                if latest_csv:
                    ans = input(f"이전 수집 파일({latest_csv})에 이어서 저장할까요? (y=append/n=new): ").strip().lower()
                    if ans == 'y':
                        filename = latest_csv
                        append_mode = True
            except:
                pass
    while True:
        retry_count = 0
        while retry_count < 30:
            try:
                params = {'page': page, 'limit': limit}
                response = requests.get(api_url, params=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                rows = data.get('rows', []) if 'rows' in data else data.get('blocks', [])
                if not rows:
                    print(f"페이지 {page}에서 더 이상 데이터가 없어 1차 스캔을 종료합니다.")
                    if os.path.exists(resume_file):
                        os.remove(resume_file)
                    break
                # 중복 제거 및 저장
                new_blocks = []
                for block in rows:
                    block_id = block.get('number')
                    if block_id not in seen_block_ids:
                        new_blocks.append(block)
                if new_blocks:
                    for block in new_blocks:
                        block_id = block.get('number')
                        seen_block_ids.add(block_id)
                    mode = 'a' if (not first_page or append_mode) else 'w'
                    header = not (not first_page or append_mode)
                    df = pd.DataFrame(new_blocks)
                    df.to_csv(filename, mode=mode, header=header, index=False, encoding='utf-8-sig')
                    total_count += len(new_blocks)
                print(f"페이지 {page} 완료 (누적 {total_count}건 저장, 이번 페이지 {len(new_blocks)}건)")
                first_page = False
                page += 1
                time.sleep(0.02)
                break
            except requests.exceptions.RequestException as e:
                print(f"페이지 {page} 요청 중 오류 발생: {e}")
                retry_count += 1
                if retry_count < 30:
                    print(f"1분 후 재시도... (시도 {retry_count}/30)")
                    with open(resume_file, 'w') as f:
                        f.write(str(page))
                    time.sleep(60)
                else:
                    print(f"페이지 {page}에서 30회 재시도 실패. 중단합니다.")
                    with open(resume_file, 'w') as f:
                        f.write(str(page))
                    break
            except Exception as e:
                print(f"알 수 없는 오류 발생: {e}")
                with open(resume_file, 'w') as f:
                    f.write(str(page))
                break
        else:
            # retry_count == 30으로 중단된 경우
            break
        if not rows:
            break
    if total_count == 0:
        print("\n⚠️ 1차 스캔에서 수집된 데이터가 없습니다. 프로그램을 종료합니다.")
        exit()
    print(f"\n✅ 1차 스캔 완료. {total_count}개의 데이터가 '{filename}' 파일에 저장되었습니다.")

    print(f"\n--- 2차 스캔을 시작합니다. 1차 스캔 동안 추가/변경된 데이터를 확인합니다. ---")
    # 2차 스캔 (누락분)
    missing_blocks = fetch_blocks_in_batches(existing_block_ids=seen_block_ids, start_page=1, resume_file=None)
    if missing_blocks:
        print(f"\n✅ 2차 스캔 완료. {len(missing_blocks)}개의 누락된 데이터를 발견했습니다.")
        new_df = pd.DataFrame(missing_blocks)
        new_df.to_csv(filename, mode='a', header=False, index=False, encoding='utf-8-sig')
        print(f"누락된 데이터가 '{filename}' 파일에 성공적으로 추가되었습니다.")
        total_records = total_count + len(missing_blocks)
        print(f"최종적으로 총 {total_records}개의 데이터가 저장되었습니다.")
    else:
        print("\n✅ 2차 스캔 완료. 추가로 발견된 누락 데이터는 없습니다.")
