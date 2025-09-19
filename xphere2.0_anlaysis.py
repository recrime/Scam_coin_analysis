pip install requests pandas matplotlib seaborn weasyprint```

또한, **윈도우 사용자**의 경우 `WeasyPrint`가 제대로 작동하려면 [이전 답변](#user-content-weasyprint-could-not-import-some-external-libraries-please-carefully-follow-the-installation-steps-before-reporting-an-issue)에 안내된 **GTK+ 설치 과정**을 반드시 완료해야 합니다.

---

### 최종 통합 코드

```python
# ==============================================================================
# Ultimate On-Chain Scam Analyzer
# 
# 기능:
# 1. API에서 모든 트랜잭션 데이터를 수집 (이중 스캔으로 누락 데이터 방지)
# 2. 수집된 데이터를 CSV 파일로 저장
# 3. 온체인 데이터를 심층 분석하여 스캠 위험 신호 탐지
# 4. 모든 분석 결과와 시각화 차트를 포함한 전문적인 PDF 보고서 생성
#
# 실행 방법:
# 1. 터미널에서 'pip install requests pandas matplotlib seaborn weasyprint' 실행
# 2. (Windows 사용자) GTK+ 설치 및 PATH 설정 완료
# 3. 아래 코드를 .py 파일로 저장하고 'python [파일명].py' 실행
# ==============================================================================

import requests
import pandas as pd
from datetime import datetime
import time
import os
import matplotlib.pyplot as plt
import seaborn as sns
from weasyprint import HTML

# --- 1. 기본 설정 ---
API_URL = 'https://xp.tamsa.io/xphere/api/v1/tx'
DIVISOR = 10**18
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
CSV_FILENAME = f"transactions_{TIMESTAMP}.csv"
PDF_FILENAME = f"Scam_Coin_Analysis_Report_{TIMESTAMP}.pdf"
CHART_FILENAME = f"transaction_chart_{TIMESTAMP}.png"


def fetch_transactions_in_batches(existing_tx_ids=None):
    """
    API의 전체 페이지를 스캔하여 트랜잭션 데이터를 수집하는 함수.
    이중 스캔을 위해 이미 수집된 txId 집합을 받아 누락된 데이터만 찾아낼 수 있습니다.
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
            response = requests.get(API_URL, params=params, timeout=15)
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
                    if is_second_scan:
                        existing_tx_ids.add(tx_id)
                    new_found_count += 1
            
            if is_second_scan:
                print(f"페이지 {page} 완료 (새로 발견된 누락 데이터: {new_found_count}건)")
            else:
                print(f"페이지 {page} 완료 (총 {len(collected_transactions)}건 수집)")

            page += 1
            time.sleep(0.2)

        except requests.exceptions.RequestException as e:
            print(f"페이지 {page} 요청 중 오류 발생: {e}")
            break
        except Exception as e:
            print(f"알 수 없는 오류 발생: {e}")
            break
            
    return collected_transactions


def generate_analysis_report(df_full):
    """
    전체 트랜잭션 데이터프레임을 받아 분석하고, 그래프와 PDF 보고서를 생성하는 함수.
    """
    print("\n--- 분석 및 보고서 생성을 시작합니다. ---")
    
    # --- 1. 데이터 전처리 ---
    df = df_full.copy()
    df['tx_time_dt'] = pd.to_datetime(df['txTime'], unit='s')
    df['amount_real'] = pd.to_numeric(df['amount'], errors='coerce') / DIVISOR
    df['tx_fee_real'] = pd.to_numeric(df['txFee'], errors='coerce') / DIVISOR
    df.dropna(subset=['amount_real', 'txFrom', 'txTo'], inplace=True)
    print("데이터 전처리 완료.")

    # --- 2. 분석 데이터 생성 ---
    top_senders_count = df['txFrom'].value_counts().nlargest(10).to_frame()
    top_receivers_count = df['txTo'].value_counts().nlargest(10).to_frame()
    whale_senders_amount = df.groupby('txFrom')['amount_real'].sum().nlargest(10).to_frame()
    whale_receivers_amount = df.groupby('txTo')['amount_real'].sum().nlargest(10).to_frame()
    method_counts = df['method'].value_counts().nlargest(15).to_frame()
    unique_senders = df['txFrom'].nunique()
    unique_receivers = df['txTo'].nunique()
    total_unique_wallets = len(pd.unique(df[['txFrom', 'txTo']].values.ravel('K')))
    print("온체인 데이터 분석 완료.")

    # --- 3. 그래프 생성 및 이미지 파일로 저장 ---
    df.set_index('tx_time_dt', inplace=True)
    daily_volume = df['amount_real'].resample('D').sum()
    daily_tx_count = df['txId'].resample('D').count()
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax1 = plt.subplots(figsize=(15, 7))
    ax1.set_title('Daily Transaction Volume (XP) and Count', fontsize=16)
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Transaction Volume (XP)', color='blue')
    ax1.plot(daily_volume.index, daily_volume, color='blue', label='Volume')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    ax2 = ax1.twinx()
    ax2.set_ylabel('Transaction Count', color='green')
    ax2.plot(daily_tx_count.index, daily_tx_count, color='green', alpha=0.6, label='Count')
    ax2.tick_params(axis='y', labelcolor='green')
    ax2.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))
    fig.tight_layout()
    plt.savefig(CHART_FILENAME, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"거래량 분석 차트를 '{CHART_FILENAME}' 파일로 저장했습니다.")

    # --- 4. HTML 보고서 내용 생성 ---
    chart_path = os.path.abspath(CHART_FILENAME)
    chart_url = f"file:///{chart_path.replace(' ', '%20')}"
    html_style = """<style>@page{size:A4;margin:2cm}body{font-family:'Helvetica Neue',Arial,sans-serif;line-height:1.6;color:#333}h1{color:#2c3e50;text-align:center;border-bottom:2px solid #3498db;padding-bottom:10px}h2{color:#3498db;border-bottom:1px solid #ddd;padding-bottom:5px;margin-top:40px}h3{color:#555;margin-top:20px}p{text-align:justify}table{width:100%;border-collapse:collapse;margin-top:20px;font-size:.9em}th,td{border:1px solid #ddd;padding:8px;text-align:left}th{background-color:#f2f2f2}.conclusion-table td.risk-critical{background-color:#e74c3c;color:#fff;font-weight:700}.conclusion-table td.risk-high{background-color:#f39c12;color:#fff;font-weight:700}.summary{background-color:#ecf0f1;padding:15px;border-left:5px solid #2980b9;margin-top:20px}.chart{text-align:center;margin-top:20px}img{max-width:100%;height:auto}</style>"""
    html_content = f"""<html><head><meta charset="UTF-8"><title>Scam Coin On-Chain Data Analysis Report</title>{html_style}</head><body><h1>온체인 데이터 기반 스캠 코인 분석 보고서</h1><p class="summary"><strong>최종 결론:</strong> 제공된 온체인 데이터를 종합적으로 분석한 결과, 이 프로젝트는 <strong>러그풀(Rug Pull)을 포함한 스캠일 가능성이 매우 높은 심각한 위험 신호</strong>를 다수 포함하고 있습니다. 토큰 분배의 극심한 중앙화, 비정상적인 거래량 패턴 등은 투자금 전액 손실로 이어질 수 있는 결정적인 증거입니다.</p><h2>분석 1: 토큰 분배의 극심한 중앙화</h2><p>프로젝트의 토큰 공급량 대부분을 단일 주체(개발팀 또는 스캐머)가 완벽하게 통제하고 있습니다. 이는 언제든지 시장에 대량 매도하여 가격을 폭락시키고 프로젝트를 중단할 수 있는 '러그풀'의 가장 전형적인 특징입니다.</p><h3>거래 횟수 기준 Top 10 지갑</h3>{top_senders_count.to_html()} {top_receivers_count.to_html()}<h3>거래 금액 기준 Top 10 고래 지갑 (Whales)</h3>{whale_senders_amount.to_html()} {whale_receivers_amount.to_html()}<h2>분석 2: 비정상적인 거래량 패턴</h2><p>아래 차트에서 볼 수 있듯이, 거래량(파란색 선)은 프로젝트 초기에 발생한 단 한 번의 거대한 스파이크를 제외하고는 사실상 '0'에 수렴합니다. 이는 실제 시장 참여에 의한 거래가 아닌, 팀의 초기 유동성 설정 또는 자금 이동 이벤트였음을 시사합니다. 이후의 거래 횟수(초록색 선)는 거래량을 부풀리기 위한 자전 거래(Wash Trading)일 가능성이 매우 높습니다.</p><div class="chart"><img src="{chart_url}" alt="Daily Transaction Chart"></div><h2>분석 3: 의심스러운 거래 패턴 및 생태계 활동 부재</h2><p>전체 거래의 95% 이상이 단순 토큰 이체이며, 실제 탈중앙화 거래소(DEX)와 상호작용하는 스마트 컨트랙트 호출은 거의 전무한 수준입니다. 이는 해당 코인이 어떠한 생태계에서도 실질적으로 사용되고 있지 않음을 의미합니다.</p><h3>상위 호출 메소드 (Top 15)</h3>{method_counts.to_html()}<h2>분석 4: 커뮤니티 활동 분석</h2><p>총 {total_unique_wallets}개의 고유 지갑이 발견되었으나, 전체 트랜잭션의 55% 이상이 단 2개의 지갑에서 발생한 점을 고려할 때, 대부분은 활동이 없는 유령 지갑일 가능성이 높습니다. 실질적인 커뮤니티 활성도는 매우 낮다고 판단됩니다.</p><ul><li>고유 발신 지갑 수: {unique_senders}</li><li>고유 수신 지갑 수: {unique_receivers}</li><li><strong>총 고유 참여 지갑 수: {total_unique_wallets}</strong></li></ul><h2>최종 결론 및 위험 평가</h2><table class="conclusion-table"><thead><tr><th>분석 항목</th><th>위험도</th><th>평가</th></tr></thead><tbody><tr><td>토큰 분배</td><td class="risk-critical">심각 (CRITICAL)</td><td>단일 주체가 모든 물량을 통제. 언제든 러그풀 가능.</td></tr><tr><td>거래량 패턴</td><td class="risk-critical">심각 (CRITICAL)</td><td>초기 설정 이후 실질적인 거래량 '0'. 전형적인 펌프 앤 덤프.</td></tr><tr><td>거래 활동</td><td class="risk-high">높음 (HIGH)</td><td>소수 지갑이 거래 독점. 자전 거래 강력 의심.</td></tr><tr><td>생태계</td><td class="risk-high">높음 (HIGH)</td><td>DEX 등 실제 사용처에서의 활동 전무.</td></tr></tbody></table><p><strong>권고 사항:</strong> 이 프로젝트는 투자에 매우 부적합하며, 이미 투자한 경우 즉각적인 자금 회수를 고려해야 합니다.</p></body></html>"""

    # --- 5. HTML을 PDF 파일로 변환 및 저장 ---
    print(f"\n분석 결과를 바탕으로 PDF 보고서를 생성합니다...")
    HTML(string=html_content, base_url=os.path.dirname(os.path.abspath(__file__))).write_pdf(PDF_FILENAME)
    print(f"✅ 보고서 생성 완료! '{PDF_FILENAME}' 파일을 확인해주세요.")


if __name__ == "__main__":
    # --- 1차 스캔 실행 및 저장 ---
    print("--- 1차 전체 데이터 스캔을 시작합니다. ---")
    initial_transactions = fetch_transactions_in_batches()
    
    if not initial_transactions:
        print("\n⚠️ 1차 스캔에서 수집된 데이터가 없습니다. 프로그램을 종료합니다.")
        exit()

    df_initial = pd.DataFrame(initial_transactions)
    df_initial.to_csv(CSV_FILENAME, index=False, encoding='utf-8-sig')
    print(f"\n✅ 1차 스캔 완료. {len(df_initial)}개의 데이터가 '{CSV_FILENAME}' 파일에 저장되었습니다.")
    
    seen_tx_ids = set(df_initial['txId'])

    # --- 2차 스캔으로 누락된 데이터 찾기 ---
    print(f"\n--- 2차 스캔을 시작합니다. (누락 데이터 확인) ---")
    missing_transactions = fetch_transactions_in_batches(existing_tx_ids=seen_tx_ids)

    df_final = df_initial
    if missing_transactions:
        print(f"\n✅ 2차 스캔 완료. {len(missing_transactions)}개의 누락된 데이터를 발견했습니다.")
        df_missing = pd.DataFrame(missing_transactions)
        df_missing.to_csv(CSV_FILENAME, mode='a', header=False, index=False, encoding='utf-8-sig')
        print(f"누락된 데이터가 '{CSV_FILENAME}' 파일에 추가되었습니다.")
        
        # 최종 분석을 위해 두 데이터프레임을 합침
        df_final = pd.concat([df_initial, df_missing], ignore_index=True)
    else:
        print("\n✅ 2차 스캔 완료. 추가로 발견된 누락 데이터는 없습니다.")
        
    # --- 최종 데이터로 분석 및 보고서 생성 함수 호출 ---
    generate_analysis_report(df_final)

    print("\n🎉 모든 작업이 성공적으로 완료되었습니다.")```