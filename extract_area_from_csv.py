#!/usr/bin/env python3
"""
既存のCSVファイルから地域（area）カテゴリの企業を抽出してユニークリストを作成
"""
import csv
import os
from datetime import datetime
from collections import defaultdict

def extract_area_companies():
    """CSVファイルから地域企業を抽出"""
    
    print("="*70)
    print("既存CSVから地域企業データを抽出")
    print(f"作成時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # 既存のマージ済みCSVファイルを探す
    csv_files = [
        'all_companies_merged_20250812_203443.csv',
        'all_companies_merged_20250812_200904.csv',
        'all_companies_merged_20250812_200849.csv',
        'all_companies_merged_20250812_161609.csv'
    ]
    
    all_records = []
    
    for file in csv_files:
        if os.path.exists(file):
            print(f"\n読み込み: {file}")
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    records = list(reader)
                    all_records = records  # 最新のファイルを使用
                    print(f"  {len(records)}件のレコード")
                    break
            except Exception as e:
                print(f"  エラー: {e}")
    
    if not all_records:
        print("\nCSVファイルが見つかりません")
        return 0
    
    # 地域カテゴリのレコードのみ抽出
    area_records = [r for r in all_records if r.get('category_group') == 'area']
    print(f"\n地域カテゴリのレコード: {len(area_records)}件")
    
    # ユニークな企業を抽出（official_site_urlをキー）
    unique_companies = {}
    area_distribution = defaultdict(list)
    
    for record in area_records:
        url = record.get('official_site_url', '').strip()
        if not url:
            continue
        
        # 各企業が属する地域を記録
        area_name = record.get('category_name', '')
        if url not in unique_companies:
            unique_companies[url] = record
            unique_companies[url]['areas'] = [area_name]
        else:
            # 同じ企業が複数の地域に属する場合
            if area_name not in unique_companies[url]['areas']:
                unique_companies[url]['areas'].append(area_name)
        
        area_distribution[area_name].append(url)
    
    print(f"\nユニーク企業数: {len(unique_companies)}件")
    
    # 地域別分布を表示
    print("\n地域別企業数:")
    for area, urls in sorted(area_distribution.items(), key=lambda x: len(x[1]), reverse=True)[:15]:
        unique_count = len(set(urls))
        total_count = len(urls)
        print(f"  {area}: {unique_count}社（重複含む: {total_count}件）")
    
    # CSVファイルに保存
    output_file = f"area_unique_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        # ヘッダー
        writer.writerow([
            'company_name',
            'official_site_url',
            'address',
            'areas',  # 複数地域の場合は｜区切り
            'yuryoweb_url'
        ])
        
        # データ
        for company in unique_companies.values():
            writer.writerow([
                company.get('company_name', ''),
                company.get('official_site_url', ''),
                company.get('address', ''),
                '｜'.join(company.get('areas', [])),
                company.get('yuryoweb_url', '')
            ])
    
    print(f"\n✅ CSVファイル作成完了")
    print(f"   ファイル名: {output_file}")
    print(f"   ユニーク企業数: {len(unique_companies)}件")
    
    # 統計情報
    multi_area_companies = [c for c in unique_companies.values() if len(c.get('areas', [])) > 1]
    if multi_area_companies:
        print(f"   複数地域に属する企業: {len(multi_area_companies)}件")
    
    return len(unique_companies)

if __name__ == "__main__":
    extract_area_companies()