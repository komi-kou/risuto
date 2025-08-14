#!/usr/bin/env python3
"""
現在取得中のJSONデータから地域企業のユニークリストを作成
"""
import json
import csv
import os
from datetime import datetime
from collections import defaultdict

def create_area_csv():
    """JSONファイルから地域企業データを抽出"""
    
    print("="*70)
    print("JSONデータから地域企業リストを作成")
    print(f"作成時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # 最新の中間保存ファイルを使用
    json_files = [
        'industry_temp_38_20250813_134030.json',
        'industry_temp_33_20250813_112435.json',
        'industry_temp_20_20250813_023139.json'
    ]
    
    all_data = []
    used_file = None
    
    for file in json_files:
        if os.path.exists(file):
            print(f"\n読み込み中: {file}")
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_data = data
                    used_file = file
                    print(f"  {len(data)}件のレコード")
                    break
            except Exception as e:
                print(f"  エラー: {e}")
    
    if not all_data:
        print("\nデータファイルが見つかりません")
        return 0
    
    # カテゴリグループ別に分類
    area_records = []
    industry_records = []
    price_records = []
    feature_records = []
    
    for record in all_data:
        group = record.get('category_group', '')
        if group == 'area':
            area_records.append(record)
        elif group == 'industry':
            industry_records.append(record)
        elif group == 'price':
            price_records.append(record)
        elif group == 'feature':
            feature_records.append(record)
    
    print(f"\nカテゴリ別レコード数:")
    print(f"  地域(area): {len(area_records)}件")
    print(f"  業種(industry): {len(industry_records)}件")
    print(f"  価格(price): {len(price_records)}件")
    print(f"  特徴(feature): {len(feature_records)}件")
    
    if not area_records:
        print("\n地域データが含まれていません。全データから企業を抽出します。")
        # 全データからユニーク企業を抽出
        unique_companies = {}
        
        for record in all_data:
            url = record.get('official_site_url', '').strip()
            if not url:
                continue
            
            if url not in unique_companies:
                unique_companies[url] = record
    else:
        # 地域データからユニーク企業を抽出
        unique_companies = {}
        area_distribution = defaultdict(list)
        
        for record in area_records:
            url = record.get('official_site_url', '').strip()
            if not url:
                continue
            
            area_name = record.get('category_name', '')
            if url not in unique_companies:
                unique_companies[url] = record
                unique_companies[url]['areas'] = [area_name]
            else:
                if area_name not in unique_companies[url]['areas']:
                    unique_companies[url]['areas'].append(area_name)
            
            area_distribution[area_name].append(url)
        
        # 地域別分布を表示
        print("\n地域別企業数（上位15）:")
        for area, urls in sorted(area_distribution.items(), key=lambda x: len(set(x[1])), reverse=True)[:15]:
            unique_count = len(set(urls))
            print(f"  {area}: {unique_count}社")
    
    print(f"\nユニーク企業数: {len(unique_companies)}件")
    
    # CSVファイルに保存
    output_file = f"companies_unique_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        # ヘッダー
        if area_records:
            writer.writerow([
                'company_name',
                'official_site_url',
                'address',
                'areas',
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
        else:
            writer.writerow([
                'company_name',
                'official_site_url',
                'address',
                'category_group',
                'category_name',
                'yuryoweb_url'
            ])
            
            # データ
            for company in unique_companies.values():
                writer.writerow([
                    company.get('company_name', ''),
                    company.get('official_site_url', ''),
                    company.get('address', ''),
                    company.get('category_group', ''),
                    company.get('category_name', ''),
                    company.get('yuryoweb_url', '')
                ])
    
    print(f"\n✅ CSVファイル作成完了")
    print(f"   ファイル名: {output_file}")
    print(f"   ユニーク企業数: {len(unique_companies)}件")
    print(f"   使用データ: {used_file}")
    
    return len(unique_companies)

if __name__ == "__main__":
    create_area_csv()