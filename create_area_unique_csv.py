#!/usr/bin/env python3
"""
地域データからユニークな企業リストを作成
"""
import json
import csv
import os
from datetime import datetime

def create_area_unique_csv():
    """地域データからユニークな企業CSVを作成"""
    
    print("="*70)
    print("地域データからユニーク企業リストを作成")
    print(f"作成時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # 地域データファイルを確認
    area_files = [
        'area_all_data_20250812_154808.json',
        'area_all_data_20250812_111213.json',
        'area_data_20250812_034941.json'
    ]
    
    # 最も大きいファイルを使用
    area_data = []
    used_file = None
    
    for file in area_files:
        if os.path.exists(file):
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if len(data) > len(area_data):
                        area_data = data
                        used_file = file
                        print(f"読み込み: {file} ({len(data)}件)")
            except Exception as e:
                print(f"エラー: {file} - {e}")
    
    if not area_data:
        print("地域データが見つかりません")
        return 0
    
    print(f"\n使用ファイル: {used_file}")
    print(f"総レコード数: {len(area_data)}件")
    
    # ユニークな企業を抽出（official_site_urlをキーとして使用）
    unique_companies = {}
    area_distribution = {}
    
    for record in area_data:
        key = record.get('official_site_url', '')
        if not key:
            key = record.get('company_name', '')
        
        if key and key not in unique_companies:
            unique_companies[key] = record
        
        # 地域分布をカウント
        area_name = record.get('category_name', '')
        if area_name:
            area_distribution[area_name] = area_distribution.get(area_name, 0) + 1
    
    print(f"\nユニーク企業数: {len(unique_companies)}件")
    
    # 地域分布を表示
    print("\n地域別企業数（重複含む）:")
    for area, count in sorted(area_distribution.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {area}: {count}件")
    
    # CSVファイルに保存
    output_file = f"area_unique_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        
        # ヘッダー
        writer.writerow([
            'company_name',
            'official_site_url',
            'address',
            'area',
            'yuryoweb_url'
        ])
        
        # データ
        for company in unique_companies.values():
            writer.writerow([
                company.get('company_name', ''),
                company.get('official_site_url', ''),
                company.get('address', ''),
                company.get('category_name', ''),  # 地域名
                company.get('yuryoweb_url', '')
            ])
    
    print(f"\n✅ CSVファイル作成完了")
    print(f"   ファイル名: {output_file}")
    print(f"   企業数: {len(unique_companies)}件")
    
    return len(unique_companies)

if __name__ == "__main__":
    create_area_unique_csv()